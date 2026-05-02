"""
modules/fast_dip_detector.py — Live fast-dip detector (Stage 1)

Stage 1 scope (no decision gate, no suppressions, no Telegram):
  - Per-token rolling-max over the previous 60s of swap prices.
  - Trigger when live drop ≥ 40% from rolling-max → write one
    fast_dip_shadow row.
  - State machine per token:
      IDLE  → trigger fires → IN_DIP   (write trigger row, save row id)
      IN_DIP → dip-end met  → IDLE     (UPDATE row with dip_end fields)
    Dip-end conditions (Pass 1 spec, applied to live data):
      a) live drop < 0.20 from current rolling-max, OR
      b) 60-second swap gap (block_time of new swap > last + 60).
  - One row per dip episode. While IN_DIP, sub-swaps update state but
    do NOT write a new shadow row.

Architecture:
  - Subscribes to gRPC indexer's per-event callback (`on_event`),
    invoked synchronously from grpc_indexer._consume_stream after
    each fee event is decoded but before the next one is processed.
  - All state mutation is synchronous in the callback so consecutive
    events can't race the state machine. DB writes (INSERT at
    trigger, UPDATE at dip-end) are dispatched via
    asyncio.create_task so the indexer loop is never blocked by
    SQLite latency.
  - Callback exceptions are swallowed by grpc_indexer; detector
    failures cannot affect the indexer's primary write path.

Time source:
  - All window math (60s rolling-max, 60s gap-end, 5s density window
    in Stage 2) uses event.block_time, NOT wall-clock. A burst of
    catch-up events from the same Solana slot is treated as a single
    block_time period and won't fire spurious triggers.
  - Wall-clock is recorded only as trigger_wall_time so Stage 2 can
    compute trigger_lag_seconds = wall_clock - block_time.

Memory cap:
  - At most TOKEN_CAP tokens kept in memory simultaneously. New live
    events for an unknown token, when at cap, evict the LRU token.
  - On startup, backfill the rolling-max window for the
    BACKFILL_TOKEN_CAP most-recently-active in-scope tokens. Tokens
    outside that set get added on first live event (with eviction).

SOL/USD denomination:
  - Trigger logic is purely ratio-based — independent of any
    SOL/USD constant. Prices are stored in SOL units. Stage 2's
    pre_dip_1m_usd_vol is the only column that requires a SOL/USD
    multiplier; that conversion lives in Stage 2.
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

import aiosqlite

import database as db

logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────────────────────

# All windows are in seconds of block_time.
ROLLING_MAX_WINDOW_SEC = 60
TRIGGER_DROP_PCT       = 0.40   # locked: Pass 1 / Pass 1.5
DIP_END_DROP_PCT       = 0.20   # locked: Pass 1
DIP_END_GAP_SEC        = 60     # locked: Pass 1
# Trigger-time data-quality gate: require enough swap activity in the
# 5s leading up to the trigger swap so we don't fire on sparse/noisy
# tokens. Distinct from Stage 2's swap_count<5 suppression which
# measures the 10s AFTER trigger — same threshold, different window,
# different purpose.
TRIGGER_DENSITY_WINDOW_SEC = 5
TRIGGER_DENSITY_MIN        = 5

# Memory bound — see module docstring.
TOKEN_CAP              = 500
BACKFILL_TOKEN_CAP     = 500

# Refresh cadence for the symbol + scope caches (seconds, wall-clock).
CACHE_REFRESH_SEC      = 300

# Backfill timeout — flagged in startup() if exceeded so the operator
# can investigate before declaring Stage 1 verified.
BACKFILL_SOFT_TIMEOUT_SEC = 30

# Status set the detector watches (Stage 1: same as Pass 1's cohort).
WATCHED_STATUSES = ("tracking", "ath_confirmed", "alerted")


# ── Data structures ──────────────────────────────────────────────────────────

@dataclass
class _SwapEvent:
    """One swap observation kept in a token's rolling-max deque."""
    block_time: float
    price_sol: float
    signature: str
    event_type: str         # 'buy' or 'sell'


def price_sol_from_event(quote_amount: int, base_amount: int | None) -> float | None:
    """Derive SOL-per-token price from a PumpSwap event's amounts.

    Pool orientation is treated as deterministic-normal across the
    cohort (208/208 verified — see CLAUDE/MEMORY notes). Returns
    None for malformed amounts so the caller can drop the event.

    quote_amount is in lamports (1e9 / SOL). base_amount is in
    raw token units; pump.fun mints are 6-decimal so the divisor
    is 1e6. Token decimals are NOT looked up per-token — the
    pump.fun cohort is uniformly 6-decimal.
    """
    if not quote_amount or quote_amount <= 0:
        return None
    if base_amount is None or base_amount <= 0:
        return None
    # SOL per token = (quote_amount / 1e9) / (base_amount / 1e6)
    #               = quote_amount * 1e6 / (base_amount * 1e9)
    #               = quote_amount / (base_amount * 1e3)
    return quote_amount / (base_amount * 1_000.0)


# ── Detector ─────────────────────────────────────────────────────────────────

class FastDipDetector:
    """Stage 1 live fast-dip detector. See module docstring."""

    def __init__(self, db_path: str):
        self._db_path = db_path

        # Per-token state (keyed on token_address):
        self._buffers: dict[str, deque[_SwapEvent]] = {}
        # Mirror of buffer signatures for O(1) backfill/live dedup.
        # Tuple is (signature, event_type) since one tx can emit both.
        self._buffer_sigs: dict[str, set[tuple[str, str]]] = {}
        # IDLE | IN_DIP. Absent key == IDLE.
        self._states: dict[str, str] = {}
        # Active dip's INSERT task per token. The task RESULT is the
        # shadow-row id (int) on success or None on failure. Storing
        # the task itself instead of a row_id-on-completion dict means
        # back-to-back dips on the same token can't have their row ids
        # overwrite each other in shared state — each dip episode is
        # bound to its own task object.
        self._pending_inserts: dict[str, asyncio.Task] = {}
        # Wall-clock last-touch for LRU eviction. Updated on every
        # accepted on_event for that token.
        self._last_touch: dict[str, float] = {}

        # Caches (refreshed periodically; see _periodic_refresh_loop).
        self._symbol_cache: dict[str, str] = {}
        self._symbol_cache_loaded_at: float = 0.0
        self._token_scope: set[str] = set()
        self._token_scope_loaded_at: float = 0.0

        # Lifecycle / observability counters.
        self._stats = {
            "events_seen":             0,
            "events_dropped_dedup":    0,
            "events_out_of_scope":     0,
            "events_no_price":         0,
            "triggers_fired":          0,
            "triggers_density_skipped": 0,
            "dips_ended_recovered":    0,
            "dips_ended_gap":          0,
            "dips_ended_evicted":      0,
            "tokens_evicted":          0,
        }

        # The event loop the detector runs on. Captured the first time
        # on_event is called from the indexer task so we can dispatch
        # background coroutines onto the same loop.
        self._loop: asyncio.AbstractEventLoop | None = None

        # Periodic-refresh task handle (set by start_periodic_refresh).
        self._refresh_task: asyncio.Task | None = None

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def startup(self):
        """Backfill the rolling-max window for the most-recently-active
        in-scope tokens. Run once before the gRPC stream starts so the
        first live event has a populated window to compare against.
        """
        await self._refresh_token_scope(force=True)
        await self._refresh_symbol_cache(force=True)

        if not self._token_scope:
            logger.info("📉 fast_dip: no in-scope tokens at startup; backfill skipped")
            return

        backfill_started = time.time()
        cutoff_block_time = time.time() - ROLLING_MAX_WINDOW_SEC

        # Pick the BACKFILL_TOKEN_CAP most-recently-active tokens so we
        # don't load the full ~hundreds-of-tokens cohort uniformly when
        # most of them haven't traded in the last 60s.
        recent_tokens = await self._fetch_recent_active_tokens(
            cutoff_block_time, BACKFILL_TOKEN_CAP
        )
        if not recent_tokens:
            logger.info(
                "📉 fast_dip: no in-scope tokens with swaps in the last "
                f"{ROLLING_MAX_WINDOW_SEC}s; backfill skipped"
            )
            return

        loaded_count = 0
        async with db.db_connect(self._db_path) as conn:
            for addr in recent_tokens:
                async with conn.execute(
                    """
                    SELECT signature, event_type, block_time,
                           quote_amount, base_amount
                    FROM pumpswap_fees
                    WHERE token_address = ?
                      AND block_time >= ?
                      AND base_amount IS NOT NULL
                      AND base_amount > 0
                      AND quote_amount > 0
                    ORDER BY block_time ASC
                    """,
                    (addr, cutoff_block_time),
                ) as cur:
                    rows = await cur.fetchall()
                if not rows:
                    continue
                buf: deque[_SwapEvent] = deque()
                sigs: set[tuple[str, str]] = set()
                for sig, ev_type, bt, qa, ba in rows:
                    price = price_sol_from_event(qa, ba)
                    if price is None or bt is None:
                        continue
                    key = (sig, ev_type)
                    if key in sigs:
                        continue
                    buf.append(_SwapEvent(
                        block_time=float(bt),
                        price_sol=price,
                        signature=sig,
                        event_type=ev_type,
                    ))
                    sigs.add(key)
                if buf:
                    self._buffers[addr] = buf
                    self._buffer_sigs[addr] = sigs
                    self._last_touch[addr] = time.time()
                    loaded_count += 1

        elapsed = time.time() - backfill_started
        logger.info(
            f"📉 fast_dip: backfill seeded {loaded_count} tokens in {elapsed:.1f}s "
            f"(scope={len(self._token_scope)}, recent_active={len(recent_tokens)})"
        )
        if elapsed > BACKFILL_SOFT_TIMEOUT_SEC:
            logger.warning(
                f"📉 fast_dip: backfill took {elapsed:.1f}s "
                f"(>{BACKFILL_SOFT_TIMEOUT_SEC}s soft-timeout). Investigate "
                f"before declaring Stage 1 verified."
            )

    def start_periodic_refresh(self):
        """Spawn the background task that refreshes the symbol + scope
        caches every CACHE_REFRESH_SEC. Idempotent — safe to call once
        from main() after the event loop is running.
        """
        if self._refresh_task is not None:
            return
        self._refresh_task = asyncio.create_task(self._periodic_refresh_loop())

    async def _periodic_refresh_loop(self):
        while True:
            try:
                await asyncio.sleep(CACHE_REFRESH_SEC)
                await self._refresh_token_scope()
                await self._refresh_symbol_cache()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"📉 fast_dip: periodic refresh error: {e}")

    # ── Cache refresh ────────────────────────────────────────────────────

    async def _refresh_token_scope(self, force: bool = False):
        if not force and (time.time() - self._token_scope_loaded_at) < CACHE_REFRESH_SEC:
            return
        async with db.db_connect(self._db_path) as conn:
            placeholders = ",".join("?" for _ in WATCHED_STATUSES)
            async with conn.execute(
                f"SELECT address FROM tokens WHERE status IN ({placeholders})",
                WATCHED_STATUSES,
            ) as cur:
                rows = await cur.fetchall()
        self._token_scope = {r[0] for r in rows}
        self._token_scope_loaded_at = time.time()

    async def _refresh_symbol_cache(self, force: bool = False):
        if not force and (time.time() - self._symbol_cache_loaded_at) < CACHE_REFRESH_SEC:
            return
        async with db.db_connect(self._db_path) as conn:
            async with conn.execute(
                "SELECT address, symbol FROM tokens WHERE symbol IS NOT NULL"
            ) as cur:
                rows = await cur.fetchall()
        self._symbol_cache = {r[0]: r[1] for r in rows if r[1]}
        self._symbol_cache_loaded_at = time.time()

    async def _fetch_recent_active_tokens(
        self, since_block_time: float, limit: int
    ) -> list[str]:
        """Return the in-scope token_addresses with a swap in
        [since_block_time, now], ordered most-recent-first, capped at
        `limit`. Used to prioritize backfill.
        """
        if not self._token_scope:
            return []
        # SQLite IN-list cap: chunk if scope is huge. Phoenix's scope is
        # in the hundreds, so a single query is fine.
        scope = list(self._token_scope)
        placeholders = ",".join("?" for _ in scope)
        params = (*scope, since_block_time, limit)
        async with db.db_connect(self._db_path) as conn:
            async with conn.execute(
                f"""
                SELECT token_address, MAX(block_time) AS last_block
                FROM pumpswap_fees
                WHERE token_address IN ({placeholders})
                  AND block_time >= ?
                GROUP BY token_address
                ORDER BY last_block DESC
                LIMIT ?
                """,
                params,
            ) as cur:
                rows = await cur.fetchall()
        return [r[0] for r in rows]

    # ── Event hook (called from grpc_indexer._consume_stream) ────────────

    def on_event(
        self,
        *,
        token_address: str,
        pool_address: str | None,
        block_time: float,
        quote_amount: int,
        base_amount: int | None,
        event_type: str,
        signature: str,
    ) -> None:
        """Synchronous observer for one decoded PumpSwap event.

        Mutates state inline; dispatches DB writes onto the running
        loop via asyncio.create_task so the gRPC stream is never
        blocked on SQLite.

        Exceptions raised here are swallowed by grpc_indexer's
        try/except guard (see _consume_stream), so the primary write
        path stays untouchable. Internal try/except inside this
        method protects per-token state from being left mid-mutation
        on an unexpected error.
        """
        try:
            self._stats["events_seen"] += 1

            # Only look at tokens we're tracking (Stage 1 scope).
            if token_address not in self._token_scope:
                self._stats["events_out_of_scope"] += 1
                return

            price_sol = price_sol_from_event(quote_amount, base_amount)
            if price_sol is None or block_time is None:
                self._stats["events_no_price"] += 1
                return

            # Capture the loop on first valid event so background dispatch
            # uses the same loop the indexer is on.
            if self._loop is None:
                try:
                    self._loop = asyncio.get_running_loop()
                except RuntimeError:
                    # Should not happen — grpc_indexer calls us from inside
                    # an async task. Drop to silent-no-op DB dispatch.
                    self._loop = None

            self._ingest_event(
                token_address=token_address,
                pool_address=pool_address,
                block_time=float(block_time),
                price_sol=price_sol,
                signature=signature,
                event_type=event_type,
            )
        except Exception as e:
            # Defensive: per-event handler must not raise into the
            # indexer's outer try/except (which would log it once but
            # that's our last defense, not our first).
            logger.error(
                f"📉 fast_dip: on_event handler error for {token_address}: {e}",
                exc_info=True,
            )

    def _ingest_event(
        self,
        *,
        token_address: str,
        pool_address: str | None,
        block_time: float,
        price_sol: float,
        signature: str,
        event_type: str,
    ) -> None:
        """Core state-machine step. Caller has already validated price."""

        # Dedup against the per-token signature set — backfill rows may
        # reappear in the live stream during the race window after
        # startup() finishes and before the gRPC stream catches up.
        sigs = self._buffer_sigs.get(token_address)
        sig_key = (signature, event_type)
        if sigs is not None and sig_key in sigs:
            self._stats["events_dropped_dedup"] += 1
            return

        # New token → may need to evict to stay within TOKEN_CAP.
        if token_address not in self._buffers:
            self._maybe_evict_lru(now_wall=time.time())
            self._buffers[token_address] = deque()
            self._buffer_sigs[token_address] = set()

        buf = self._buffers[token_address]
        sigs = self._buffer_sigs[token_address]

        # Capture the block_time of the most-recently-seen event
        # BEFORE appending or evicting. _maybe_end_dip uses this for
        # gap detection — looking at buf[-2] doesn't work because a
        # large gap can evict the entire prior window first, leaving
        # a single-element deque from which no gap can be inferred.
        prev_last_block_time: float | None = buf[-1].block_time if buf else None

        # Append new event. Out-of-order block_time (catchup burst)
        # is allowed — the deque is just a window of observations,
        # not strictly ordered. Eviction still works on block_time.
        buf.append(_SwapEvent(
            block_time=block_time,
            price_sol=price_sol,
            signature=signature,
            event_type=event_type,
        ))
        sigs.add(sig_key)
        self._last_touch[token_address] = time.time()

        # Evict events older than 60s of block_time. Use the new
        # event's block_time as the reference so a brief catchup
        # burst doesn't shrink the window prematurely.
        cutoff = block_time - ROLLING_MAX_WINDOW_SEC
        while buf and buf[0].block_time < cutoff:
            old = buf.popleft()
            sigs.discard((old.signature, old.event_type))

        # State machine
        state = self._states.get(token_address, "IDLE")
        if state == "IDLE":
            self._maybe_trigger(
                token_address=token_address,
                pool_address=pool_address,
                buf=buf,
                trigger_block_time=block_time,
                trigger_price_sol=price_sol,
                trigger_signature=signature,
                trigger_event_type=event_type,
            )
        else:  # IN_DIP
            self._maybe_end_dip(
                token_address=token_address,
                buf=buf,
                prev_last_block_time=prev_last_block_time,
                latest_block_time=block_time,
                latest_price_sol=price_sol,
            )

    def _maybe_trigger(
        self,
        *,
        token_address: str,
        pool_address: str | None,
        buf: deque[_SwapEvent],
        trigger_block_time: float,
        trigger_price_sol: float,
        trigger_signature: str,
        trigger_event_type: str,
    ) -> None:
        """While IDLE: check if the new swap crosses the 40% threshold.
        If yes, flip to IN_DIP and dispatch the shadow-row INSERT.
        """
        # Rolling-max over the deque (which already holds the last 60s
        # ending at trigger_block_time, including the new event itself).
        rolling_max = -1.0
        rolling_max_bt = trigger_block_time
        for ev in buf:
            if ev.price_sol > rolling_max:
                rolling_max = ev.price_sol
                rolling_max_bt = ev.block_time
        if rolling_max <= 0:
            return

        drop_pct = 1.0 - (trigger_price_sol / rolling_max)
        if drop_pct < TRIGGER_DROP_PCT:
            return

        # Trigger-time density gate. Count distinct swap events the
        # detector has already seen in [trigger_bt - 5s, trigger_bt].
        # The deque already deduplicates by (signature, event_type) at
        # append time, so each entry counts as one distinct swap. The
        # trigger event itself is included (it's at buf[-1]). If we
        # don't yet have ≥5 trades in the 5s ramp-up, suppress the
        # trigger — sparse activity makes the rolling-max unreliable.
        density_cutoff = trigger_block_time - TRIGGER_DENSITY_WINDOW_SEC
        swap_density_5s = sum(1 for ev in buf if ev.block_time >= density_cutoff)
        if swap_density_5s < TRIGGER_DENSITY_MIN:
            self._stats["triggers_density_skipped"] += 1
            return

        # Trigger! Transition to IN_DIP and dispatch the INSERT.
        self._states[token_address] = "IN_DIP"
        self._stats["triggers_fired"] += 1

        symbol = self._symbol_cache.get(token_address)
        wall_now = time.time()

        logger.info(
            f"📉 fast_dip TRIGGER ${symbol or token_address[:8]} "
            f"drop={drop_pct*100:.1f}% rmax={rolling_max:.6g} SOL "
            f"px={trigger_price_sol:.6g} SOL bt={trigger_block_time:.0f}"
        )

        if self._loop is None:
            return

        async def _do_insert() -> int | None:
            try:
                return await db.log_fast_dip_trigger(
                    token_address=token_address,
                    pool_address=pool_address,
                    symbol=symbol,
                    trigger_block_time=trigger_block_time,
                    trigger_wall_time=wall_now,
                    trigger_signature=trigger_signature,
                    trigger_event_type=trigger_event_type,
                    trigger_price_sol=trigger_price_sol,
                    rolling_max_price_sol=rolling_max,
                    rolling_max_block_time=rolling_max_bt,
                    drop_pct=drop_pct,
                    db_path=self._db_path,
                )
            except Exception as e:
                logger.error(
                    f"📉 fast_dip: INSERT failed for {token_address}: {e}"
                )
                return None

        task = self._loop.create_task(_do_insert())
        self._pending_inserts[token_address] = task

    def _maybe_end_dip(
        self,
        *,
        token_address: str,
        buf: deque[_SwapEvent],
        prev_last_block_time: float | None,
        latest_block_time: float,
        latest_price_sol: float,
    ) -> None:
        """While IN_DIP: check both Pass 1 dip-end conditions on the
        rolling window we already maintain. If either fires, flip to
        IDLE and dispatch the shadow-row UPDATE.

        Note: the 60s-gap branch is also inspected when a NEW event
        arrives — meaning a true gap-end is detected when the next
        swap finally appears. A token that goes silent and never
        trades again leaves its IN_DIP row open until the next
        startup-time review (or future Stage-3 sweeper). This matches
        the diagnostic methodology, where dip_end is bounded by the
        cohort's MAX(block_time).
        """
        # Need at least one observation (we just appended one, so buf
        # is guaranteed non-empty).
        if not buf:
            return

        # Gap detection uses the prior most-recent event's block_time
        # captured in _ingest_event BEFORE the deque was mutated. This
        # is robust against a gap large enough to evict the entire
        # 60s window — buf[-2] would be undefined in that case.
        gap_seconds: float | None = None
        if prev_last_block_time is not None:
            gap_seconds = latest_block_time - prev_last_block_time

        # Gap takes precedence over recovery. A gap large enough to
        # evict the entire 60s window leaves only the latest event
        # in the deque, which trivially satisfies "live_drop < 0.20"
        # (rolling_max == latest_price). That's a window-shift
        # artifact, not a real recovery, so we attribute it to the
        # gap. When neither condition is gap-truthy, drop wins
        # normally.
        end_reason: str | None = None
        live_drop: float | None = None
        if gap_seconds is not None and gap_seconds > DIP_END_GAP_SEC:
            end_reason = "gap"
        else:
            rolling_max = max(ev.price_sol for ev in buf)
            if rolling_max <= 0:
                return
            live_drop = 1.0 - (latest_price_sol / rolling_max)
            if live_drop < DIP_END_DROP_PCT:
                end_reason = "recovered"

        if end_reason is None:
            return

        self._states[token_address] = "IDLE"
        if end_reason == "recovered":
            self._stats["dips_ended_recovered"] += 1
        else:
            self._stats["dips_ended_gap"] += 1

        pending = self._pending_inserts.pop(token_address, None)

        symbol = self._symbol_cache.get(token_address)
        if end_reason == "recovered":
            tail = f"live_drop={live_drop*100:.1f}%"
        else:
            tail = f"gap={gap_seconds:.0f}s"
        logger.info(
            f"📉 fast_dip END    ${symbol or token_address[:8]} "
            f"reason={end_reason} {tail} bt={latest_block_time:.0f}"
        )

        if pending is None or self._loop is None:
            return

        async def _do_update():
            try:
                # The trigger INSERT task's result IS the row id (None on
                # failure). Awaiting it here serves both as "wait for
                # INSERT to land" and "fetch row id" without any shared
                # dict state that could be clobbered by a back-to-back
                # second dip on the same token.
                rid = await pending
                if rid is None:
                    return
                await db.update_fast_dip_dip_end(
                    row_id=rid,
                    dip_end_block_time=latest_block_time,
                    dip_end_reason=end_reason,
                    db_path=self._db_path,
                )
            except Exception as e:
                logger.error(
                    f"📉 fast_dip: UPDATE failed for {token_address}: {e}"
                )

        self._loop.create_task(_do_update())

    # ── Eviction ─────────────────────────────────────────────────────────

    def _maybe_evict_lru(self, now_wall: float) -> None:
        """If at TOKEN_CAP, drop the least-recently-touched token. If
        the evicted token is IN_DIP, force-end its dip with reason
        'evicted' so the shadow row isn't left dangling."""
        if len(self._buffers) < TOKEN_CAP:
            return
        # Pick LRU. Ties broken arbitrarily by dict iteration order.
        victim = min(self._last_touch, key=self._last_touch.get, default=None)
        if victim is None:
            return

        state = self._states.pop(victim, "IDLE")
        pending = self._pending_inserts.pop(victim, None)
        # Capture victim's most recent block_time before dropping its
        # buffer — used as the dip_end_block_time stamp for the
        # force-ended dip below.
        victim_buf = self._buffers.pop(victim, None)
        last_block_time = victim_buf[-1].block_time if victim_buf else now_wall
        self._buffer_sigs.pop(victim, None)
        self._last_touch.pop(victim, None)
        self._stats["tokens_evicted"] += 1

        if state == "IN_DIP" and pending is not None:
            self._stats["dips_ended_evicted"] += 1
            symbol = self._symbol_cache.get(victim)
            logger.info(
                f"📉 fast_dip EVICT  ${symbol or victim[:8]} "
                f"(IN_DIP, force-ending dip)"
            )
            if self._loop is None:
                return

            async def _do_evict_update():
                try:
                    rid = await pending
                    if rid is None:
                        return
                    await db.update_fast_dip_dip_end(
                        row_id=rid,
                        dip_end_block_time=last_block_time,
                        dip_end_reason="evicted",
                        db_path=self._db_path,
                    )
                except Exception as e:
                    logger.error(
                        f"📉 fast_dip: eviction UPDATE failed for {victim}: {e}"
                    )

            self._loop.create_task(_do_evict_update())

    # ── Introspection (used by tests + future stats logger) ─────────────

    def stats(self) -> dict:
        """Return a snapshot of detector counters."""
        return dict(self._stats)
