"""
modules/ath_seeder.py — Shared Birdeye ATH seeding for newly tracked tokens.

Single source of truth for "give this token an authoritative ATH from
Birdeye OHLCV right after it lands in the tokens table." Called by:
  - migration_ws._build_token (live WS path)
  - backfill.backfill_recent_migrations (startup catch-up)
  - backfill.periodic_backfill_loop (safety-net polling)
  - price_tracker._process_token (promotion guard, via enqueue_retry)

Module-level retry queue with idempotent enqueue backs all callers.
migration_ws.process_ath_retry_queue() is preserved as a thin wrapper
that delegates here, so main.py is unchanged.

Rate limit: 5 calls/sec to Birdeye, enforced module-wide via the
_rate_limit() helper wrapping every Birdeye call in this module.

Resolution caveat: Birdeye OHLCV resolution is selected by token age at
seed time. 1m candles for <20 min, 15m for 20m-2h, coarser thereafter.
Backfill catches that arrive 20+ minutes after migration get coarser
candles and may approximate sub-15m peaks. Acceptable degraded behavior;
the alternative is no Birdeye data at all.

Queue structure: dict[str, dict] keyed by token address. Dict keys
provide O(1) idempotency for enqueue_retry — no separate set needed.
Entry shape:
  {"queued_at": float, "last_attempt": float,
   "first_success_at": Optional[float]}
"""

import asyncio
import logging
import time
from typing import Optional

import aiohttp

import database as db
from models import TrackedToken
from utils.birdeye import get_ath_since_migration
from modules import ath_refresh_shadow
from modules.phantom_validator import validate_current_after_ath_update

logger = logging.getLogger(__name__)


# ── Module-level retry queue ──────────────────────────────────────────────
_ath_retry_queue: dict[str, dict] = {}


# ── Birdeye rate limiter (5 calls/sec, single module-wide limiter) ────────
_RATE_LIMIT_INTERVAL = 0.200  # 200ms = 5 calls/sec
_last_call_ts: float = 0.0
_rate_lock: asyncio.Lock = asyncio.Lock()


async def _rate_limit() -> None:
    """Enforce ≥200ms between Birdeye seed calls across all tasks."""
    global _last_call_ts
    async with _rate_lock:
        delta = time.time() - _last_call_ts
        if delta < _RATE_LIMIT_INTERVAL:
            await asyncio.sleep(_RATE_LIMIT_INTERVAL - delta)
        _last_call_ts = time.time()


async def rehydrate_retry_queue(db_path: str, config: dict) -> None:
    """Rebuild the in-memory retry queue from the persisted table on
    startup, dropping entries that are past the max_age ceiling or that
    the tokens table now reports as authoritatively seeded.

    Called once from main() before the price loop starts. Without this,
    every restart silently drops in-flight retries into running_max,
    since Phoenix sees ~13 process boundaries per 9 days.
    """
    now = time.time()
    retry_cfg  = (config or {}).get("ath_retry", {}) or {}
    max_age    = retry_cfg.get("max_age_seconds", 1800)

    rows = await db.load_ath_retry_queue(db_path)

    kept = 0
    pruned = 0
    for address, entry in rows.items():
        queued_at = entry.get("queued_at", now)

        # Prune by age first (cheap), then by ath_source (per-row DB hit).
        if (now - queued_at) > max_age:
            await db.delete_ath_retry(address)
            pruned += 1
            continue

        token = await db.get_token(address)
        if token is None or token.ath_source in ("birdeye", "birdeye_corrected"):
            await db.delete_ath_retry(address)
            pruned += 1
            continue

        _ath_retry_queue[address] = entry
        kept += 1

    logger.info(f"Rehydrated ATH retry queue: {kept} kept, {pruned} pruned")


async def enqueue_retry(token: TrackedToken) -> bool:
    """Add token to the Birdeye retry queue. Idempotent on token.address.

    Returns True if newly added, False if already queued.
    """
    if token.address in _ath_retry_queue:
        return False
    now = time.time()
    _ath_retry_queue[token.address] = {
        "queued_at": now,
        "last_attempt": now,
    }
    await db.upsert_ath_retry(token.address, now, now, None)
    return True


async def _run_phantom_validation(
    token: TrackedToken,
    session: aiohttp.ClientSession,
    config: dict,
) -> None:
    """Run phantom_validator after a Birdeye ATH update and persist any
    cooldown the validator decides on.

    Always logs the validation outcome (positive AND negative) to
    phantom_abort_log so the week-1 review has both populations.
    Honours the `phantom_validator.enabled` kill switch in config.
    Fail-open: any error logging or persisting must not propagate up
    into the price loop — a phantom miss is preferable to a crashed
    ATH-update path.
    """
    pv_cfg = (config or {}).get("phantom_validator", {}) or {}
    if not pv_cfg.get("enabled", True):
        return
    try:
        is_phantom, log_data = await validate_current_after_ath_update(
            token, session, config
        )
    except Exception as e:
        logger.warning(
            f"phantom_validator unexpected error for ${token.symbol}: {e!r}"
        )
        return

    try:
        await db.log_phantom_validation(log_data)
    except Exception as e:
        logger.warning(
            f"phantom_abort_log insert failed for ${token.symbol}: {e!r}"
        )

    if is_phantom:
        cooldown_secs = float(pv_cfg.get("cooldown_seconds", 120))
        token.phantom_cooldown_until = time.time() + cooldown_secs
        try:
            await db.save_token(token)
        except Exception as e:
            logger.warning(
                f"failed to persist phantom_cooldown_until for ${token.symbol}: {e!r}"
            )


async def seed_ath_for_token(
    token: TrackedToken,
    http_session: aiohttp.ClientSession,
    config: dict,
) -> None:
    """Seed ATH from Birdeye immediately after migration detection.

    Success → ath_source = 'birdeye'. No retry needed.
    Falsy    → ath_source stays 'unseeded'; queue for retry. Do NOT set
               'fallback' here — fallback is a price_tracker responsibility
               that only kicks in when a live poll finds ath_price <= 0.
    """
    try:
        await _rate_limit()
        birdeye_ath = await get_ath_since_migration(
            token_address=token.address,
            migration_time=token.migration_time,
            api_key=config["birdeye"]["api_key"],
            session=http_session,
        )

        if birdeye_ath and birdeye_ath > 0:
            token.ath_price = birdeye_ath
            if token.current_price > 0:
                token.ath_mcap = token.current_mcap * (birdeye_ath / token.current_price)
            else:
                token.ath_mcap = token.current_mcap
            token.ath_time   = time.time()
            token.ath_source = "birdeye"
            await db.save_token(token)
            ath_refresh_shadow.observe_seed(token, birdeye_ath, "birdeye")
            logger.info(
                f"📈 ${token.symbol} ATH seeded on detection: "
                f"${birdeye_ath:.10f} (mcap ~${token.ath_mcap:,.0f})"
            )
            await _run_phantom_validation(token, http_session, config)
        else:
            # Birdeye not indexed yet — persist 'unseeded' and queue retry.
            now = time.time()
            token.ath_source = "unseeded"
            await db.save_token(token)
            ath_refresh_shadow.observe_seed(token, 0, "unseeded")
            _ath_retry_queue[token.address] = {
                "queued_at": now,
                "last_attempt": now,
            }
            await db.upsert_ath_retry(token.address, now, now, None)
            logger.info(
                f"⏳ ${token.symbol} queued for ATH retry (Birdeye not ready yet)"
            )

    except Exception as e:
        logger.error(f"ATH seed error for ${token.symbol}: {e}")


async def refresh_ath_at_alert(
    token: TrackedToken,
    http_session: aiohttp.ClientSession,
    config: dict,
) -> bool:
    """Alert-time Birdeye ATH refresh. Closes the Dex-poll cadence gap.

    Fetches Birdeye OHLCV from migration_time to now, takes the max.
    If max > token.ath_price, mutates the token in-memory AND persists.
    Returns True if ATH was raised, False if no change or on error.

    Fail-open: any exception is logged and returns False so the alert
    still fires with whatever ATH was previously known.
    """
    await _rate_limit()
    try:
        new_birdeye_ath = await get_ath_since_migration(
            token_address=token.address,
            migration_time=token.migration_time,
            api_key=config["birdeye"]["api_key"],
            session=http_session,
        )
    except Exception as e:
        logger.warning(
            f"Alert-time ATH refresh Birdeye error for ${token.symbol}: {e!r}"
        )
        return False

    if not new_birdeye_ath or new_birdeye_ath <= token.ath_price:
        return False

    previous_price = token.ath_price
    previous_mcap = token.ath_mcap
    token.ath_price = new_birdeye_ath
    token.ath_mcap = (
        token.current_mcap * (new_birdeye_ath / token.current_price)
        if token.current_price > 0
        else 0.0
    )
    token.ath_time = time.time()
    token.ath_source = "birdeye_alert_refresh"
    await db.save_token(token)
    gap_pct = (
        (new_birdeye_ath / previous_price - 1) * 100
        if previous_price > 0
        else 0.0
    )
    logger.info(
        f"ALERT_REFRESH_HIT: ${token.symbol} "
        f"ath_price {previous_price:.10f} -> {new_birdeye_ath:.10f} "
        f"(mcap ${previous_mcap:,.0f} -> ${token.ath_mcap:,.0f}, "
        f"gap_pct={gap_pct:.1f}%)"
    )
    return True


async def process_retry_queue(
    http_session: aiohttp.ClientSession,
    config: dict,
) -> int:
    """Retry Birdeye ATH seeding for tokens that were too fresh on first try.

    Cadence is tiered (from config.yaml ath_retry):
      - age ≤ initial_window  → retry every initial_interval_seconds  (hot)
      - age  > initial_window → retry every sustained_interval_seconds (cold)
      - age  > max_age        → give up; 'fallback'/'unseeded' → 'running_max'

    Exit reasons, in priority order:
      1. age > max_age            — transition source + remove
      2. ath_source == 'birdeye'  — another path already seeded authoritatively
      3. Birdeye returned a hit   — seed/correct and remove
      (default: update last_attempt, keep the entry)

    Returns the number of queue entries processed (for caller logging).
    """
    if not _ath_retry_queue:
        return 0

    now = time.time()
    to_remove = []
    processed = 0

    retry_cfg = config.get("ath_retry", {}) or {}
    initial_interval   = retry_cfg.get("initial_interval_seconds", 30)
    initial_window     = retry_cfg.get("initial_window_seconds", 600)
    sustained_interval = retry_cfg.get("sustained_interval_seconds", 120)
    max_age            = retry_cfg.get("max_age_seconds", 1800)
    reseed_window      = retry_cfg.get("reseed_window_seconds", 600)

    for address, entry in list(_ath_retry_queue.items()):
        queued_at        = entry.get("queued_at", now)
        last_attempt     = entry.get("last_attempt", 0.0)
        first_success_at = entry.get("first_success_at")
        in_reseed_mode = first_success_at is not None

        token = await db.get_token(address)
        if not token:
            to_remove.append(address)
            continue

        # Use migration_time as the canonical age reference. It's tied to
        # the token, not to when migration_ws happened to notice it, so it
        # stays stable across restarts and re-queues.
        age = (
            now - token.migration_time
            if token.migration_time > 0
            else now - queued_at
        )

        # (1) Age ceiling — transition source, drop from queue
        if age > max_age:
            logger.warning(
                f"ATH retry exhausted for ${token.symbol} after "
                f"{age/60:.1f}min; running_max ATH in effect "
                f"(ath_price=${token.ath_price:.10f})"
            )
            if token.ath_source in ("unseeded", "fallback"):
                token.ath_source = "running_max"
                await db.save_token(token)
            to_remove.append(address)
            processed += 1
            continue

        # (2) Birdeye has seeded this token — decide whether to keep
        # re-querying (reseed mode) or drop from the queue. Reseed mode
        # catches ATH updates as new 1m candles close on Birdeye's side.
        if token.ath_source in ("birdeye", "birdeye_reseeded"):
            reseed_expired = (
                age > reseed_window
                or (in_reseed_mode and (now - first_success_at) > reseed_window)
            )
            if reseed_expired:
                logger.info(
                    f"📈 ${token.symbol} ATH reseed window expired — "
                    f"removing from retry queue"
                )
                to_remove.append(address)
                processed += 1
                continue
            # else: fall through to cadence gate + re-query

        # Cadence gate — hot window vs sustained window.
        # Once in reseed mode, always use sustained cadence.
        if in_reseed_mode:
            required_interval = sustained_interval
        else:
            required_interval = (
                initial_interval if age <= initial_window else sustained_interval
            )
        if now - last_attempt < required_interval:
            continue

        # (3) Attempt Birdeye
        try:
            await _rate_limit()
            birdeye_ath = await get_ath_since_migration(
                token_address=token.address,
                migration_time=token.migration_time,
                api_key=config["birdeye"]["api_key"],
                session=http_session,
            )

            if birdeye_ath and birdeye_ath > 0:
                previous_ath    = token.ath_price
                previous_source = token.ath_source
                previous_mcap   = token.ath_mcap

                if in_reseed_mode or previous_source in ("birdeye", "birdeye_reseeded"):
                    # Reseed path — only accept a strictly higher ATH.
                    # Birdeye returning <= current ATH is a no-op.
                    if birdeye_ath > previous_ath:
                        token.ath_price = birdeye_ath
                        if token.current_price > 0:
                            token.ath_mcap = token.current_mcap * (birdeye_ath / token.current_price)
                        else:
                            token.ath_mcap = token.current_mcap
                        token.ath_time   = time.time()
                        token.ath_source = "birdeye_reseeded"
                        await db.save_token(token)
                        ath_refresh_shadow.observe_seed(token, birdeye_ath, "birdeye_reseeded")
                        logger.info(
                            f"📈 ${token.symbol} ATH reseeded: "
                            f"${token.ath_mcap:,.0f} (was ${previous_mcap:,.0f})"
                        )
                        await _run_phantom_validation(token, http_session, config)
                    # Keep entry alive so reseed_window bounds the loop.
                    new_first_success_at = first_success_at or now
                    _ath_retry_queue[address] = {
                        "queued_at": queued_at,
                        "last_attempt": now,
                        "first_success_at": new_first_success_at,
                    }
                    await db.upsert_ath_retry(
                        address, queued_at, now, new_first_success_at
                    )
                else:
                    # First-time success — never regress a running-max
                    # ath_price that may already be higher (especially
                    # for 'fallback'-seeded tokens).
                    corrected_ath   = max(previous_ath, birdeye_ath)
                    token.ath_price = corrected_ath
                    if token.current_price > 0:
                        token.ath_mcap = token.current_mcap * (corrected_ath / token.current_price)
                    else:
                        token.ath_mcap = token.current_mcap
                    token.ath_time   = time.time()
                    token.ath_source = "birdeye"
                    await db.save_token(token)
                    ath_refresh_shadow.observe_seed(token, corrected_ath, "birdeye")
                    if previous_source == "fallback":
                        logger.info(
                            f"📈 ${token.symbol} ATH corrected via Birdeye retry: "
                            f"${token.ath_mcap:,.0f} mcap "
                            f"(was ${previous_ath:.10f} from fallback, "
                            f"birdeye=${birdeye_ath:.10f})"
                        )
                    else:
                        logger.info(
                            f"📈 ${token.symbol} ATH seeded on retry: "
                            f"${birdeye_ath:.10f} (mcap ~${token.ath_mcap:,.0f})"
                        )
                    await _run_phantom_validation(token, http_session, config)
                    # Keep in queue for reseeding if still young;
                    # otherwise exit cleanly.
                    if age < reseed_window:
                        _ath_retry_queue[address] = {
                            "queued_at": queued_at,
                            "last_attempt": now,
                            "first_success_at": now,
                        }
                        await db.upsert_ath_retry(
                            address, queued_at, now, now
                        )
                    else:
                        to_remove.append(address)
            else:
                # Still no data — bump last_attempt only. queued_at is
                # preserved so the max_age ceiling can't be reset by
                # chained failed retries (fixes the prior bug where
                # resetting queued_time would extend the window forever).
                _ath_retry_queue[address] = {
                    "queued_at": queued_at,
                    "last_attempt": now,
                }
                await db.upsert_ath_retry(address, queued_at, now, None)
                logger.info(f"No Birdeye ATH yet for ${token.symbol} — will retry")

            processed += 1

        except Exception as e:
            logger.error(f"ATH retry error for {address[:8]}: {e}")
            _ath_retry_queue[address] = {
                "queued_at": queued_at,
                "last_attempt": now,
            }
            await db.upsert_ath_retry(address, queued_at, now, None)

    for address in to_remove:
        _ath_retry_queue.pop(address, None)
        await db.delete_ath_retry(address)

    return processed


async def process_t15m_correction(
    http_session: aiohttp.ClientSession,
    config: dict,
) -> int:
    """One-shot T+15m correction pass for already-seeded tokens.

    For tokens that have already exited the retry queue: if a 15m
    candle closed shortly after the reseed window expired and reveals
    a higher peak than what the 1m-candle loop captured, correct it.
    Uses a 60s window (correction_delay_seconds .. +60) so a token
    gets at most one correction attempt per poll interval.

    Independent of the retry queue — operates on db.load_all_tokens()
    and runs on every poll, even when the queue is empty.

    Returns the number of corrections applied (for caller logging).
    """
    logger.debug("T+15m correction pass entering")
    now = time.time()
    corrections = 0

    retry_cfg = config.get("ath_retry", {}) or {}
    correction_delay = retry_cfg.get("correction_delay_seconds", 900)
    correction_window_end = correction_delay + 60

    try:
        all_tokens = await db.load_all_tokens()
    except Exception as e:
        logger.debug(f"T+15m correction pass skipped (db load error): {e}")
        all_tokens = []

    for token in all_tokens:
        if token.ath_source not in ("birdeye", "birdeye_reseeded", "birdeye_running_max"):
            continue
        if token.migration_time <= 0:
            continue
        token_age = now - token.migration_time
        if not (correction_delay <= token_age <= correction_window_end):
            continue

        try:
            await _rate_limit()
            birdeye_ath = await get_ath_since_migration(
                token_address=token.address,
                migration_time=token.migration_time,
                api_key=config["birdeye"]["api_key"],
                session=http_session,
                resolution="15m",
            )
            if birdeye_ath and birdeye_ath > token.ath_price:
                previous_mcap = token.ath_mcap
                token.ath_price = birdeye_ath
                if token.current_price > 0:
                    token.ath_mcap = token.current_mcap * (birdeye_ath / token.current_price)
                else:
                    token.ath_mcap = token.current_mcap
                token.ath_time   = time.time()
                token.ath_source = "birdeye_corrected"
                await db.save_token(token)
                logger.info(
                    f"📈 ${token.symbol} ATH corrected at T+15m: "
                    f"${token.ath_mcap:,.0f} (was ${previous_mcap:,.0f})"
                )
                await _run_phantom_validation(token, http_session, config)
                corrections += 1
        except Exception as e:
            logger.debug(f"T+15m correction error for {token.address[:8]}: {e}")

    return corrections
