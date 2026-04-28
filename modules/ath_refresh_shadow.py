"""
modules/ath_refresh_shadow.py
Logging-only instrumentation that records what a future adaptive ATH-refresh
system WOULD do (delta triggers, scheduled re-seeds, concurrent snapshots).
NEVER calls Birdeye. NEVER changes any decision path. Purely additive.

event_type values in ath_refresh_shadow_log:
    delta_trigger          Dex-price delta >= taper threshold (would-trigger)
    seed_reseed_60s        T+60s scheduled re-seed (shadow-only)
    seed_reseed_5min       T+300s scheduled re-seed (shadow-only)
    status_transition      token.status changed (incl. initial creation)
    concurrent_snapshot    hourly per-status token counts
    real_seed_observed     price_tracker actually seeded ATH from Birdeye
    session_start          bot started with shadow enabled (restart counter)

Gated by config.ath_refresh_shadow.enabled. Auto-disables 48h after first
log entry to prevent forgotten accumulation. To remove entirely: flip
enabled:false, delete this file + its call sites, drop the log table.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)

# ── Module-level state ────────────────────────────────────────────────────
# In-memory baseline price per token for delta checks. Resets on restart —
# the session_start counter in the log table makes drift quantifiable.
_last_refresh_price: dict[str, float] = {}
_enabled: bool = False
_cfg: dict = {}
_db_path: str = "data/bot.db"


# ── Schema ────────────────────────────────────────────────────────────────
async def _init_schema(db_path: str):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ath_refresh_shadow_log (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address           TEXT NOT NULL,
                logged_at               REAL NOT NULL,
                event_type              TEXT NOT NULL,
                token_age_seconds       INTEGER,
                token_status            TEXT,
                current_price           REAL,
                previous_refresh_price  REAL,
                delta_pct               REAL,
                threshold_used_pct      REAL,
                taper_phase             TEXT,
                migration_time          REAL,
                notes                   TEXT
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_shadow_token      ON ath_refresh_shadow_log(token_address)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_shadow_logged_at  ON ath_refresh_shadow_log(logged_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_shadow_event_type ON ath_refresh_shadow_log(event_type)")
        await db.commit()


# ── Startup ───────────────────────────────────────────────────────────────
async def startup_check(db_path: str, config: dict) -> bool:
    """Init schema + decide enablement. Auto-disables if first log > 48h old."""
    global _enabled, _cfg, _db_path
    _cfg = (config.get("ath_refresh_shadow") or {})
    _db_path = db_path

    if not _cfg.get("enabled", False):
        logger.info("ATH refresh shadow: disabled via config")
        _enabled = False
        return False

    await _init_schema(db_path)

    auto_h = _cfg.get("auto_disable_after_hours", 48)
    try:
        async with aiosqlite.connect(db_path) as d:
            async with d.execute(
                "SELECT MIN(logged_at) FROM ath_refresh_shadow_log"
            ) as cur:
                row = await cur.fetchone()
        first_at = row[0] if row else None
        if first_at:
            age_h = (time.time() - float(first_at)) / 3600.0
            if age_h > auto_h:
                logger.warning(
                    f"ATH refresh shadow auto-disabled: has been running "
                    f"{age_h:.1f}h > cap {auto_h}h. Flip enabled:false in "
                    "config or clear ath_refresh_shadow_log to re-arm."
                )
                _enabled = False
                return False
    except Exception as e:
        logger.warning(f"Shadow startup_check non-fatal error: {e}")

    _enabled = True
    logger.info(
        f"ATH refresh shadow ENABLED | "
        f"h1={_cfg.get('hour_1_threshold_pct', 10)}% "
        f"h2+={_cfg.get('hour_2_plus_threshold_pct', 20)}% "
        f"reseed=T+{_cfg.get('reseed_times_seconds', [60, 300])}s"
    )
    await _log_session_start()
    return True


def is_enabled() -> bool:
    return _enabled


# ── Internals ─────────────────────────────────────────────────────────────
def _taper_for_age(age_s: float) -> tuple[float, str]:
    if age_s < 3600:
        return float(_cfg.get("hour_1_threshold_pct", 10.0)), "hour_1"
    return float(_cfg.get("hour_2_plus_threshold_pct", 20.0)), "hour_2_plus"


def _seed_baseline(address: str, price: float):
    if price > 0:
        _last_refresh_price[address] = price


async def _write_row(**row):
    """Fire-and-forget insert. Never raises."""
    if not _enabled:
        return
    try:
        async with aiosqlite.connect(_db_path) as d:
            await d.execute("""
                INSERT INTO ath_refresh_shadow_log (
                    token_address, logged_at, event_type,
                    token_age_seconds, token_status,
                    current_price, previous_refresh_price,
                    delta_pct, threshold_used_pct, taper_phase,
                    migration_time, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get("token_address", "SYSTEM"),
                row.get("logged_at", time.time()),
                row["event_type"],
                row.get("token_age_seconds"),
                row.get("token_status"),
                row.get("current_price"),
                row.get("previous_refresh_price"),
                row.get("delta_pct"),
                row.get("threshold_used_pct"),
                row.get("taper_phase"),
                row.get("migration_time"),
                row.get("notes"),
            ))
            await d.commit()
    except Exception as e:
        logger.warning(f"Shadow write failed ({row.get('event_type')}): {e}")


async def _log_session_start():
    """Write a session_start row and print cumulative session count."""
    prior = 0
    try:
        async with aiosqlite.connect(_db_path) as d:
            async with d.execute(
                "SELECT COUNT(*) FROM ath_refresh_shadow_log WHERE event_type='session_start'"
            ) as cur:
                row = await cur.fetchone()
        prior = int(row[0]) if row and row[0] is not None else 0
    except Exception as e:
        logger.warning(f"session_start count failed: {e}")

    session_num = prior + 1
    logger.info(
        f"ATH refresh shadow: bot session #{session_num} "
        f"(restart count informs reseed drift)"
    )
    try:
        await _write_row(
            token_address="SYSTEM",
            logged_at=time.time(),
            event_type="session_start",
            notes=json.dumps({"session_num": session_num}),
        )
    except Exception as e:
        logger.warning(f"session_start write failed: {e}")


# ── Public hooks ──────────────────────────────────────────────────────────
def observe_token_created(token):
    """Called on the first save_token() after migration detection."""
    if not _enabled:
        return
    _seed_baseline(token.address, token.current_price)


def observe_seed(token, price: float, seed_source: str):
    """Called when price_tracker does a real Birdeye seed (success or fallback).
    Resets the baseline AND writes a real_seed_observed row so post-analysis
    can separate seed-driven refreshes from delta-driven refreshes.

    seed_source in {'birdeye', 'fallback'} — recorded in notes JSON.
    """
    if not _enabled:
        return
    addr = token.address
    prev = _last_refresh_price.get(addr)
    _seed_baseline(addr, price)

    if token.migration_time and token.migration_time > 0:
        age = time.time() - token.migration_time
        threshold, phase = _taper_for_age(age)
        age_s = int(age)
    else:
        age_s, threshold, phase = None, None, None

    status_s = token.status.value if hasattr(token.status, "value") else str(token.status)
    asyncio.create_task(_write_row(
        token_address=addr,
        logged_at=time.time(),
        event_type="real_seed_observed",
        token_age_seconds=age_s,
        token_status=status_s,
        current_price=price,
        previous_refresh_price=prev,
        delta_pct=None,
        threshold_used_pct=threshold,
        taper_phase=phase,
        migration_time=token.migration_time,
        notes=json.dumps({"symbol": token.symbol, "seed_source": seed_source}),
    ))


def check_delta(token):
    """Per-poll. Logs a delta_trigger row if taper threshold hit."""
    if not _enabled or token.current_price <= 0 or token.migration_time <= 0:
        return
    addr = token.address
    prev = _last_refresh_price.get(addr)
    if prev is None or prev <= 0:
        _seed_baseline(addr, token.current_price)
        return

    age = time.time() - token.migration_time
    if age < 0:
        return

    delta_pct = ((token.current_price - prev) / prev) * 100.0
    threshold, phase = _taper_for_age(age)
    if abs(delta_pct) < threshold:
        return

    _seed_baseline(addr, token.current_price)  # reset after trigger

    status_s = token.status.value if hasattr(token.status, "value") else str(token.status)
    asyncio.create_task(_write_row(
        token_address=addr,
        logged_at=time.time(),
        event_type="delta_trigger",
        token_age_seconds=int(age),
        token_status=status_s,
        current_price=token.current_price,
        previous_refresh_price=prev,
        delta_pct=delta_pct,
        threshold_used_pct=threshold,
        taper_phase=phase,
        migration_time=token.migration_time,
        notes=f"symbol={token.symbol}",
    ))


def schedule_reseeds(address: str, migration_time: float, symbol: str):
    """Schedule two shadow-only reseed log events at T+60s and T+5min."""
    if not _enabled or migration_time <= 0:
        return
    reseed_times = _cfg.get("reseed_times_seconds", [60, 300])
    now = time.time()
    age_now = now - migration_time
    for t_sec in reseed_times:
        delay = t_sec - age_now
        if delay <= 0:
            continue
        evt = "seed_reseed_60s" if t_sec <= 60 else "seed_reseed_5min"
        asyncio.create_task(
            _deferred_reseed(address, migration_time, symbol, delay, evt, t_sec)
        )


async def _deferred_reseed(address, migration_time, symbol, delay, event_type, target_age):
    try:
        await asyncio.sleep(delay)
        if not _enabled:
            return
        import database as db_mod  # lazy to avoid circular
        token = await db_mod.get_token(address)
        if not token:
            return
        age = time.time() - migration_time
        threshold, phase = _taper_for_age(age)
        price = token.current_price
        prev = _last_refresh_price.get(address)
        _seed_baseline(address, price)
        status_s = token.status.value if hasattr(token.status, "value") else str(token.status)
        await _write_row(
            token_address=address,
            logged_at=time.time(),
            event_type=event_type,
            token_age_seconds=int(age),
            token_status=status_s,
            current_price=price,
            previous_refresh_price=prev,
            delta_pct=None,
            threshold_used_pct=threshold,
            taper_phase=phase,
            migration_time=migration_time,
            notes=f"symbol={symbol} target_age_s={target_age}",
        )
    except Exception as e:
        logger.warning(f"Shadow deferred reseed ({event_type}) failed: {e}")


def log_status_transition(address, old_status, new_status, migration_time, symbol):
    """Log any status change. old_status=None for initial creation."""
    if not _enabled:
        return
    age_s = int(time.time() - migration_time) if migration_time and migration_time > 0 else None
    notes = json.dumps({
        "from": str(old_status) if old_status is not None else None,
        "to": str(new_status),
        "symbol": symbol,
    })
    asyncio.create_task(_write_row(
        token_address=address,
        logged_at=time.time(),
        event_type="status_transition",
        token_age_seconds=age_s,
        token_status=str(new_status),
        migration_time=migration_time,
        notes=notes,
    ))


async def shadow_snapshot_loop(db_path: str):
    """Hourly concurrent-token snapshot. Returns immediately if disabled."""
    if not _enabled:
        return
    interval = int(_cfg.get("snapshot_interval_seconds", 3600))
    now = time.time()
    next_tick = (int(now // interval) + 1) * interval
    try:
        await asyncio.sleep(max(1.0, next_tick - now))
    except asyncio.CancelledError:
        raise

    while _enabled:
        try:
            async with aiosqlite.connect(db_path) as d:
                async with d.execute(
                    "SELECT status, COUNT(*) FROM tokens GROUP BY status"
                ) as cur:
                    counts = {row[0]: row[1] async for row in cur}
            await _write_row(
                token_address="SYSTEM",
                logged_at=time.time(),
                event_type="concurrent_snapshot",
                notes=json.dumps({"counts": counts}),
            )
        except Exception as e:
            logger.warning(f"Shadow snapshot loop error: {e}")

        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
