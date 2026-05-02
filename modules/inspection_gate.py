"""
modules/inspection_gate.py — Inception bundle/wash detection v2 (shadow mode).

Derives the true migration slot from MIN(slot) in pumpswap_fees (first pool event),
then queries the [derived_slot, derived_slot + window_slots] window.
Aggregates quote_amount (lamports) by buy/sell, converts SOL -> USD, labels per rule.
The WS-reported migration_slot is kept in the signature for caller compat but NOT
used for window anchoring (observed multi-thousand-slot drift).

Anchor note: the window anchor is derived from MIN(slot) in pumpswap_fees for this
token — i.e. the first PumpSwap pool event. It is NOT the bonding curve inception
on pump.fun, and NOT the WS-reported migration_slot.

Shadow mode — writes to inspection_gate_log table only. Does not block alerts.

Threshold rule (v2):
  - BUNDLE_WASH_LIKELY  if buy_usd >= min_buy_usd AND sell/buy ratio < max_sell_buy_ratio
  - THIN_DATA           if fewer than min_events events in window
  - CLEAN               otherwise
  - CHECK_FAILED        on exceptions
"""

import asyncio
import logging
import time

import aiohttp
import aiosqlite
import yaml

from database import db_connect

from utils.birdeye import get_sol_price_at, get_sol_price_now

logger = logging.getLogger(__name__)

DB_PATH = "data/bot.db"
CONFIG_PATH = "config.yaml"
LAMPORTS_PER_SOL = 1_000_000_000
SETTLE_DELAY_S = 10


def _load_gate_config() -> tuple[dict, dict]:
    """Load inspection_gate and birdeye config sections from config.yaml."""
    with open(CONFIG_PATH) as f:
        full = yaml.safe_load(f)
    return full.get("inspection_gate", {}), full.get("birdeye", {})


async def check_inception_bundle(token_address: str, migration_slot: int) -> None:
    """
    Query pumpswap_fees around migration_slot, label the inception window,
    write to inspection_gate_log.  Shadow mode — never blocks alerts.
    """
    ig_cfg, birdeye_cfg = _load_gate_config()
    if not ig_cfg.get("enabled", False):
        return

    window_slots = ig_cfg.get("window_slots", 5)
    min_buy_usd = ig_cfg.get("min_buy_usd", 10000.0)
    max_ratio = ig_cfg.get("max_sell_buy_ratio", 0.07)
    min_events = ig_cfg.get("min_events", 3)
    threshold_ver = ig_cfg.get("threshold_version", "v2")

    # Let gRPC indexer persist the window before querying
    await asyncio.sleep(SETTLE_DELAY_S)

    started_at = time.time()
    label = "CHECK_FAILED"
    error_reason = None
    buy_sol = sell_sol = buy_usd = sell_usd = 0.0
    buy_count = sell_count = 0
    sol_price = None
    sell_buy_ratio = 0.0
    block_time = None
    symbol = "???"
    anchor_slot = migration_slot      # overridden by derived slot when available
    window_end = migration_slot + window_slots

    try:
        async with db_connect(DB_PATH) as db:
            # Symbol for logging
            async with db.execute(
                "SELECT symbol FROM tokens WHERE address = ?", (token_address,)
            ) as cur:
                row = await cur.fetchone()
                if row:
                    symbol = row[0] or "???"

            # Derive actual migration slot from first PumpSwap event.
            # The WS-reported slot can drift thousands of slots from
            # the real migrate tx (it's the delivery slot, not the tx slot).
            async with db.execute(
                "SELECT MIN(slot) FROM pumpswap_fees WHERE token_address = ?",
                (token_address,),
            ) as cur:
                row = await cur.fetchone()
            derived_slot = row[0] if row and row[0] else None

            if derived_slot is None:
                # No fee rows at all — nothing to aggregate
                label = "THIN_DATA"
            else:
                anchor_slot = derived_slot
                window_end = derived_slot + window_slots

                if migration_slot != derived_slot:
                    logger.info(
                        f"inspection_gate ${symbol} slot drift: "
                        f"ws={migration_slot} db={derived_slot} "
                        f"delta={migration_slot - derived_slot}"
                    )

                # Aggregate quote_amount by event_type in the slot window
                async with db.execute("""
                    SELECT event_type,
                           COALESCE(SUM(quote_amount), 0),
                           COUNT(*)
                    FROM pumpswap_fees
                    WHERE token_address = ? AND slot BETWEEN ? AND ?
                    GROUP BY event_type
                """, (token_address, anchor_slot, window_end)) as cur:
                    fee_rows = await cur.fetchall()

                # Grab a representative block_time for SOL price lookup
                async with db.execute("""
                    SELECT MAX(block_time) FROM pumpswap_fees
                    WHERE token_address = ? AND slot BETWEEN ? AND ?
                """, (token_address, anchor_slot, window_end)) as cur:
                    bt_row = await cur.fetchone()
                    block_time = bt_row[0] if bt_row and bt_row[0] else None

        # Only aggregate + price-convert when we have fee data
        if label != "THIN_DATA":
            for etype, lamports, cnt in fee_rows:
                if etype == "buy":
                    buy_count = cnt
                    buy_sol = lamports / LAMPORTS_PER_SOL
                elif etype == "sell":
                    sell_count = cnt
                    sell_sol = lamports / LAMPORTS_PER_SOL

            total_events = buy_count + sell_count

            # SOL -> USD conversion
            api_key = birdeye_cfg.get("api_key", "")
            async with aiohttp.ClientSession() as session:
                if block_time:
                    sol_price = await get_sol_price_at(block_time, api_key, session)
                if not sol_price:
                    sol_price = await get_sol_price_now(api_key, session)

            if not sol_price:
                raise ValueError("sol_price_unavailable")

            buy_usd = buy_sol * sol_price
            sell_usd = sell_sol * sol_price
            sell_buy_ratio = (sell_usd / buy_usd) if buy_usd > 0 else 0.0

            # Apply label
            if total_events < min_events:
                label = "THIN_DATA"
            elif buy_usd >= min_buy_usd and sell_buy_ratio < max_ratio:
                label = "BUNDLE_WASH_LIKELY"
            else:
                label = "CLEAN"

    except Exception as e:
        label = "CHECK_FAILED"
        error_reason = f"{type(e).__name__}: {str(e)[:200]}"
        logger.exception(f"inspection_gate error for {token_address[:8]}: {e}")

    completed_at = time.time()
    latency_ms = int((completed_at - started_at) * 1000)

    await _write_log_row(
        token_address=token_address,
        symbol=symbol,
        migration_slot=anchor_slot,
        block_time=block_time,
        window_end=window_end,
        buy_count=buy_count,
        sell_count=sell_count,
        buy_sol=buy_sol,
        sell_sol=sell_sol,
        sol_price=sol_price,
        buy_usd=buy_usd,
        sell_usd=sell_usd,
        sell_buy_ratio=sell_buy_ratio,
        label=label,
        threshold_version=threshold_ver,
        started_at=int(started_at),
        completed_at=int(completed_at),
        latency_ms=latency_ms,
        error_reason=error_reason,
    )

    logger.info(
        f"inspection_gate ${symbol} -> {label} "
        f"(buy=${buy_usd:,.0f}, ratio={sell_buy_ratio:.3f}, "
        f"events={buy_count + sell_count}, {latency_ms}ms)"
    )


async def _write_log_row(
    token_address: str, symbol: str,
    migration_slot: int, block_time, window_end: int,
    buy_count: int, sell_count: int,
    buy_sol: float, sell_sol: float, sol_price, buy_usd: float,
    sell_usd: float, sell_buy_ratio: float,
    label: str, threshold_version: str,
    started_at: int, completed_at: int, latency_ms: int,
    error_reason,
) -> None:
    """Insert one inspection_gate_log row. Reuses existing column names."""
    try:
        async with db_connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO inspection_gate_log (
                    token_address, symbol,
                    inception_slot, inception_block_time, window_end_slot,
                    buy_count, sell_count,
                    buy_sol, sell_sol, gross_sol, net_sol,
                    sol_price_usd, buy_usd, sell_usd, sell_to_buy_ratio,
                    label, threshold_version,
                    check_started_at, check_completed_at, check_latency_ms,
                    rpc_calls_made, retry_attempted, error_reason, alert_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    token_address, symbol,
                    migration_slot, block_time, window_end,
                    buy_count, sell_count,
                    buy_sol, sell_sol,
                    buy_sol + sell_sol,      # gross_sol
                    buy_sol - sell_sol,      # net_sol
                    sol_price, buy_usd, sell_usd, sell_buy_ratio,
                    label, threshold_version,
                    started_at, completed_at, latency_ms,
                    0, 0, error_reason, None,   # rpc_calls=0, retry=0, no alert_id
                ),
            )
            await db.commit()
    except Exception as e:
        logger.error(f"inspection_gate DB write failed for {token_address[:8]}: {e}")
