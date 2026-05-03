"""
main.py — Solana Dip Bot v2
Entry point. Runs:
  0. Backfill          — catches migrations missed during downtime
  1. Migration WebSocket — real-time pump.fun → PumpSwap migration detection
  2. Price tracker       — updates prices + ATH for all tracked tokens
  3. Alert trigger       — fires Telegram alerts at dip tiers
  4. Periodic backfill   — safety net polling for missed WebSocket events
  5. Daily recap         — posts performance summary to Telegram at midnight CST
  6. Command listener    — responds to /stats, /perf, /perf7 in Telegram
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import aiohttp
import aiosqlite
import yaml

import database as db
from modules.migration_ws      import MigrationWebSocket
from modules.backfill           import backfill_recent_migrations, periodic_backfill_loop
from modules.price_tracker      import PriceTracker
from modules.alert_trigger      import AlertTrigger
from modules.telegram_sender    import TelegramSender
from modules                    import alert_gate
from modules.fast_dip_detector  import FastDipDetector
from snapshot_holders            import snapshot_top_holders
from holder_filter               import evaluate_holder_filter, log_holder_filter, get_recent_filter_result, mark_actually_blocked

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")

CST = timezone(timedelta(hours=-6))


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


async def price_alert_loop(
    migration_ws: MigrationWebSocket,
    tracker: PriceTracker,
    trigger: AlertTrigger,
    sender: TelegramSender,
    config: dict,
    session: aiohttp.ClientSession,
    db_path: str,
):
    """Loop that updates prices, checks dip alert tiers, and processes retry queue.

    Cadence split (Part 2 shadow mode):
      - Loop tick: every SHADOW_LOOP_INTERVAL_S seconds (10s).
      - Dexscreener poll + retry queues: every DEX_CYCLE_MOD cycles (~30s —
        matches config.tracking.poll_interval_seconds so external API load
        is unchanged from pre-shadow behaviour).
      - gRPC shadow peak lookup (fresh tokens): every tick (local SQLite).
      - Alert trigger: every tick, unchanged behaviour. ath_price is still
        Dex-sourced; gRPC is observe-only.
    """
    # Match Dex cadence to the legacy poll interval so external API load
    # stays identical to pre-shadow behaviour.
    SHADOW_LOOP_INTERVAL_S = 10
    dex_poll_interval = config.get("tracking", {}).get("poll_interval_seconds", 30)
    DEX_CYCLE_MOD = max(1, round(dex_poll_interval / SHADOW_LOOP_INTERVAL_S))

    cycle_count = 0

    while True:
        try:
            tokens = await db.load_all_tokens(db_path)

            if tokens:
                # ── Dexscreener poll (every DEX_CYCLE_MOD cycles) ─────
                # External API — gated to preserve the ~30s cadence and
                # avoid rate limits. The 10s tick only speeds up local
                # gRPC peak reads, not Dexscreener.
                if cycle_count % DEX_CYCLE_MOD == 0:
                    newly_confirmed = await tracker.update_prices(tokens, session)
                    if newly_confirmed:
                        logger.info(
                            f"✅ {len(newly_confirmed)} token(s) crossed {tracker.min_pump_multiple}x threshold"
                        )
                    # Reload after saves (status may have changed)
                    tokens = await db.load_all_tokens(db_path)

                # Check dip tiers
                to_alert = trigger.check_tokens(tokens)
                for token, tier in to_alert:
                    tier_index = tier["index"]
                    logger.info(
                        f"🔔 Alerting ${token.symbol} | "
                        f"{tier['name']} | -{token.drop_from_ath*100:.0f}% from ATH"
                    )
                    ghost_filter_result = None
                    hf_log_row_id = None
                    ping_ts = time.time()

                    gate_result = await alert_gate.evaluate(
                        token, tier, ping_ts, sender, db, config
                    )
                    if not gate_result.allow:
                        logger.info(
                            f"Alert blocked: ${token.symbol} ({token.address}) "
                            f"tier={tier_index} reason={gate_result.block_reason}"
                        )
                        continue

                    if tier_index == 0:
                        # T1: always live snapshot + filter
                        snapshot = await snapshot_top_holders(
                            token_mint=token.address,
                            tier="T1",
                            ping_time=ping_ts,
                            pool_address=token.pool_address,
                            symbol=token.symbol,
                            decimals=6,
                        )
                        if (snapshot.get("snapshot_status") != "error"
                                and snapshot.get("holders")):
                            ghost_filter_result = evaluate_holder_filter(snapshot)
                            hf_log_row_id = await log_holder_filter(
                                token_address=token.address,
                                alert_time=ping_ts,
                                snapshot_id=None,
                                result=ghost_filter_result,
                            )

                    else:
                        # T2/T3: carry forward if fresh, else live refresh
                        cached = await get_recent_filter_result(token.address)
                        if cached is not None:
                            ghost_filter_result = cached
                        else:
                            tier_label = f"T{tier_index + 1}"
                            snapshot = await snapshot_top_holders(
                                token_mint=token.address,
                                tier=tier_label,
                                ping_time=ping_ts,
                                pool_address=token.pool_address,
                                symbol=token.symbol,
                                decimals=6,
                            )
                            if (snapshot.get("snapshot_status") != "error"
                                    and snapshot.get("holders")):
                                ghost_filter_result = evaluate_holder_filter(snapshot)
                                hf_log_row_id = await log_holder_filter(
                                    token_address=token.address,
                                    alert_time=ping_ts,
                                    snapshot_id=None,
                                    result=ghost_filter_result,
                                )

                    # Suppress alerts when the ghost filter says block.
                    # Cached payloads pre-Phase 1 lack `verdict`; mirror
                    # the formatter's fallback so behavior is consistent.
                    gf_verdict = None
                    if ghost_filter_result is not None:
                        gf_verdict = ghost_filter_result.get("verdict") or (
                            "block" if ghost_filter_result.get("would_block") else "pass"
                        )

                    if gf_verdict == "block":
                        logger.info(
                            f"Alert suppressed: ${token.symbol} ({token.address}) "
                            f"tier={tier_index} verdict=block"
                            f"{' (cached)' if hf_log_row_id is None else ''}"
                        )
                        if hf_log_row_id is not None:
                            await mark_actually_blocked(hf_log_row_id)
                        continue

                    await sender.send_dip_alert(
                        token, tier, session,
                        ghost_filter_result=ghost_filter_result,
                        fee_eval=gate_result.fee_eval,
                    )
                    await trigger.mark_alerted(token, tier_index, alert_time=ping_ts)

            # Process retry queues on the Dex cadence only — both call
            # external APIs (Dexscreener + Birdeye). Keeping them at the
            # pre-shadow frequency means shadow mode adds zero new
            # external-API load.
            if cycle_count % DEX_CYCLE_MOD == 0:
                try:
                    await migration_ws.process_retry_queue()
                except Exception as e:
                    logger.error(f"Retry queue error: {e}")

                try:
                    await migration_ws.process_ath_retry_queue()
                except Exception as e:
                    logger.error(f"ATH retry queue error: {e}")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Price/alert loop error: {e}")

        cycle_count += 1
        await asyncio.sleep(SHADOW_LOOP_INTERVAL_S)


async def daily_recap_loop(
    sender: TelegramSender,
    session: aiohttp.ClientSession,
    db_path: str,
):
    """Posts daily performance recap at midnight CST."""
    logger.info("📊 Daily recap loop started (midnight CST)")

    while True:
        try:
            # Calculate seconds until next midnight CST
            now_cst = datetime.now(CST)
            tomorrow_midnight = now_cst.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=1)
            seconds_until = (tomorrow_midnight - now_cst).total_seconds()

            logger.info(
                f"📊 Next daily recap in {seconds_until / 3600:.1f} hours"
            )
            await asyncio.sleep(seconds_until)

            # It's midnight CST — pull last 24h of alerts
            since = time.time() - 86400
            alerts = await db.get_alerts_since(since, db_path)

            if not alerts:
                logger.info("📊 No alerts in last 24h, skipping recap")
                continue

            # Build lookup of current token data for live prices
            tokens = await db.load_all_tokens(db_path)
            tokens_lookup = {
                t.address: {
                    "current_price": t.current_price,
                    "current_mcap": t.current_mcap,
                }
                for t in tokens
            }

            await sender.send_daily_recap(alerts, tokens_lookup, session)
            logger.info(f"📊 Daily recap sent ({len(alerts)} alerts)")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Daily recap error: {e}")
            # Don't sleep the full day on error, retry in 5 min
            await asyncio.sleep(300)


async def command_listener_loop(
    sender: TelegramSender,
    session: aiohttp.ClientSession,
    db_path: str,
):
    """Polls Telegram for commands and responds."""
    logger.info("🎮 Command listener started")

    # Small delay to let startup finish
    await asyncio.sleep(5)

    while True:
        try:
            commands = await sender.poll_commands(session)

            for cmd_data in commands:
                cmd = cmd_data["command"]

                if cmd in ("/stats", "/perf"):
                    # Last 24h performance
                    await _send_perf_recap(sender, session, db_path, days=1)

                elif cmd in ("/perf7", "/perf 7", "/stats7"):
                    # Last 7 days performance
                    await _send_perf_recap(sender, session, db_path, days=7)
                
                elif cmd.startswith("/prim"):
                    parts = cmd.split()
                    days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 7
                    await sender.send_prim_report(session, days)

                elif cmd == "/help":
                        help_msg = (
                            "🤖 <b>Phoenix Bot Commands</b>\n"
                            "\n"
                            "/stats — Last 24h performance recap\n"
                            "/perf — Same as /stats\n"
                            "/perf7 — Last 7 days performance\n"
                            "/prim — Primitive report (last 7d, or /prim 14)\n"
                            "/help — Show this message"
                        )
                        await sender._send(help_msg, session)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug(f"Command listener error: {e}")

        await asyncio.sleep(3)  # poll every 3 seconds


async def _send_perf_recap(
    sender: TelegramSender,
    session: aiohttp.ClientSession,
    db_path: str,
    days: int,
):
    """Build and send performance recap for the given number of days."""
    since = time.time() - (days * 86400)
    alerts = await db.get_alerts_since(since, db_path)

    if not alerts:
        period = "24 hours" if days == 1 else f"{days} days"
        await sender._send(f"📊 No alerts in the last {period}.", session)
        return

    tokens = await db.load_all_tokens(db_path)
    tokens_lookup = {
        t.address: {
            "current_price": t.current_price,
            "current_mcap": t.current_mcap,
        }
        for t in tokens
    }

    await sender.send_daily_recap(alerts, tokens_lookup, session)


from modules.grpc_indexer import run_grpc_indexer


async def pumpswap_fees_prune_loop(db_path: str):
    """Prune pumpswap_fees rows older than 48h. Runs hourly.

    Added after the 2026-05-02 incident where pumpswap_fees grew
    unbounded (millions of rows from grpc_indexer at ~14 evt/s) and
    caused chronic lock contention. Live modules don't read fee history
    beyond alert-time windows; older data lives in cold-storage
    backup_2026_05_02."""
    PRUNE_INTERVAL_SECONDS = 3600
    RETENTION_HOURS = 48  # 2 days — older lives in cold storage backups

    # Initial delay so we don't slam the DB at startup
    await asyncio.sleep(300)  # 5 min

    while True:
        try:
            deleted = await db.prune_old_pumpswap_fees(
                retention_hours=RETENTION_HOURS,
                db_path=db_path,
            )
            if deleted > 0:
                logger.info(
                    f"🧹 Pruned {deleted} pumpswap_fees rows older than {RETENTION_HOURS}h"
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Prune loop error: {e}")

        await asyncio.sleep(PRUNE_INTERVAL_SECONDS)


async def periodic_checkpoint_loop(db_path: str):
    """Run a TRUNCATE WAL checkpoint every 30 minutes so the WAL can't
    silently balloon between cleanup runs. TRUNCATE briefly blocks
    writers (~ms for a healthy WAL) but always shrinks the file back to
    zero pages — PASSIVE skips frames held by other writers, and with
    the gRPC indexer writing constantly that meant PASSIVE rarely
    truncated anything.

    journal_size_limit is per-connection (NOT a persistent file property),
    so we set it on this connection too. Otherwise the post-checkpoint
    truncation step is a no-op and the WAL won't shrink even when the
    checkpoint succeeds."""
    while True:
        await asyncio.sleep(1800)  # 30 minutes
        try:
            async with db.db_connect(db_path) as db_conn:
                await db_conn.execute("PRAGMA journal_size_limit = 1073741824")
                result = await db_conn.execute_fetchall(
                    "PRAGMA wal_checkpoint(TRUNCATE)"
                )
            wal_path = db_path.replace('.db', '.db-wal')
            if os.path.exists(wal_path):
                wal_mb = os.path.getsize(wal_path) / (1024 * 1024)
                logger.info(f"Periodic checkpoint complete. WAL: {wal_mb:.1f} MB (result: {result})")
                if wal_mb > 500:
                    logger.warning(f"WAL is {wal_mb:.1f} MB — investigate if this persists")
            else:
                logger.info(f"Periodic checkpoint complete (no WAL file). result: {result}")
        except Exception as e:
            logger.error(f"Periodic checkpoint failed: {e}")


async def main():
    config = load_config()

    # Init DB
    db_path = config.get("database", {}).get("path", "data/bot.db")
    await db.init_db(db_path)

    # ── ATH refresh shadow (logging-only, auto-disables after 48h) ────────
    from modules import ath_refresh_shadow
    await ath_refresh_shadow.startup_check(db_path, config)

    # Init modules
    migration_ws = MigrationWebSocket(config)
    tracker      = PriceTracker(config)
    trigger      = AlertTrigger(config)
    sender       = TelegramSender(config)

    logger.info("🚀 Dip Bot v2 starting up...")

    async with aiohttp.ClientSession() as session:
        # Live fast-dip detector (Stage 2: trigger + +10s decision gate,
        # shadow-only — no Telegram, no alert dispatch). Constructed
        # inside the aiohttp session block so the detector can pass the
        # session to utils.dexscreener.get_sol_price for SOL/USD
        # conversion in pre_dip_1m_usd_vol. Backfill seeds the rolling-
        # max windows from pumpswap_fees so the first live event has
        # 60s of history to compare against.
        fast_dip = FastDipDetector(db_path=db_path, session=session)
        await fast_dip.startup()
        fast_dip.start_periodic_refresh()

        await sender.send_startup_message(session)

        # Backfill missed migrations before starting live detection
        try:
            await backfill_recent_migrations(config, session)
        except Exception as e:
            logger.error(f"Backfill error (continuing anyway): {e}")

        # Run all loops as parallel tasks
        await asyncio.gather(
            migration_ws.run(session),
            price_alert_loop(migration_ws, tracker, trigger, sender, config, session, db_path),
            periodic_backfill_loop(config, session),
            daily_recap_loop(sender, session, db_path),
            command_listener_loop(sender, session, db_path),
            run_grpc_indexer(on_event=fast_dip.on_event),
            periodic_checkpoint_loop(db_path),
            pumpswap_fees_prune_loop(db_path),
        )


if __name__ == "__main__":
    asyncio.run(main())