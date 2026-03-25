"""
main.py — Solana Dip Bot v2
Entry point. Runs:
  0. Backfill          — catches migrations missed during downtime
  1. Migration WebSocket — real-time pump.fun → PumpSwap migration detection
  2. Price tracker       — updates prices + ATH for all tracked tokens
  3. Alert trigger       — fires Telegram alerts at dip tiers
"""

import asyncio
import logging
import os
import sys
import time

import aiohttp
import yaml

import database as db
from modules.migration_ws      import MigrationWebSocket
from modules.backfill           import backfill_recent_migrations
from modules.price_tracker      import PriceTracker
from modules.alert_trigger      import AlertTrigger
from modules.telegram_sender    import TelegramSender

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
    """Loop that updates prices, checks dip alert tiers, and processes retry queue."""
    price_interval = config.get("tracking", {}).get("poll_interval_seconds", 30)

    while True:
        try:
            tokens = await db.load_all_tokens(db_path)

            if tokens:
                # Update prices
                newly_confirmed = await tracker.update_prices(tokens, session)
                if newly_confirmed:
                    logger.info(
                        f"✅ {len(newly_confirmed)} token(s) crossed 1.5x threshold"
                    )

                # Reload after saves (status may have changed)
                tokens = await db.load_all_tokens(db_path)

                # Check dip tiers
                to_alert = trigger.check_tokens(tokens)
                for token, tier in to_alert:
                    tier_index = config.get("dip_tiers", []).index(tier)
                    logger.info(
                        f"🔔 Alerting ${token.symbol} | "
                        f"{tier['name']} | -{token.drop_from_ath*100:.0f}% from ATH"
                    )
                    await sender.send_dip_alert(token, tier, session)
                    await trigger.mark_alerted(token, tier_index)

            # Process retry queue for tokens Dexscreener wasn't ready for
            try:
                await migration_ws.process_retry_queue()
            except Exception as e:
                logger.error(f"Retry queue error: {e}")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Price/alert loop error: {e}")

        await asyncio.sleep(price_interval)


async def main():
    config = load_config()

    # Init DB
    db_path = config.get("database", {}).get("path", "data/bot.db")
    await db.init_db(db_path)

    # Init modules
    migration_ws = MigrationWebSocket(config)
    tracker      = PriceTracker(config)
    trigger      = AlertTrigger(config)
    sender       = TelegramSender(config)

    logger.info("🚀 Dip Bot v2 starting up...")

    async with aiohttp.ClientSession() as session:
        await sender.send_startup_message(session)

        # Backfill missed migrations before starting live detection
        try:
            await backfill_recent_migrations(config, session)
        except Exception as e:
            logger.error(f"Backfill error (continuing anyway): {e}")

        # Run migration websocket and price/alert loop as parallel tasks
        await asyncio.gather(
            migration_ws.run(session),
            price_alert_loop(migration_ws, tracker, trigger, sender, config, session, db_path),
        )


if __name__ == "__main__":
    asyncio.run(main())