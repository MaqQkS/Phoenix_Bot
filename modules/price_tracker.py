"""
modules/price_tracker.py
Polls Dexscreener for price updates. Seeds ATH from Birdeye.
Re-checks Birdeye ATH every 5 minutes to catch spikes between polls.
"""

import asyncio
import aiohttp
import logging
import time

import database as db
from models import TrackedToken, TokenStatus
from utils.dexscreener import get_pumpswap_pair, extract_price_data
from utils.birdeye import get_ath_since_migration

logger = logging.getLogger(__name__)


class PriceTracker:
    def __init__(self, config: dict):
        self.config          = config
        self.birdeye_api_key = config["birdeye"]["api_key"]
        self.min_pump_multiple = config.get("tracking", {}).get("min_pump_multiple", 1.5)
        self.max_age_hours   = config.get("tracking", {}).get("max_token_age_hours", 168)
        # Track when we last checked Birdeye for each token (separate from ath_time)
        self._last_birdeye_check: dict[str, float] = {}

    async def update_prices(
        self, tokens: list[TrackedToken], session: aiohttp.ClientSession
    ) -> list[TrackedToken]:
        """
        Update prices for all active tokens.
        Returns tokens that just crossed the pump threshold (newly ATH_CONFIRMED).
        """
        newly_confirmed = []

        for token in tokens:
            if token.status == TokenStatus.EXPIRED:
                continue

            # Expire old tokens
            if token.age_hours > self.max_age_hours:
                token.status = TokenStatus.EXPIRED
                await db.save_token(token)
                logger.info(f"⏰ ${token.symbol} expired after {token.age_hours:.1f}h")
                continue

            try:
                pair = await get_pumpswap_pair(token.address, session)
                if not pair:
                    await asyncio.sleep(0.3)
                    continue

                data  = extract_price_data(pair)
                price = data.get("price_usd", 0)
                mcap  = data.get("mcap", 0)

                if price <= 0:
                    await asyncio.sleep(0.3)
                    continue

                # Update current state
                token.current_price   = price
                token.current_mcap    = mcap
                token.liquidity_usd   = data.get("liquidity_usd", 0) or 0
                token.volume_1h       = data.get("volume_1h", 0) or 0
                token.volume_6h       = data.get("volume_6h", 0) or 0
                token.volume_24h      = data.get("volume_24h", 0) or 0
                token.last_price_update = time.time()

                # Fill missing fields
                if token.migration_price <= 0:
                    token.migration_price = price
                if not token.pool_address and data.get("pair_address"):
                    token.pool_address = data["pair_address"]
                if token.symbol == "???" and data.get("symbol"):
                    token.symbol = data["symbol"]

               # ── Seed ATH from Birdeye (one-time only) ─────────────────
                if token.ath_price <= 0:
                    
                    birdeye_ath = await get_ath_since_migration(
                        token_address  = token.address,
                        migration_time = token.migration_time,
                        api_key        = self.birdeye_api_key,
                        session        = session,
                    )
                    
                    if birdeye_ath and birdeye_ath > 0:
                        if birdeye_ath > token.ath_price:
                            token.ath_price = birdeye_ath
                            # Estimate ATH mcap from price ratio
                            if price > 0:
                                token.ath_mcap = mcap * (birdeye_ath / price)
                            else:
                                token.ath_mcap = mcap
                            token.ath_time  = time.time()
                            logger.info(
                                f"📈 ${token.symbol} ATH updated: ${birdeye_ath:.10f}"
                            )
                    elif token.ath_price <= 0:
                        # Fallback: use current price as starting ATH
                        token.ath_price = price
                        token.ath_time  = time.time()

                # ── Update ATH if new high from live price ─────────────────
                if price > token.ath_price:
                    token.ath_price = price
                    token.ath_mcap  = mcap
                    token.ath_time  = time.time()

                # ── Check pump threshold ───────────────────────────────────
                if (
                    token.status == TokenStatus.TRACKING
                    and token.pump_multiple >= self.min_pump_multiple
                ):
                    token.status = TokenStatus.ATH_CONFIRMED
                    newly_confirmed.append(token)
                    logger.info(
                        f"🚀 ${token.symbol} hit {token.pump_multiple:.2f}x from migration! "
                        f"Dip alerts now active."
                    )

                await db.save_token(token)

                # ── Update peak/trough-after-alert for performance tracking ──
                if token.status == TokenStatus.ALERTED:
                    try:
                        # Sanity check: ignore mcap spikes >50x from ATH (data glitch or manipulation)
                        if token.ath_mcap > 0 and mcap > token.ath_mcap * 50:
                            logger.warning(
                                f"⚠️ ${token.symbol} mcap ${mcap:,.0f} is >50x ATH ${token.ath_mcap:,.0f} — skipping peak update"
                            )
                        else:
                            now = time.time()
                            await db.update_peak_after_alert(
                                address=token.address,
                                current_price=price,
                                current_mcap=mcap,
                                current_time=now,
                            )
                            await db.update_trough_after_alert(
                                address=token.address,
                                current_price=price,
                                current_mcap=mcap,
                                current_time=now,
                            )
                    except Exception as e:
                        logger.debug(f"Peak update error for ${token.symbol}: {e}")

            except Exception as e:
                logger.error(f"Price update error for ${token.symbol}: {e}")

            # Delay between tokens to avoid Dexscreener rate limits
            await asyncio.sleep(0.3)

        return newly_confirmed