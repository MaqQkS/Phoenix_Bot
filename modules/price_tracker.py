"""
modules/price_tracker.py
Polls Dexscreener for price updates.
Uses bulk Dexscreener API calls — one request per batch instead of per token.
Also updates peak-after-alert for performance tracking.
"""

import asyncio
import aiohttp
import logging
import time

import database as db
from models import TrackedToken, TokenStatus
from utils.dexscreener import get_pumpswap_pairs_bulk, get_pumpswap_pair, extract_price_data
from modules import ath_refresh_shadow

logger = logging.getLogger(__name__)

# How many tokens per bulk Dexscreener request
BATCH_SIZE = 20


class PriceTracker:
    def __init__(self, config: dict):
        self.config          = config
        self.min_pump_multiple = config.get("tracking", {}).get("min_pump_multiple", 1.30)
        self.max_age_hours   = config.get("tracking", {}).get("max_token_age_hours", 168)
        self._last_known_mcap: dict[str, float] = {}

    async def update_prices(
        self, tokens: list[TrackedToken], session: aiohttp.ClientSession
    ) -> list[TrackedToken]:
        """
        Update prices for all active tokens using bulk Dexscreener API calls.
        Returns tokens that just crossed the pump threshold (newly ATH_CONFIRMED).
        """
        newly_confirmed = []
        # Sort newest tokens first so they get Birdeye ATH seeded ASAP
        tokens = sorted(tokens, key=lambda t: t.migration_time, reverse=True)

        # Filter out expired/blocked tokens first.
        # load_all_tokens already excludes BLOCKED at SQL, but defense-in-depth:
        # if a token gets marked BLOCKED in-memory mid-cycle, don't poll prices.
        active_tokens = []
        for token in tokens:
            if token.status in (TokenStatus.EXPIRED, TokenStatus.BLOCKED):
                continue
            if token.age_hours > self.max_age_hours:
                old_status = token.status.value
                token.status = TokenStatus.EXPIRED
                await db.save_token(token)
                logger.info(f"⏰ ${token.symbol} expired after {token.age_hours:.1f}h")
                ath_refresh_shadow.log_status_transition(
                    token.address, old_status, "expired", token.migration_time, token.symbol
                )
                continue
            active_tokens.append(token)

        # Process in batches — one API call per batch
        for i in range(0, len(active_tokens), BATCH_SIZE):
            batch = active_tokens[i:i + BATCH_SIZE]
            addresses = [t.address for t in batch]

            # Single API call for the whole batch
            pairs_map = await get_pumpswap_pairs_bulk(addresses, session)

            # Process each token with its pair data
            for token in batch:
                pair = pairs_map.get(token.address)
                result = await self._process_token(token, pair, session)
                if result:
                    newly_confirmed.append(token)

            # Delay between batches
            await asyncio.sleep(1)

        return newly_confirmed

    async def _process_token(
        self, token: TrackedToken, pair: dict | None, session: aiohttp.ClientSession
    ) -> bool:
        """
        Process a single token with its already-fetched pair data.
        Returns True if the token just crossed the pump threshold.
        """
        try:
            if not pair:
                return False

            data  = extract_price_data(pair)
            price = data.get("price_usd", 0)
            mcap  = data.get("mcap", 0)

            if price <= 0:
                return False

            # ── Stale data detection ───────────────────────────────────
            last_mcap = self._last_known_mcap.get(token.address, 0)
            if last_mcap > 0 and mcap > 0:
                drop_pct = 1 - (mcap / last_mcap)
                if drop_pct > 0.70:
                    logger.warning(
                        f"⚠️ ${token.symbol} mcap dropped {drop_pct*100:.0f}% in one poll "
                        f"(${last_mcap:,.0f} → ${mcap:,.0f}) — re-fetching to confirm"
                    )
                    await asyncio.sleep(2)
                    pair2 = await get_pumpswap_pair(token.address, session)
                    if pair2:
                        data2 = extract_price_data(pair2)
                        price2 = data2.get("price_usd", 0)
                        mcap2 = data2.get("mcap", 0)
                        if price2 > 0 and mcap2 > 0:
                            price = price2
                            mcap = mcap2
                            data = data2
                            logger.info(
                                f"⚠️ ${token.symbol} re-fetch result: ${mcap2:,.0f} mcap"
                            )

            self._last_known_mcap[token.address] = mcap

            # Update current state
            token.current_price   = price
            token.current_mcap    = mcap
            token.liquidity_usd   = data.get("liquidity_usd", 0) or 0
            token.volume_1h       = data.get("volume_1h", 0) or 0
            token.volume_6h       = data.get("volume_6h", 0) or 0
            token.volume_24h      = data.get("volume_24h", 0) or 0
            token.last_price_update = time.time()

            # Fill missing fields
            # Prefer deriving from migration_mcap (same 1B fixed supply
            # basis as new rows). Fall back to Dexscreener price only
            # for legacy rows where migration_mcap is also missing.
            if token.migration_price <= 0:
                if token.migration_mcap > 0:
                    token.migration_price = token.migration_mcap / 1_000_000_000
                else:
                    token.migration_price = price
            if not token.pool_address and data.get("pair_address"):
                token.pool_address = data["pair_address"]
            if token.symbol == "???" and data.get("symbol"):
                token.symbol = data["symbol"]

            # ── Update ATH if new high from live price ─────────────────
            # When running-max exceeds a Birdeye-derived ATH, flip source to
            # 'birdeye_running_max' so downstream can tell live-poll peaks
            # apart from Birdeye-sourced peaks. 'unseeded' rows get
            # promoted to 'running_max' since there's no Birdeye baseline
            # to qualify with. 'fallback' and 'running_max' are untouched.
            if price > token.ath_price:
                old_ath_mcap = token.ath_mcap
                token.ath_price = price
                token.ath_mcap  = mcap
                token.ath_time  = time.time()
                if token.ath_source in ("birdeye", "birdeye_reseeded", "birdeye_corrected"):
                    token.ath_source = "birdeye_running_max"
                elif token.ath_source == "unseeded":
                    token.ath_source = "running_max"
                if token.status in (TokenStatus.ATH_CONFIRMED, TokenStatus.ALERTED):
                    logger.info(
                        f"📈 ${token.symbol} new ATH: ${mcap:,.0f} mcap "
                        f"(was ${old_ath_mcap:,.0f})"
                    )

            # ── Log price for alert-eligible tokens ────────────────────
            if token.status in (TokenStatus.ATH_CONFIRMED, TokenStatus.ALERTED):
                drop = token.drop_from_ath
                if drop >= 0.40:
                    logger.debug(
                        f"💲 ${token.symbol} price: ${mcap:,.0f} mcap | "
                        f"ATH ${token.ath_mcap:,.0f} | drop {drop*100:.0f}% | "
                        f"last_tier={token.last_alerted_tier}"
                    )

            # ── Check pump threshold ───────────────────────────────────
            is_newly_confirmed = False
            if (
                token.status == TokenStatus.TRACKING
                and token.pump_multiple >= self.min_pump_multiple
            ):
                old_status = token.status.value
                token.status = TokenStatus.ATH_CONFIRMED
                is_newly_confirmed = True
                logger.info(
                    f"🚀 ${token.symbol} hit {token.pump_multiple:.2f}x from migration! "
                    f"Dip alerts now active."
                )
                ath_refresh_shadow.log_status_transition(
                    token.address, old_status, "ath_confirmed", token.migration_time, token.symbol
                )

            await db.save_token(token)
            ath_refresh_shadow.check_delta(token)

            # ── Update peak-after-alert for performance tracking ───────
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

            return is_newly_confirmed

        except Exception as e:
            logger.error(f"Price update error for ${token.symbol}: {e}")
            return False