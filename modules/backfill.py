"""
modules/backfill.py
On startup, fetches recently created PumpSwap pairs from Dexscreener
to catch any migrations that happened while the bot was offline.

Also runs a periodic backfill loop as a safety net for WebSocket drops.
"""

import asyncio
import aiohttp
import logging
import time
from typing import Optional

import database as db
from models import TrackedToken, TokenStatus
from utils.dexscreener import get_sol_price, extract_price_data
from modules import ath_refresh_shadow, ath_seeder
from modules.migration_ws import fetch_pool_metadata, _register_grpc_pool_meta

logger = logging.getLogger(__name__)

DEXSCREENER_URL = "https://api.dexscreener.com"


async def backfill_recent_migrations(
    config: dict,
    session: aiohttp.ClientSession,
):
    """
    Fetch recently created PumpSwap pairs from Dexscreener and add
    any missing tokens to the DB. Runs once at startup.
    """
    backfill_hours = config.get("tracking", {}).get("backfill_hours", 6)
    cutoff_time = time.time() - (backfill_hours * 3600)

    logger.info(f"🔄 Backfilling migrations from last {backfill_hours} hours...")

    sol_price = await get_sol_price(session)
    migration_mcap = sol_price * 410
    # Derive migration_price from migration_mcap to share a single
    # supply basis (1B fixed) — keeps pump_multiple consistent.
    migration_price = migration_mcap / 1_000_000_000

    # Fetch latest pairs from Dexscreener
    pairs = await _fetch_latest_pumpswap_pairs(session)
    if not pairs:
        logger.info("🔄 No recent PumpSwap pairs found for backfill")
        return 0

    added = 0
    for pair in pairs:
        try:
            # Filter: must be PumpSwap
            if pair.get("dexId", "").lower() != "pumpswap":
                continue

            # Filter: must have been created within backfill window
            created_at = (pair.get("pairCreatedAt", 0) or 0) / 1000  # ms → s
            if created_at < cutoff_time:
                continue

            # Filter: base token must end with "pump"
            base_token = pair.get("baseToken", {})
            token_address = base_token.get("address", "")
            if not token_address.endswith("pump"):
                continue

            # Skip if already tracking
            if await db.token_exists(token_address):
                continue

            # Build token from pair data
            data = extract_price_data(pair)
            price = data.get("price_usd", 0)
            mcap = data.get("mcap", 0)

            if price <= 0:
                continue

            token = TrackedToken(
                address=token_address,
                symbol=data.get("symbol", "???"),
                pool_address=data.get("pair_address", ""),
                status=TokenStatus.TRACKING,
                migration_price=migration_price,
                migration_mcap=migration_mcap,
                current_price=price,
                current_mcap=mcap,
                liquidity_usd=data.get("liquidity_usd", 0),
                ath_price=0.0,
                migration_time=created_at,
                volume_1h=data.get("volume_1h", 0),
                volume_6h=data.get("volume_6h", 0),
                volume_24h=data.get("volume_24h", 0),
            )

            try:
                orientation, decimals = await fetch_pool_metadata(
                    session, config["helius"]["rpc_url"],
                    token.pool_address, token.address,
                )
                token.pool_orientation = orientation
                token.token_decimals = decimals
                if orientation is not None and decimals is not None:
                    logger.info(
                        f"Pool metadata: ${token.symbol} "
                        f"orientation={orientation} decimals={decimals}"
                    )
            except Exception as e:
                logger.warning(
                    f"Pool metadata fetch failed for ${token.symbol}: {e}"
                )

            await db.save_token(token)
            await ath_seeder.seed_ath_for_token(token, session, config)
            _register_grpc_pool_meta(token)
            ath_refresh_shadow.observe_token_created(token)
            ath_refresh_shadow.log_status_transition(
                token.address, None, "tracking", token.migration_time, token.symbol
            )
            added += 1
            logger.info(
                f"🔄 Backfilled: ${token.symbol} | {token_address[:8]}... | "
                f"mcap {mcap:,.0f} | age {token.age_hours:.1f}h"
            )

            await asyncio.sleep(0.2)  # gentle rate limiting

        except Exception as e:
            logger.error(f"Backfill error for pair: {e}")

    logger.info(f"🔄 Backfill complete: {added} new token(s) added")
    return added


async def _fetch_latest_pumpswap_pairs(
    session: aiohttp.ClientSession,
) -> list[dict]:
    """
    Fetch recently created pairs on Solana from Dexscreener.
    Returns list of pair objects.
    """
    all_pairs = []

    # Method 1: Latest pairs endpoint
    try:
        url = f"{DEXSCREENER_URL}/latest/dex/pairs/solana"
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data if isinstance(data, list) else data.get("pairs", [])
                pumpswap_pairs = [
                    p for p in pairs
                    if p.get("dexId", "").lower() == "pumpswap"
                ]
                all_pairs.extend(pumpswap_pairs)
                logger.info(
                    f"🔄 Dexscreener latest: {len(pairs)} pairs, "
                    f"{len(pumpswap_pairs)} PumpSwap"
                )
            elif resp.status == 429:
                logger.warning("🔄 Dexscreener rate limited during backfill")
            else:
                logger.debug(f"🔄 Dexscreener latest pairs returned {resp.status}")
    except Exception as e:
        logger.error(f"🔄 Dexscreener latest pairs error: {e}")

    await asyncio.sleep(1)

    # Method 2: Search for recent PumpSwap tokens via token-profiles
    try:
        url = f"{DEXSCREENER_URL}/token-profiles/latest/v1"
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status == 200:
                profiles = await resp.json()
                if isinstance(profiles, list):
                    # Get token addresses from profiles and look them up
                    addresses = [
                        p.get("tokenAddress", "")
                        for p in profiles[:50]
                        if p.get("chainId") == "solana"
                        and p.get("tokenAddress", "").endswith("pump")
                    ]

                    # Batch lookup these tokens on Dexscreener
                    for i in range(0, len(addresses), 30):
                        batch = addresses[i:i+30]
                        if not batch:
                            continue
                        batch_str = ",".join(batch)
                        lookup_url = f"{DEXSCREENER_URL}/tokens/v1/solana/{batch_str}"
                        try:
                            async with session.get(
                                lookup_url,
                                timeout=aiohttp.ClientTimeout(total=15),
                            ) as lresp:
                                if lresp.status == 200:
                                    token_data = await lresp.json()
                                    if isinstance(token_data, list):
                                        pumpswap = [
                                            p for p in token_data
                                            if p.get("dexId", "").lower() == "pumpswap"
                                        ]
                                        all_pairs.extend(pumpswap)
                        except Exception as e:
                            logger.debug(f"Batch lookup error: {e}")

                        await asyncio.sleep(1)
    except Exception as e:
        logger.debug(f"🔄 Token profiles error: {e}")

    # Deduplicate by pair address
    seen = set()
    unique = []
    for p in all_pairs:
        pa = p.get("pairAddress", "")
        if pa and pa not in seen:
            seen.add(pa)
            unique.append(p)

    return unique


async def periodic_backfill_loop(
    config: dict,
    session: aiohttp.ClientSession,
):
    """
    Safety net: polls Dexscreener every N minutes for new PumpSwap pairs
    that the WebSocket might have missed. Runs continuously alongside WS.
    """
    interval = config.get("tracking", {}).get("periodic_backfill_seconds", 180)
    logger.info(f"🔄 Periodic backfill started (every {interval}s)")

    # Small delay so startup backfill finishes first
    await asyncio.sleep(30)

    while True:
        try:
            sol_price = await get_sol_price(session)
            migration_mcap = sol_price * 410
            migration_price = migration_mcap / 1_000_000_000

            pairs = await _fetch_latest_pumpswap_pairs(session)
            if not pairs:
                await asyncio.sleep(interval)
                continue

            added = 0
            for pair in pairs:
                try:
                    if pair.get("dexId", "").lower() != "pumpswap":
                        continue

                    base_token = pair.get("baseToken", {})
                    token_address = base_token.get("address", "")
                    if not token_address.endswith("pump"):
                        continue

                    if await db.token_exists(token_address):
                        continue

                    data = extract_price_data(pair)
                    price = data.get("price_usd", 0)
                    mcap = data.get("mcap", 0)

                    if price <= 0:
                        continue

                    # Same dynamic mcap floor as WS path
                    min_migration_mcap = sol_price * 200
                    if mcap < min_migration_mcap:
                        continue

                    created_at = (pair.get("pairCreatedAt", 0) or 0) / 1000

                    token = TrackedToken(
                        address=token_address,
                        symbol=data.get("symbol", "???"),
                        pool_address=data.get("pair_address", ""),
                        status=TokenStatus.TRACKING,
                        migration_price=migration_price,
                        migration_mcap=migration_mcap,
                        current_price=price,
                        current_mcap=mcap,
                        liquidity_usd=data.get("liquidity_usd", 0),
                        ath_price=0.0,
                        migration_time=created_at if created_at > 0 else time.time(),
                        volume_1h=data.get("volume_1h", 0),
                        volume_6h=data.get("volume_6h", 0),
                        volume_24h=data.get("volume_24h", 0),
                    )

                    try:
                        orientation, decimals = await fetch_pool_metadata(
                            session, config["helius"]["rpc_url"],
                            token.pool_address, token.address,
                        )
                        token.pool_orientation = orientation
                        token.token_decimals = decimals
                        if orientation is not None and decimals is not None:
                            logger.info(
                                f"Pool metadata: ${token.symbol} "
                                f"orientation={orientation} decimals={decimals}"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Pool metadata fetch failed for ${token.symbol}: {e}"
                        )

                    await db.save_token(token)
                    await ath_seeder.seed_ath_for_token(token, session, config)
                    _register_grpc_pool_meta(token)
                    ath_refresh_shadow.observe_token_created(token)
                    ath_refresh_shadow.log_status_transition(
                        token.address, None, "tracking", token.migration_time, token.symbol
                    )
                    added += 1
                    logger.info(
                        f"🔄 Periodic backfill caught: ${token.symbol} | "
                        f"{token_address[:8]}... | mcap ${mcap:,.0f}"
                    )

                    await asyncio.sleep(0.2)

                except Exception as e:
                    logger.error(f"Periodic backfill token error: {e}")

            if added:
                logger.info(f"🔄 Periodic backfill: {added} new token(s) caught")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Periodic backfill loop error: {e}")

        await asyncio.sleep(interval)