"""
utils/birdeye.py — Birdeye API client.
Single responsibility: seed the real ATH for a token using OHLCV history.
Uses adaptive candle sizes based on token age:
  - Under 2 hours:  15m candles
  - 2-12 hours:     1H candles
  - 12h-3 days:     4H candles
  - 3+ days:        8H candles
"""

import aiohttp
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

BASE_URL = "https://public-api.birdeye.so"


def _pick_resolution(migration_time: float) -> str:
    """Pick candle resolution based on token age."""
    age_hours = (time.time() - migration_time) / 3600 if migration_time > 0 else 0

    if age_hours < 2:
        return "15m"
    elif age_hours < 12:
        return "1H"
    elif age_hours < 72:  # 3 days
        return "4H"
    else:
        return "8H"


async def get_ath_since_migration(
    token_address: str,
    migration_time: float,
    api_key: str,
    session: aiohttp.ClientSession,
) -> Optional[float]:
    """
    Fetch OHLCV candles since migration_time and return the highest price seen.
    Uses adaptive candle size based on token age.
    Returns ATH price in USD, or None on failure.
    """
    now = int(time.time())
    time_from = int(migration_time) if migration_time > 0 else now - 86400

    resolution = _pick_resolution(migration_time)

    try:
        url = f"{BASE_URL}/defi/ohlcv"
        params = {
            "address":    token_address,
            "type":       resolution,
            "time_from":  time_from,
            "time_to":    now,
        }
        headers = {
            "X-API-KEY": api_key,
            "x-chain":   "solana",
        }
        async with session.get(
            url,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                items = data.get("data", {}).get("items", [])
                if not items:
                    logger.debug(f"No OHLCV data for {token_address[:8]} at {resolution}")
                    return None

                # Find highest 'h' (high) across all candles
                highs = [float(c.get("h", 0) or 0) for c in items if c.get("h")]
                if highs:
                    ath = max(highs)
                    logger.info(
                        f"Birdeye ATH for {token_address[:8]}: "
                        f"${ath:.10f} from {len(items)} {resolution} candles"
                    )
                    return ath

            elif resp.status == 429:
                logger.warning("Birdeye rate limited")
                return None
            else:
                logger.debug(f"Birdeye OHLCV {resp.status} for {token_address[:8]}")

    except Exception as e:
        logger.error(f"Birdeye OHLCV error for {token_address[:8]}: {e}")

    return None