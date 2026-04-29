"""
utils/birdeye.py — Birdeye API client.
Single responsibility: seed the real ATH for a token using OHLCV history.
Uses adaptive candle sizes based on token age:
  - Under 20 min:   1m candles
  - 20m - 2 hours:  15m candles
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

    if age_hours < (20 / 60):   # < 20 minutes — 1m candles close fast, fresher ATH
        return "1m"
    elif age_hours < 2:
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
    resolution: Optional[str] = None,
) -> Optional[float]:
    """
    Fetch OHLCV candles since migration_time and return the highest price seen.
    Uses adaptive candle size based on token age unless `resolution` is passed
    explicitly (e.g. "15m" for the T+15m one-shot correction pass).
    Returns ATH price in USD, or None on failure.
    """
    now = int(time.time())
    time_from = int(migration_time) if migration_time > 0 else now - 86400

    if resolution is None:
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

# Native SOL mint address (Birdeye's identifier for SOL)
SOL_MINT = "So11111111111111111111111111111111111111112"

# In-memory cache: minute_bucket_unix -> sol_price_usd
# Cleared on process restart (intentional — small, ephemeral)
_SOL_PRICE_CACHE: dict[int, float] = {}
_CACHE_MAX_SIZE = 5000  # ~3.5 days of per-minute buckets


def _bucket_minute(timestamp: float) -> int:
    """Floor to nearest minute for cache keying."""
    return int(timestamp // 60) * 60


def _evict_cache_if_full():
    """Simple FIFO eviction when cache exceeds max size."""
    if len(_SOL_PRICE_CACHE) <= _CACHE_MAX_SIZE:
        return
    # Drop oldest 20% of entries
    keys_sorted = sorted(_SOL_PRICE_CACHE.keys())
    drop_count = _CACHE_MAX_SIZE // 5
    for k in keys_sorted[:drop_count]:
        _SOL_PRICE_CACHE.pop(k, None)


async def get_sol_price_at(
    timestamp: float,
    api_key: str,
    session: aiohttp.ClientSession,
) -> Optional[float]:
    """
    Get historical SOL/USD price at the given unix timestamp.

    Uses Birdeye /defi/history_price with 1-minute resolution.
    Caches results by minute-bucket so 30 tokens in the same minute = 1 API call.

    Returns None if Birdeye has no data (caller should fall back to current price).
    """
    bucket = _bucket_minute(timestamp)

    # Cache hit
    if bucket in _SOL_PRICE_CACHE:
        return _SOL_PRICE_CACHE[bucket]

    # Birdeye history_price wants time_from / time_to range — fetch a 2-min window
    # centered on the bucket, then return the midpoint candle's value.
    time_from = bucket - 60
    time_to = bucket + 60

    url = "https://public-api.birdeye.so/defi/history_price"
    params = {
        "address": SOL_MINT,
        "address_type": "token",
        "type": "1m",
        "time_from": time_from,
        "time_to": time_to,
    }
    headers = {
        "X-API-KEY": api_key,
        "x-chain": "solana",
        "accept": "application/json",
    }

    try:
        async with session.get(
            url, params=params, headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                logger.debug(f"Birdeye history_price status {resp.status} for ts={timestamp}")
                return None
            data = await resp.json()

        items = (data.get("data") or {}).get("items") or []
        if not items:
            logger.debug(f"Birdeye history_price empty items for ts={timestamp}")
            return None

        # Find the candle closest to our bucket
        best = min(items, key=lambda it: abs(int(it.get("unixTime", 0)) - bucket))
        price = float(best.get("value", 0))
        if price <= 0:
            return None

        _SOL_PRICE_CACHE[bucket] = price
        _evict_cache_if_full()
        return price

    except Exception as e:
        logger.debug(f"Birdeye history_price error for ts={timestamp}: {e}")
        return None


async def get_sol_price_now(
    api_key: str,
    session: aiohttp.ClientSession,
) -> Optional[float]:
    """
    Get current SOL/USD price. Used as fallback when historical lookup fails
    (e.g., for a token that incepted seconds ago and Birdeye hasn't candled yet).
    """
    url = "https://public-api.birdeye.so/defi/price"
    params = {"address": SOL_MINT}
    headers = {
        "X-API-KEY": api_key,
        "x-chain": "solana",
        "accept": "application/json",
    }
    try:
        async with session.get(
            url, params=params, headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
        price = float(((data.get("data") or {}).get("value")) or 0)
        return price if price > 0 else None
    except Exception as e:
        logger.debug(f"Birdeye current price error: {e}")
        return None