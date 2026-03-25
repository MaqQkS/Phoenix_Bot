"""
utils/dexscreener.py — Dexscreener API client.
Handles price, mcap, liquidity, and volume.
Free, no API key required.
"""

import aiohttp
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

BASE_URL = "https://api.dexscreener.com"
SOL_MINT = "So11111111111111111111111111111111111111112"

_sol_cache = {"price": 150.0, "ts": 0.0}


async def get_sol_price(session: aiohttp.ClientSession) -> float:
    """SOL/USD price, cached 60s."""
    now = time.time()
    if _sol_cache["price"] > 0 and now - _sol_cache["ts"] < 60:
        return _sol_cache["price"]
    try:
        url = f"{BASE_URL}/tokens/v1/solana/{SOL_MINT}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list) and data:
                    price = float(data[0].get("priceUsd", 0) or 0)
                    if price > 0:
                        _sol_cache["price"] = price
                        _sol_cache["ts"] = now
                        return price
    except Exception as e:
        logger.debug(f"SOL price fetch failed: {e}")
    return _sol_cache["price"]


async def get_pumpswap_pair(token_address: str, session: aiohttp.ClientSession) -> Optional[dict]:
    """Get the primary PumpSwap pair for a token address."""
    url = f"{BASE_URL}/tokens/v1/solana/{token_address}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if not isinstance(data, list) or not data:
                    return None
                # Prefer PumpSwap pairs
                pumpswap = [p for p in data if p.get("dexId", "").lower() == "pumpswap"]
                if not pumpswap:
                    return None
                return max(pumpswap, key=lambda p: p.get("liquidity", {}).get("usd", 0) or 0)
            elif resp.status == 429:
                logger.warning("Dexscreener rate limited")
                return None
    except Exception as e:
        logger.error(f"Dexscreener request failed for {token_address[:8]}: {e}")
    return None


def extract_price_data(pair: dict) -> dict:
    """Pull the fields we care about from a Dexscreener pair object."""
    price_usd    = float(pair.get("priceUsd", 0) or 0)
    price_native = float(pair.get("priceNative", 0) or 0)
    mcap         = float(pair.get("marketCap", 0) or pair.get("fdv", 0) or 0)
    liquidity    = pair.get("liquidity", {}).get("usd", 0) or 0

    volume       = pair.get("volume", {}) or {}
    volume_5m    = float(volume.get("m5", 0) or 0)
    volume_1h    = float(volume.get("h1", 0) or 0)
    volume_6h    = float(volume.get("h6", 0) or 0)
    volume_24h   = float(volume.get("h24", 0) or 0)

    # Volume spike detection: compare 1h vs 6h/6 average
    avg_1h_from_6h = volume_6h / 6 if volume_6h > 0 else 0
    is_spiking = volume_1h > (avg_1h_from_6h * 1.5) if avg_1h_from_6h > 0 else False

    pair_created_at = (pair.get("pairCreatedAt", 0) or 0) / 1000  # ms → s

    return {
        "price_usd":        price_usd,
        "price_native":     price_native,
        "mcap":             mcap,
        "liquidity_usd":    liquidity,
        "volume_1h":        volume_1h,
        "volume_6h":        volume_6h,
        "volume_24h":       volume_24h,
        "is_spiking":       is_spiking,
        "pair_address":     pair.get("pairAddress", ""),
        "symbol":           pair.get("baseToken", {}).get("symbol", "???"),
        "name":             pair.get("baseToken", {}).get("name", ""),
        "url":              pair.get("url", ""),
        "pair_created_at":  pair_created_at,
    }