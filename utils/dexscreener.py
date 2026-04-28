"""
utils/dexscreener.py — Dexscreener API client.
Handles price, mcap, liquidity, and volume.
Free, no API key required.
Supports bulk token fetching to minimize API calls.
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


async def get_pumpswap_pairs_bulk(
    token_addresses: list[str], session: aiohttp.ClientSession
) -> dict[str, Optional[dict]]:
    """
    Fetch PumpSwap pairs for multiple tokens in a single API call.
    Returns dict mapping address -> best PumpSwap pair (or None).
    Dexscreener supports comma-separated addresses in one request.
    """
    result = {addr: None for addr in token_addresses}

    if not token_addresses:
        return result

    # Dexscreener supports up to 30 addresses per call
    addresses_str = ",".join(token_addresses)
    url = f"{BASE_URL}/tokens/v1/solana/{addresses_str}"

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if not isinstance(data, list):
                    return result

                # Group pairs by base token address
                pairs_by_token: dict[str, list[dict]] = {}
                for pair in data:
                    base_addr = pair.get("baseToken", {}).get("address", "")
                    if base_addr in result:
                        if base_addr not in pairs_by_token:
                            pairs_by_token[base_addr] = []
                        pairs_by_token[base_addr].append(pair)

                # Pick best PumpSwap pair for each token
                for addr, pairs in pairs_by_token.items():
                    pumpswap = [p for p in pairs if p.get("dexId", "").lower() == "pumpswap"]
                    if pumpswap:
                        result[addr] = max(
                            pumpswap,
                            key=lambda p: p.get("liquidity", {}).get("usd", 0) or 0,
                        )

            elif resp.status == 429:
                logger.warning("Dexscreener rate limited (bulk)")
            else:
                logger.debug(f"Dexscreener bulk fetch status {resp.status}")

    except Exception as e:
        logger.error(f"Dexscreener bulk request failed: {e}")

    return result


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