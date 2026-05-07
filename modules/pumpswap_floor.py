"""
modules/pumpswap_floor.py — pumpswap_fees-derived peak-mcap floor helper.

Pure-math helper for the Phase 1A ATH-floor work. Computes the highest
USD market cap implied by a token's qualifying pumpswap_fees rows
(quote_amount >= 0.1 SOL) using historical SOL/USD pricing from Birdeye.

Returns a FloorResult or None — never mutates the token, never writes
to the DB. Caller decides what to do with the result.

Validated against:
  - LEBRON  (6jWfYfPAuw1Nyv6Fqor7GbTfnC7t4VQXsYis8vacpump) → ~$61,416
  - Soothsayer (mgqrZEriPE3zGSc1FNzy39YrSNH78giwX9XJKVUpump) → ~$80,636
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp

import database as db
from models import TrackedToken
from utils.birdeye import get_sol_price_at, get_sol_price_now

logger = logging.getLogger(__name__)

# 0.1 SOL — quality filter that rejects dust trades from the floor
# computation. INVESTIGATION.md §6 confirms this threshold has teeth
# without wiping out coverage (~72% of pools have ≥1 qualifying row in
# the first 60s post-pool-inception).
MIN_QUOTE_LAMPORTS = 100_000_000

# Pump.fun cohort is uniformly 6-decimal (736/737 of live tokens; see
# INVESTIGATION.md §3). The lookup still consults token.token_decimals
# defensively — None is treated as "decimals unavailable" and the
# function no-ops with a WARN.
_TOTAL_SUPPLY = 1_000_000_000  # pump.fun fixed supply (1 B tokens)


@dataclass
class FloorResult:
    max_price_sol: float
    max_mcap_usd: float
    peak_signature: str
    peak_block_time: float
    peak_quote_amount: int
    peak_base_amount: int
    peak_sol_usd: float
    sample_count: int       # all rows for the pool
    qualifying_count: int   # rows passing the quote_amount filter


async def compute_pumpswap_floor(
    token: TrackedToken,
    http_session: aiohttp.ClientSession,
    api_key: str,
    db_path: str = db.DB_PATH,
) -> Optional[FloorResult]:
    """Compute the peak-mcap floor implied by a token's pumpswap_fees rows.

    Returns None on any of:
      - empty pool_address (DEBUG)
      - zero rows / zero qualifying rows (DEBUG)
      - missing token_decimals (WARN)
      - no row for which Birdeye returned a SOL/USD price (WARN)
    """
    if not token.pool_address:
        logger.debug(
            f"compute_pumpswap_floor: no pool_address for {token.address[:8]}"
        )
        return None

    decimals = token.token_decimals
    if decimals is None:
        logger.warning(
            f"compute_pumpswap_floor: token_decimals unavailable for "
            f"{token.address[:8]} ({token.symbol})"
        )
        return None

    async with db.db_connect(db_path) as conn:
        async with conn.execute(
            "SELECT COUNT(*) FROM pumpswap_fees WHERE pool_address = ?",
            (token.pool_address,),
        ) as cur:
            row = await cur.fetchone()
            sample_count = int(row[0]) if row else 0

        async with conn.execute(
            "SELECT signature, quote_amount, base_amount, block_time "
            "FROM pumpswap_fees "
            "WHERE pool_address = ? AND quote_amount >= ? "
            "ORDER BY block_time",
            (token.pool_address, MIN_QUOTE_LAMPORTS),
        ) as cur:
            qualifying_rows = await cur.fetchall()

    qualifying_count = len(qualifying_rows)
    if qualifying_count == 0:
        logger.debug(
            f"compute_pumpswap_floor: no qualifying rows for "
            f"{token.address[:8]} (pool={token.pool_address[:8]}, "
            f"sample_count={sample_count})"
        )
        return None

    base_divisor = 10 ** decimals
    quote_divisor = 1e9  # lamports → SOL

    best: Optional[FloorResult] = None
    skipped_no_sol = 0
    for signature, quote_amount, base_amount, block_time in qualifying_rows:
        if not base_amount or base_amount <= 0:
            continue
        if not block_time:
            continue

        sol_usd = await get_sol_price_at(block_time, api_key, http_session)
        if sol_usd is None:
            sol_usd = await get_sol_price_now(api_key, http_session)
        if sol_usd is None or sol_usd <= 0:
            skipped_no_sol += 1
            continue

        price_sol = (quote_amount / quote_divisor) / (base_amount / base_divisor)
        mcap_usd = price_sol * sol_usd * _TOTAL_SUPPLY

        if best is None or mcap_usd > best.max_mcap_usd:
            best = FloorResult(
                max_price_sol=price_sol,
                max_mcap_usd=mcap_usd,
                peak_signature=signature,
                peak_block_time=float(block_time),
                peak_quote_amount=int(quote_amount),
                peak_base_amount=int(base_amount),
                peak_sol_usd=float(sol_usd),
                sample_count=sample_count,
                qualifying_count=qualifying_count,
            )

    if best is None:
        logger.warning(
            f"compute_pumpswap_floor: no priced row for "
            f"{token.address[:8]} ({token.symbol}) — "
            f"qualifying={qualifying_count}, skipped_no_sol={skipped_no_sol}"
        )
        return None

    return best
