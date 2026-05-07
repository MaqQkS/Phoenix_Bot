#!/usr/bin/env python3
"""scripts/smoke_test_pumpswap_floor.py — fixture-driven smoke test.

Validates modules.pumpswap_floor.compute_pumpswap_floor against two
pre-reconstructed fixtures:

  - LEBRON     (6jWfYfPAuw1Nyv6Fqor7GbTfnC7t4VQXsYis8vacpump) → ~$61,416
                 (within 1% — see diagnostics_out/ath_floor_dryrun)
  - Soothsayer (mgqrZEriPE3zGSc1FNzy39YrSNH78giwX9XJKVUpump) → ~$80,636
                 (within 0.1% — see diagnostics_out/soothsayer_reconstruction)

Runs against the live bot.db (default ../../../data/bot.db relative to
this script when run from a worktree, override via --db) and hits
Birdeye for SOL/USD pricing — non-hermetic.

Exit code 0 on pass, 1 on any failure.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

import aiohttp
import yaml

# Make project root importable when running this script directly.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import database as db
from modules.pumpswap_floor import compute_pumpswap_floor

logger = logging.getLogger("smoke_test_pumpswap_floor")


# Default DB — bot.db lives outside the worktree (gitignored). When this
# script runs from .claude/worktrees/<name>/scripts/, the live DB sits
# four parents up at <repo_root>/data/bot.db.
DEFAULT_DB_CANDIDATES = [
    _ROOT / "data" / "bot.db",
    _ROOT.parents[2] / "data" / "bot.db",  # worktree → repo root
]


FIXTURES = [
    {
        "name": "LEBRON",
        "address": "6jWfYfPAuw1Nyv6Fqor7GbTfnC7t4VQXsYis8vacpump",
        "expected_mcap_usd": 61_416.0,
        "tolerance_pct": 1.0,
    },
    {
        "name": "Soothsayer",
        "address": "mgqrZEriPE3zGSc1FNzy39YrSNH78giwX9XJKVUpump",
        "expected_mcap_usd": 80_636.0,
        "tolerance_pct": 0.1,
    },
]


def _resolve_db(arg: str | None) -> str:
    if arg:
        return arg
    env = os.environ.get("PHOENIX_DB")
    if env:
        return env
    for candidate in DEFAULT_DB_CANDIDATES:
        if candidate.exists():
            return str(candidate)
    raise SystemExit(
        "no bot.db found — pass --db <path> or set PHOENIX_DB env var"
    )


async def _run_one(
    fixture: dict,
    api_key: str,
    db_path: str,
    session: aiohttp.ClientSession,
) -> bool:
    name = fixture["name"]
    address = fixture["address"]
    expected = fixture["expected_mcap_usd"]
    tol_pct = fixture["tolerance_pct"]

    token = await db.get_token(address, db_path=db_path)
    if token is None:
        logger.error(f"[{name}] address {address} not in tokens table")
        return False

    logger.info(
        f"[{name}] pool={token.pool_address[:8]}..., "
        f"decimals={token.token_decimals}, ath_source={token.ath_source}"
    )

    result = await compute_pumpswap_floor(
        token=token,
        http_session=session,
        api_key=api_key,
        db_path=db_path,
    )
    if result is None:
        logger.error(f"[{name}] compute_pumpswap_floor returned None")
        return False

    delta_pct = abs(result.max_mcap_usd - expected) / expected * 100
    pass_fail = "PASS" if delta_pct <= tol_pct else "FAIL"

    logger.info(
        f"[{name}] {pass_fail}: mcap=${result.max_mcap_usd:,.2f}  "
        f"expected≈${expected:,.0f}  delta={delta_pct:.3f}%  "
        f"tolerance={tol_pct:.2f}%"
    )
    logger.info(
        f"[{name}]   peak_sig={result.peak_signature[:16]}...  "
        f"peak_bt={result.peak_block_time}  "
        f"price_sol={result.max_price_sol:.6e}  "
        f"sol_usd=${result.peak_sol_usd:.2f}"
    )
    logger.info(
        f"[{name}]   sample_count={result.sample_count}  "
        f"qualifying_count={result.qualifying_count}"
    )

    return delta_pct <= tol_pct


async def _main_async(db_path: str) -> int:
    config_path = _ROOT / "config.yaml"
    if not config_path.exists():
        # Fall back to repo-root config when running from worktree.
        config_path = _ROOT.parents[2] / "config.yaml"
    cfg = yaml.safe_load(config_path.read_text())
    api_key = (cfg.get("birdeye") or {}).get("api_key", "")
    if not api_key:
        logger.error("birdeye.api_key missing from config.yaml")
        return 1

    async with aiohttp.ClientSession() as session:
        results = []
        for fx in FIXTURES:
            ok = await _run_one(fx, api_key, db_path, session)
            results.append((fx["name"], ok))

    failed = [n for n, ok in results if not ok]
    if failed:
        logger.error(f"FAILED fixtures: {failed}")
        return 1
    logger.info("all fixtures passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", help="path to bot.db (overrides default)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    db_path = _resolve_db(args.db)
    logger.info(f"using db_path={db_path}")
    return asyncio.run(_main_async(db_path))


if __name__ == "__main__":
    raise SystemExit(main())
