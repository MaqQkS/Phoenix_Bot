#!/usr/bin/env python3
"""
End-to-end verification of the multi-pool event attribution filter.

For each test signature we:
  1. Fetch the raw tx via Helius getTransaction (with base64 logs).
  2. Extract 'Program data:' base64 payloads and decode them via the UPDATED
     _parse_fees_from_event — which now returns event_pool_address.
  3. Determine the tx's primary pool by applying the exact same heuristic
     extract_pool_and_mint uses (walk pre/post_token_balances for the owner
     that holds both WSOL and a non-WSOL mint).
  4. Apply the filter logic: keep only events where event_pool_address
     equals primary pool.
  5. Print:
       * kept events (these would be persisted)
       * discarded events (these would be skipped by the fix, or logged
         only in DRY-RUN mode — shown as DRY-RUN sample output)

This mirrors the runtime fix in modules/grpc_indexer.py without needing
a live gRPC subscription.
"""
import asyncio
import aiohttp
import base64
import sys
import yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.onchain_fees import (
    BUY_EVENT_DISC,
    SELL_EVENT_DISC,
    _parse_fees_from_event,
)

WSOL_MINT = "So11111111111111111111111111111111111111112"

cfg = yaml.safe_load((ROOT / "config.yaml").read_text())
RPC_URL = cfg["helius"]["rpc_url"]

TEST_SIGS = [
    "4yvroVwBhwEJQNEbMUqTUVQVhYDKRtYrYwWm34VqUqEFc8Qcafm5pMJYaQquRRMrBfx6TabPAREaDGHcAsMpLKTm",
    "2hdjNBCK9QMZ3jhpsB47qS6ra78jsFywKtWjZrUs9kf9uNGUHQFpoYSmHzpS6vS6wMUZQ2245PoW6PWRcVXtpwRz",
    "5RutxCe26PcDt7zRwWSCsE7zvkrrGJ2R2XRfs6LR8t2HLZSygggPZ83DfPa6s2tSjtmTLgGCfz3jZiBykyMPXJey",
    "5m4gQaN6VWjt7djW5Q1z3zDeKo2wUuUdom74Cydv9k2YiffeJ7xp7yedRGVA9dz7r83JHaVmRoRxXH4huvyaRgse",
    "2kn2ammuaACRRddEZE2ygYdYsVsfppjDK8BZvwKs3vYex5gYLcgQrtHysiZBvCrcWmnZL3W1aNeowmX4iBSUzGpL",
]


async def fetch_tx(sig, session):
    """Fetch with jsonParsed so we can read pre/post token balance owner fields."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [sig, {
            "encoding": "jsonParsed",
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0,
        }],
    }
    async with session.post(RPC_URL, json=payload,
                            timeout=aiohttp.ClientTimeout(total=20)) as r:
        data = await r.json()
    return data.get("result")


def primary_pool_from_balances(tx):
    """
    Mirror extract_pool_and_mint on jsonParsed RPC output. Walks token balance
    entries for an owner that holds both WSOL and a non-WSOL mint — that's the
    pool address. Returns (pool, token_mint).
    """
    meta = tx.get("meta") or {}
    balances = (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or [])
    by_owner = {}
    for b in balances:
        owner = b.get("owner")
        mint = b.get("mint")
        if not owner or not mint:
            continue
        by_owner.setdefault(owner, [])
        if mint not in by_owner[owner]:
            by_owner[owner].append(mint)
    for owner, mints in by_owner.items():
        if WSOL_MINT not in mints:
            continue
        non_sol = [m for m in mints if m != WSOL_MINT]
        if not non_sol:
            continue
        return owner, non_sol[0]
    return None, None


def parse_events(tx):
    out = []
    logs = (tx.get("meta") or {}).get("logMessages") or []
    for log in logs:
        if not log.startswith("Program data: "):
            continue
        try:
            data = base64.b64decode(log[len("Program data: "):])
        except Exception:
            continue
        if len(data) < 8:
            continue
        disc = data[:8]
        if disc == BUY_EVENT_DISC:
            fees = _parse_fees_from_event(data, BUY_EVENT_DISC)
            if fees:
                out.append(("buy", fees))
        elif disc == SELL_EVENT_DISC:
            fees = _parse_fees_from_event(data, SELL_EVENT_DISC)
            if fees:
                out.append(("sell", fees))
    return out


async def main():
    print(f"=== pool filter e2e verification ===")
    print(f"Simulating dry-run mode: events that don't match primary pool")
    print(f"are reported but still counted as 'would keep'.\n")

    total_events = 0
    total_kept = 0
    total_discarded = 0

    async with aiohttp.ClientSession() as session:
        for sig in TEST_SIGS:
            tx = await fetch_tx(sig, session)
            if not tx:
                print(f"!! {sig[:16]} — tx not found")
                continue

            primary_pool, token_mint = primary_pool_from_balances(tx)
            events = parse_events(tx)

            print(f"\n--- tx {sig[:16]}... ---")
            print(f"  primary_pool (extract_pool_and_mint): {primary_pool}")
            print(f"  token_mint                         : {token_mint}")
            print(f"  events parsed                      : {len(events)}")

            kept = []
            discarded = []
            dry_run_log_lines = []
            real_filter_log_lines = []
            for et, f in events:
                evt_pool = f["event_pool_address"]
                if evt_pool and evt_pool != primary_pool:
                    discarded.append((et, f))
                    dry_run_log_lines.append(
                        f"DEBUG: DRY-RUN: would skip event from non-primary "
                        f"pool {evt_pool} (primary: {primary_pool}) in tx {sig}"
                    )
                    real_filter_log_lines.append(
                        f"DEBUG: Skipping event from non-primary pool "
                        f"{evt_pool} (primary: {primary_pool}) in tx {sig}"
                    )
                else:
                    kept.append((et, f))

            for et, f in kept:
                print(f"  KEEP    {et:4}  event_pool={f['event_pool_address']}  "
                      f"q={f['quote_amount']} base={f['base_amount']}")
            for et, f in discarded:
                print(f"  DISCARD {et:4}  event_pool={f['event_pool_address']}  "
                      f"q={f['quote_amount']} base={f['base_amount']}")

            if discarded:
                print(f"  -- DRY-RUN log lines that would be emitted --")
                for line in dry_run_log_lines:
                    # Truncate sig for readability
                    print("    " + line.replace(sig, sig[:16] + "..."))
                print(f"  -- REAL-MODE log lines that would be emitted --")
                for line in real_filter_log_lines:
                    print("    " + line.replace(sig, sig[:16] + "..."))

            total_events += len(events)
            total_kept += len(kept)
            total_discarded += len(discarded)

    print("\n" + "=" * 60)
    print(f"TOTAL events parsed   : {total_events}")
    print(f"TOTAL kept (primary)  : {total_kept}")
    print(f"TOTAL discarded (non) : {total_discarded}")
    print(
        f"Expected behavior: kept events only attribute to the primary pool; "
        f"discarded events would have been wrongly recorded against the "
        f"primary pool without the fix."
    )


if __name__ == "__main__":
    asyncio.run(main())
