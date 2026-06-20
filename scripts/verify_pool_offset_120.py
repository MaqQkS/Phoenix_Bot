#!/usr/bin/env python3
"""
Verify pool pubkey lives at byte offset 120 in BuyEvent/SellEvent data payload.

Strategy:
  1. Pick a known multi-event signature (tx that touched >1 PumpSwap pool).
  2. Re-fetch raw tx via Helius getTransaction.
  3. For each 'Program data:' log, base64-decode, check the 8-byte discriminator,
     then extract bytes [120:152] and base58-encode.
  4. Confirm each decoded pubkey appears in the tx's account_keys (i.e., it's a
     real account referenced by the tx — the only way an event can legitimately
     reference a pool).
  5. Separately: pick multi-event tx where DICKER mint passes — show that
     2+ distinct pool_pubkeys are extracted from different events in the same tx
     (which is the exact bug scenario).

Usage: python scripts/verify_pool_offset_120.py
"""
import asyncio
import aiohttp
import base64
import base58
import struct
import yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
cfg = yaml.safe_load((ROOT / "config.yaml").read_text())
RPC_URL = cfg["helius"]["rpc_url"]

BUY_EVENT_DISC = bytes([103, 244, 82, 31, 44, 245, 119, 119])
SELL_EVENT_DISC = bytes([62, 47, 55, 10, 165, 3, 220, 42])

# Multi-event signatures surfaced by diagnostics_out/blind_dip_investigation.
# Each was one of the 20 DICKER events (or similar tokens) where the investigation
# discarded non-primary-pool events via the exact same offset-120 read we're
# verifying here.
SIGS = [
    # From DICKER investigation, known multi-event
    "4yvroVwBhwEJQNEbMUqTUVQVhYDKRtYrYwWm34VqUqEFc8Qcafm5pMJYaQquRRMrBfx6TabPAREaDGHcAsMpLKTm",
    "2hdjNBCK9QMZ3jhpsB47qS6ra78jsFywKtWjZrUs9kf9uNGUHQFpoYSmHzpS6vS6wMUZQ2245PoW6PWRcVXtpwRz",
    # From BLOB investigation — multi-pool case with different pool_addresses recorded
    "5RutxCe26PcDt7zRwWSCsE7zvkrrGJ2R2XRfs6LR8t2HLZSygggPZ83DfPa6s2tSjtmTLgGCfz3jZiBykyMPXJey",
    "5m4gQaN6VWjt7djW5Q1z3zDeKo2wUuUdom74Cydv9k2YiffeJ7xp7yedRGVA9dz7r83JHaVmRoRxXH4huvyaRgse",
    "2kn2ammuaACRRddEZE2ygYdYsVsfppjDK8BZvwKs3vYex5gYLcgQrtHysiZBvCrcWmnZL3W1aNeowmX4iBSUzGpL",
]


async def fetch_tx(sig, session):
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
        return await r.json()


def collect_account_keys(tx):
    """Collect every account key referenced by the tx (incl. ALT entries)."""
    keys = set()
    if not tx:
        return keys
    msg = tx.get("transaction", {}).get("message", {})
    for k in msg.get("accountKeys", []):
        if isinstance(k, str):
            keys.add(k)
        elif isinstance(k, dict) and k.get("pubkey"):
            keys.add(k["pubkey"])
    meta = tx.get("meta") or {}
    for k in meta.get("loadedAddresses", {}).get("writable", []):
        keys.add(k)
    for k in meta.get("loadedAddresses", {}).get("readonly", []):
        keys.add(k)
    return keys


async def verify_one(sig, session):
    print(f"\n=== {sig[:16]}... ===")
    resp = await fetch_tx(sig, session)
    tx = resp.get("result")
    if not tx:
        print(f"  !! tx not found (err={resp.get('error')})")
        return None

    account_keys = collect_account_keys(tx)
    logs = (tx.get("meta") or {}).get("logMessages") or []

    event_pools = []
    for log in logs:
        if not log.startswith("Program data: "):
            continue
        try:
            data = base64.b64decode(log[len("Program data: "):])
        except Exception:
            continue
        if len(data) < 152:
            continue
        disc = data[:8]
        if disc != BUY_EVENT_DISC and disc != SELL_EVENT_DISC:
            continue
        pool_pubkey = base58.b58encode(data[120:152]).decode("ascii")
        user_pubkey = base58.b58encode(data[152:184]).decode("ascii") if len(data) >= 184 else "?"
        event_type = "buy" if disc == BUY_EVENT_DISC else "sell"
        in_keys = pool_pubkey in account_keys
        event_pools.append((event_type, pool_pubkey, user_pubkey, in_keys))

    for i, (et, pool, user, in_keys) in enumerate(event_pools):
        tag = "OK (account_keys)" if in_keys else "!! NOT in account_keys"
        print(f"  event[{i}] {et:4}  pool={pool}  user={user[:10]}...  {tag}")

    distinct_pools = {p for _, p, _, _ in event_pools}
    print(f"  events={len(event_pools)}  distinct_pools={len(distinct_pools)}")
    all_in_keys = all(in_keys for _, _, _, in_keys in event_pools)
    return {
        "sig": sig,
        "n_events": len(event_pools),
        "distinct_pools": list(distinct_pools),
        "all_pools_in_account_keys": all_in_keys,
    }


async def main():
    async with aiohttp.ClientSession() as session:
        results = []
        for sig in SIGS:
            r = await verify_one(sig, session)
            if r:
                results.append(r)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    all_valid = all(r["all_pools_in_account_keys"] for r in results)
    multi_pool = [r for r in results if len(r["distinct_pools"]) > 1]
    print(f"txs decoded: {len(results)}")
    print(f"every decoded pool pubkey appears in tx.account_keys: {all_valid}")
    print(f"txs with >1 distinct pool across events: {len(multi_pool)}")
    for r in multi_pool:
        print(f"  {r['sig'][:16]} -> {len(r['distinct_pools'])} pools: "
              f"{[p[:12] for p in r['distinct_pools']]}")

    if all_valid:
        print("\nOFFSET 120 VERIFIED: every extracted pubkey is a real account in the tx.")
    else:
        print("\nWARNING: some extracted pubkeys are NOT in account_keys — offset may be wrong.")


if __name__ == "__main__":
    asyncio.run(main())
