"""
utils/grpc_decoder.py — Helpers for decoding Yellowstone gRPC TransactionInfo messages.

Extracts the bits the indexer needs:
- base58 signature
- log messages (raw list[str])
- token mint (the non-SOL pump token in this tx)
- pool address (owner of the SOL holder account)

The actual fee parsing (BuyEvent/SellEvent → lp_fee, protocol_fee) lives in
utils/onchain_fees.py and is reused as-is.
"""

import base58
import base64
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Wrapped SOL mint - we use this to identify which side of a pool is SOL
WSOL_MINT = "So11111111111111111111111111111111111111112"


def signature_to_base58(sig_bytes: bytes) -> str:
    """Convert raw 64-byte signature to base58 string."""
    return base58.b58encode(sig_bytes).decode("ascii")


def extract_log_messages(tx_info) -> list[str]:
    """Pull log_messages list from a gRPC TransactionInfo."""
    if not tx_info.HasField("meta"):
        return []
    return list(tx_info.meta.log_messages)


def extract_program_data_bytes(log_messages: list[str]) -> list[bytes]:
    """
    Find every 'Program data: <base64>' line in the logs and return their decoded bytes.
    Anchor emits events using this exact prefix.
    """
    out = []
    for log in log_messages:
        if not log.startswith("Program data: "):
            continue
        b64 = log[len("Program data: "):]
        try:
            out.append(base64.b64decode(b64))
        except Exception:
            continue
    return out


def extract_pool_and_mint(tx_info) -> tuple[Optional[str], Optional[str]]:
    """
    Find the PumpSwap pool address and the meme token mint from a TransactionInfo.
    
    Heuristic that works for PumpSwap:
    - Walk pre_token_balances looking for two accounts with the SAME owner
    - One must hold WSOL, the other must hold a non-WSOL mint
    - That shared owner is the pool address
    - The non-WSOL mint is the meme token
    
    PumpSwap pools always have exactly one SOL side and one token side, both owned
    by the pool authority, so this is reliable.
    
    Returns: (pool_address, token_mint), either may be None if not found.
    """
    if not tx_info.HasField("meta"):
        return None, None

    meta = tx_info.meta
    balances = list(meta.pre_token_balances) + list(meta.post_token_balances)
    if not balances:
        return None, None

    # Group balances by owner
    by_owner: dict[str, list[str]] = {}
    for bal in balances:
        if not bal.owner:
            continue
        mints = by_owner.setdefault(bal.owner, [])
        if bal.mint not in mints:
            mints.append(bal.mint)

    # Find owners that hold both SOL and a non-SOL mint - that's a pool
    for owner, mints in by_owner.items():
        if WSOL_MINT not in mints:
            continue
        non_sol = [m for m in mints if m != WSOL_MINT]
        if not non_sol:
            continue
        # Found a pool - return owner as pool address, first non-sol mint as token
        return owner, non_sol[0]

    return None, None


def extract_block_time(tx_update) -> Optional[float]:
    """
    Get block_time from a gRPC TransactionUpdate if present.
    Yellowstone exposes it via the wrapper's created_at if at all - varies by version.
    Returns None if not available - the indexer will record received_at instead.
    """
    # Yellowstone's transaction update doesn't always include block_time directly.
    # We can attempt created_at on the parent SubscribeUpdate but the caller has that.
    return None


import struct

# ── Compute Budget + Jito tip extraction (tx-level, additive) ───────────────

COMPUTE_BUDGET_PROGRAM_ID = "ComputeBudget111111111111111111111111111111"
SET_CU_PRICE_DISCRIMINATOR = 0x03  # SetComputeUnitPrice: 0x03 || u64 LE micro_lamports

JITO_TIP_ACCOUNTS = frozenset({
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
})


def _full_account_keys(tx_info) -> list[str]:
    if not tx_info.HasField("transaction"):
        return []
    msg = tx_info.transaction.message
    keys = [base58.b58encode(k).decode("ascii") for k in msg.account_keys]
    if tx_info.HasField("meta"):
        keys += [base58.b58encode(k).decode("ascii") for k in tx_info.meta.loaded_writable_addresses]
        keys += [base58.b58encode(k).decode("ascii") for k in tx_info.meta.loaded_readonly_addresses]
    return keys


def extract_compute_units_consumed(tx_info) -> Optional[int]:
    if not tx_info.HasField("meta"):
        return None
    if not tx_info.meta.HasField("compute_units_consumed"):
        return None
    return int(tx_info.meta.compute_units_consumed)


def extract_priority_fee_micro_lamports(tx_info) -> int:
    if not tx_info.HasField("transaction"):
        return 0
    msg = tx_info.transaction.message
    cb_index = None
    for i, k in enumerate(msg.account_keys):
        if base58.b58encode(k).decode("ascii") == COMPUTE_BUDGET_PROGRAM_ID:
            cb_index = i
            break
    if cb_index is None:
        return 0
    for ix in msg.instructions:
        if ix.program_id_index != cb_index:
            continue
        data = bytes(ix.data)
        if len(data) < 9 or data[0] != SET_CU_PRICE_DISCRIMINATOR:
            continue
        try:
            (micro_lamports,) = struct.unpack("<Q", data[1:9])
            return int(micro_lamports)
        except Exception:
            continue
    return 0


def calc_priority_fee_lamports(micro_lamports: int, cu_consumed: Optional[int]) -> int:
    if not micro_lamports or not cu_consumed:
        return 0
    return (micro_lamports * cu_consumed + 999_999) // 1_000_000


def extract_jito_tip_lamports(tx_info) -> int:
    if not tx_info.HasField("meta"):
        return 0
    meta = tx_info.meta
    keys = _full_account_keys(tx_info)
    if not keys:
        return 0
    pre = list(meta.pre_balances)
    post = list(meta.post_balances)
    n = min(len(keys), len(pre), len(post))
    total = 0
    for i in range(n):
        if keys[i] in JITO_TIP_ACCOUNTS:
            diff = int(post[i]) - int(pre[i])
            if diff > 0:
                total += diff
    return total


# ── Ante Phase 1 tx-level extractors ─────────────────────────────────────────
# Both tx-level; the indexer attaches them only to the first event row of a
# multi-event tx so the value isn't duplicated across rows.

def extract_base_fee_lamports(tx_info) -> Optional[int]:
    """
    tx.meta.fee — the Solana transaction base fee in lamports.
    Equals 5000 × signature_count plus any tx-level network charges.
    Returns None if meta is missing (rare — only when a tx was evicted).
    """
    if not tx_info.HasField("meta"):
        return None
    return int(tx_info.meta.fee)


def extract_signature_count(tx_info) -> Optional[int]:
    """
    Number of signatures on the tx. Useful as a wash/bundler signal:
    organic single-user swaps sign once; bundled/sandwiched swaps often
    carry 2+ signatures.
    """
    if not tx_info.HasField("transaction"):
        return None
    return len(tx_info.transaction.signatures)