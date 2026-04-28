"""
utils/onchain_fees.py — On-chain fee reader for PumpSwap tokens.
Reads actual lp_fee + protocol_fee from BuyEvent/SellEvent transaction logs.
No estimation — exact on-chain data.

PumpSwap BuyEvent discriminator:  [103, 244, 82, 31, 44, 245, 119, 119]
PumpSwap SellEvent discriminator: [62, 47, 55, 10, 165, 3, 220, 42]

BuyEvent fields (in order, all u64 unless noted):
  timestamp(i64), base_amount_out, max_quote_amount_in,
  user_base_token_reserves, user_quote_token_reserves,
  pool_base_token_reserves, pool_quote_token_reserves,
  quote_amount_in, lp_fee_basis_points, lp_fee,
  protocol_fee_basis_points, protocol_fee,
  quote_amount_in_with_lp_fee, user_quote_amount_in,
  pool(pubkey), user(pubkey), ...

SellEvent fields (in order, all u64 unless noted):
  timestamp(i64), base_amount_in, min_quote_amount_out,
  user_base_token_reserves, user_quote_token_reserves,
  pool_base_token_reserves, pool_quote_token_reserves,
  quote_amount_out, lp_fee_basis_points, lp_fee,
  protocol_fee_basis_points, protocol_fee,
  quote_amount_out_without_lp_fee, user_quote_amount_out,
  pool(pubkey), user(pubkey), ...
"""

import asyncio
import aiohttp
import base64
import logging
import struct
import base58
import time
from typing import Optional

logger = logging.getLogger(__name__)

# PumpSwap program
PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

# Anchor event discriminators (first 8 bytes of base64-decoded event data)
BUY_EVENT_DISC  = bytes([103, 244, 82, 31, 44, 245, 119, 119])
SELL_EVENT_DISC = bytes([62, 47, 55, 10, 165, 3, 220, 42])

LAMPORTS_PER_SOL = 1_000_000_000


def _parse_fees_from_event(data: bytes, disc: bytes) -> Optional[dict]:
    """
    Parse lp_fee, protocol_fee, and coin_creator_fee from a BuyEvent or SellEvent.
    
    Verified offsets via manual_decode_test.py:
      lp_fee       at offset 80   (field 9)
      protocol_fee at offset 96   (field 11)
      creator_fee  at offset 352  (field 22, after 7 pubkeys + bps)
    
    All three match (amount * bps / 10000) within ~1.1% rounding.
    """
    try:
        if len(data) < 360:  # Need at least through coin_creator_fee
            return None
        
        timestamp = struct.unpack_from('<q', data, 8)[0]
        base_amount = struct.unpack_from('<q', data, 16)[0]
        quote_amount = struct.unpack_from('<Q', data, 64)[0]
        lp_fee = struct.unpack_from('<Q', data, 80)[0]
        protocol_fee = struct.unpack_from('<Q', data, 96)[0]
        creator_fee = struct.unpack_from('<Q', data, 352)[0]
        # The event's pool pubkey lives at offset 120 (the `pool` field that
        # sits between user_quote_amount_in and the user pubkey in the event
        # layout). Used by the indexer to filter out multi-hop events that
        # don't belong to the tx's primary pool.
        event_pool_address = base58.b58encode(data[120:152]).decode("ascii")
        user_pubkey = base58.b58encode(data[152:184]).decode("ascii")

        return {
            "timestamp": timestamp,
            "base_amount": base_amount,
            "quote_amount": quote_amount,
            "lp_fee": lp_fee,
            "protocol_fee": protocol_fee,
            "creator_fee": creator_fee,
            "total_fee": lp_fee + protocol_fee + creator_fee,
            "event_pool_address": event_pool_address,
            "user_pubkey": user_pubkey,
        }
    except Exception as e:
        logger.debug(f"Fee parse error: {e}")
        return None


def _extract_events_from_logs(logs: list[str]) -> list[dict]:
    """
    Extract BuyEvent/SellEvent fee data from transaction log messages.
    Anchor emits events as base64-encoded data after "Program data:" prefix.
    """
    events = []
    
    for log in logs:
        if not log.startswith("Program data: "):
            continue
        
        b64_data = log[len("Program data: "):]
        
        try:
            data = base64.b64decode(b64_data)
        except Exception:
            continue
        
        if len(data) < 8:
            continue
        
        disc = data[:8]
        
        if disc == BUY_EVENT_DISC or disc == SELL_EVENT_DISC:
            fees = _parse_fees_from_event(data, disc)
            if fees:
                event_type = "buy" if disc == BUY_EVENT_DISC else "sell"
                fees["type"] = event_type
                events.append(fees)
    
    return events


async def get_global_fees(
    pool_address: str,
    rpc_url: str,
    session: aiohttp.ClientSession,
    max_signatures: int = 0,  # 0 = fetch all
) -> Optional[dict]:
    """
    Calculate total global fees for a PumpSwap pool by parsing all transactions.
    
    Args:
        pool_address: The PumpSwap pool address
        rpc_url: Helius RPC URL with API key
        session: aiohttp session
        max_signatures: Max signatures to fetch (0 = all)
    
    Returns:
        Dict with total_fees_sol, total_lp_fee_sol, total_protocol_fee_sol,
        tx_count, event_count, or None on failure.
    """
    try:
        # Step 1: Get all transaction signatures for the pool
        all_sigs = []
        before = None
        
        while True:
            sigs_batch = await _get_signatures(
                pool_address, rpc_url, session, before=before, limit=1000
            )
            
            if not sigs_batch:
                break
            
            all_sigs.extend(sigs_batch)
            
            if max_signatures > 0 and len(all_sigs) >= max_signatures:
                all_sigs = all_sigs[:max_signatures]
                break
            
            if len(sigs_batch) < 1000:
                break  # No more pages
            
            before = sigs_batch[-1]
            await asyncio.sleep(0.1)  # Rate limit courtesy
        
        if not all_sigs:
            logger.debug(f"No signatures found for pool {pool_address[:8]}")
            return None
        
        logger.info(f"Found {len(all_sigs)} signatures for pool {pool_address[:8]}")
        
        # Step 2: Fetch transactions and parse events
        total_lp_fee = 0
        total_protocol_fee = 0
        event_count = 0
        tx_count = 0
        errors = 0
        
        # Process in batches of 20 to avoid rate limits
        batch_size = 20
        for i in range(0, len(all_sigs), batch_size):
            batch = all_sigs[i:i + batch_size]
            tasks = [_get_transaction_logs(sig, rpc_url, session) for sig in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for sig, result in zip(batch, results):
                if isinstance(result, Exception):
                    errors += 1
                    continue
                
                if result is None:
                    errors += 1
                    continue
                
                tx_count += 1
                events = _extract_events_from_logs(result)
                
                for event in events:
                    total_lp_fee += event["lp_fee"]
                    total_protocol_fee += event["protocol_fee"]
                    event_count += 1
            
            # Rate limit between batches
            await asyncio.sleep(0.2)
        
        total_fees_lamports = total_lp_fee + total_protocol_fee
        total_fees_sol = total_fees_lamports / LAMPORTS_PER_SOL
        
        result = {
            "total_fees_sol": total_fees_sol,
            "total_lp_fee_sol": total_lp_fee / LAMPORTS_PER_SOL,
            "total_protocol_fee_sol": total_protocol_fee / LAMPORTS_PER_SOL,
            "tx_count": tx_count,
            "event_count": event_count,
            "sig_count": len(all_sigs),
            "errors": errors,
        }
        
        logger.info(
            f"Pool {pool_address[:8]} fees: {total_fees_sol:.2f} SOL "
            f"(LP: {result['total_lp_fee_sol']:.2f}, Protocol: {result['total_protocol_fee_sol']:.2f}) "
            f"from {event_count} events in {tx_count} txs"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Global fee calculation error for {pool_address[:8]}: {e}")
        return None


async def _get_signatures(
    address: str,
    rpc_url: str,
    session: aiohttp.ClientSession,
    before: Optional[str] = None,
    limit: int = 1000,
) -> list[str]:
    """Get transaction signatures for an address."""
    params = [
        address,
        {"limit": limit, "commitment": "confirmed"},
    ]
    if before:
        params[1]["before"] = before
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": params,
    }
    
    try:
        async with session.post(
            rpc_url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            data = await resp.json()
        
        result = data.get("result", [])
        # Only include confirmed/finalized successful txs
        return [
            item["signature"]
            for item in result
            if item.get("err") is None
        ]
    except Exception as e:
        logger.error(f"getSignaturesForAddress error: {e}")
        return []


async def _get_transaction_logs(
    signature: str,
    rpc_url: str,
    session: aiohttp.ClientSession,
) -> Optional[list[str]]:
    """Fetch transaction and return its log messages."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            },
        ],
    }
    
    try:
        async with session.post(
            rpc_url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            data = await resp.json()
        
        tx = data.get("result")
        if not tx:
            return None
        
        logs = tx.get("meta", {}).get("logMessages", [])
        return logs if logs else None
        
    except Exception as e:
        logger.debug(f"getTransaction error ({signature[:16]}): {e}")
        return None