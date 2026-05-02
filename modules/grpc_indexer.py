"""
modules/grpc_indexer.py — Chainstack Yellowstone gRPC indexer for PumpSwap fees.

Pass 1: Connection + subscription only.
- Connects to Chainstack Yellowstone via gRPC with x-token auth
- Subscribes to PumpSwap program transactions
- Logs message counts every 10 seconds
- Auto-reconnects on disconnect with exponential backoff
- No decoding yet (Pass 2)
- No DB writes yet (Pass 2)
"""

import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import Callable, Optional

# Add ./generated to sys.path so the protoc-generated stubs can find each other
# (geyser_pb2 imports solana_storage_pb2 with absolute imports — see project notes)
_GENERATED_DIR = str(Path(__file__).parent.parent / "generated")
if _GENERATED_DIR not in sys.path:
    sys.path.insert(0, _GENERATED_DIR)

import grpc
from dotenv import load_dotenv

import geyser_pb2
import geyser_pb2_grpc

from database import DB_PATH, save_pumpswap_fees_batch
from utils.grpc_decoder import (
    signature_to_base58,
    extract_log_messages,
    extract_program_data_bytes,
    extract_pool_and_mint,
    extract_compute_units_consumed,
    extract_priority_fee_micro_lamports,
    calc_priority_fee_lamports,
    extract_jito_tip_lamports,
    extract_base_fee_lamports,
    extract_signature_count,
)
from utils.onchain_fees import (
    BUY_EVENT_DISC,
    SELL_EVENT_DISC,
    _parse_fees_from_event,
)

load_dotenv()

logger = logging.getLogger(__name__)

# ── Configuration ───────────────────────────────────────────────────────────

PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

ENDPOINT = os.getenv("CHAINSTACK_GRPC_ENDPOINT", "").replace("https://", "").replace("http://", "").rstrip("/")
XTOKEN = os.getenv("CHAINSTACK_GRPC_XTOKEN", "")
INDEXER_ENABLED = os.getenv("GRPC_INDEXER_ENABLED", "false").lower() == "true"

# Multi-pool event attribution filter (forward-only fix).
# Events whose embedded pool pubkey (offset 120 of the event payload) doesn't
# match the tx's primary pool are skipped. Dry-run logs and counts without
# actually dropping — use for the first deploy to confirm expected behavior.
POOL_FILTER_DRY_RUN = os.getenv("GRPC_INDEXER_POOL_FILTER_DRY_RUN", "false").lower() == "true"

# Reconnection backoff
INITIAL_BACKOFF = 2.0
MAX_BACKOFF = 60.0

# Stats logging interval
STATS_INTERVAL = 10.0


# ── Auth plugin ─────────────────────────────────────────────────────────────

class _XTokenAuth(grpc.AuthMetadataPlugin):
    """Injects x-token header into every gRPC request."""
    def __init__(self, token: str):
        self._token = token

    def __call__(self, context, callback):
        callback((("x-token", self._token),), None)


# ── Channel + subscription helpers ──────────────────────────────────────────

def _build_channel() -> grpc.aio.Channel:
    """Construct an authenticated TLS gRPC channel to Chainstack."""
    ssl_creds = grpc.ssl_channel_credentials()
    auth_creds = grpc.metadata_call_credentials(_XTokenAuth(XTOKEN))
    combined = grpc.composite_channel_credentials(ssl_creds, auth_creds)
    target = f"{ENDPOINT}:443"
    # max_receive_message_length: 64 MB - some Solana blocks are large
    options = [
        ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
    ]
    return grpc.aio.secure_channel(target, combined, options=options)


def _build_subscribe_request() -> geyser_pb2.SubscribeRequest:
    """
    Build the SubscribeRequest that filters for PumpSwap program transactions.
    Only successful (non-failed) transactions, confirmed commitment level.
    """
    req = geyser_pb2.SubscribeRequest()

    # One transaction filter named "pumpswap"
    tx_filter = req.transactions["pumpswap"]
    tx_filter.account_include.append(PUMPSWAP_PROGRAM)
    tx_filter.vote = False
    tx_filter.failed = False

    req.commitment = geyser_pb2.CommitmentLevel.CONFIRMED

    return req


async def _request_iterator(initial_request: geyser_pb2.SubscribeRequest):
    """
    Async generator that yields the initial subscribe request, then idles.
    gRPC bidirectional streaming requires a request iterator even though
    Yellowstone only needs the one initial request.
    """
    yield initial_request
    # Keep the generator alive forever - it ends when the outer task is cancelled
    while True:
        await asyncio.sleep(3600)


# ── Main indexer loop ───────────────────────────────────────────────────────

class GrpcIndexerStats:
    """Stats for the indexer's runtime."""
    def __init__(self):
        self.messages_received = 0
        self.transactions_seen = 0
        self.events_decoded = 0
        self.events_persisted = 0
        self.decode_failures = 0
        # Multi-pool attribution filter: total count of events whose embedded
        # pool pubkey did not match the tx's primary pool. In dry-run these are
        # counted but still persisted.
        self.events_filtered_non_primary_pool = 0
        self.start_time = time.time()
        self.last_log_time = time.time()
        self.last_log_messages = 0
        self.last_log_events = 0
        self.last_log_events_filtered = 0
        # Ante Phase 1 debug counters (tx-level, first-event-row samples)
        self.ante_samples = 0
        self.ante_sum = 0
        self.ante_min = None
        self.ante_max = None
        self.multi_sig_txs = 0
        self.last_log_ante_samples = 0
        self.last_log_ante_sum = 0
        # Observer callback failures (e.g. fast_dip_detector). Counted
        # but never propagated — the primary write path stays untouched.
        self.callback_errors = 0
        self.last_log_callback_errors = 0


# Batching config
BATCH_MAX_SIZE = 100
BATCH_MAX_AGE_SECONDS = 2.0


async def _consume_stream(
    channel: grpc.aio.Channel,
    stats: GrpcIndexerStats,
    on_event: Optional[Callable[..., None]] = None,
):
    """
    Open the Subscribe stream, decode PumpSwap fee events, batch-write to DB.

    `on_event` is an optional non-mutating observer invoked once per
    decoded fee event with the kwargs documented at the call site. It
    is wrapped in try/except so a misbehaving observer cannot affect
    the primary write path. Observers must NOT block: heavy work
    should be dispatched onto the event loop via asyncio.create_task
    inside the callback. The callback is called AFTER the event has
    been queued for batch insert, so observer logic never delays the
    DB write either.
    """
    stub = geyser_pb2_grpc.GeyserStub(channel)
    request = _build_subscribe_request()

    logger.info(f"📡 gRPC indexer subscribing to PumpSwap program {PUMPSWAP_PROGRAM[:8]}...")

    stream = stub.Subscribe(_request_iterator(request))

    pending: list[dict] = []
    last_flush = time.time()

    async def flush():
        nonlocal pending, last_flush
        if pending:
            try:
                written = await save_pumpswap_fees_batch(pending)
                stats.events_persisted += written
            except Exception as e:
                logger.error(f"📡 batch write failed ({len(pending)} records): {e}")
            pending = []
        last_flush = time.time()

    async for update in stream:
        stats.messages_received += 1

        kind = update.WhichOneof("update_oneof")
        if kind != "transaction":
            continue

        stats.transactions_seen += 1
        tx_update = update.transaction
        tx_info = tx_update.transaction
        slot = tx_update.slot

        # Pull log messages and look for Anchor event emissions
        logs = extract_log_messages(tx_info)
        if not logs:
            continue

        event_payloads = extract_program_data_bytes(logs)
        if not event_payloads:
            continue

        # Find PumpSwap fee events in the payloads
        fee_events = []
        for data in event_payloads:
            if len(data) < 8:
                continue
            disc = data[:8]
            if disc == BUY_EVENT_DISC:
                fees = _parse_fees_from_event(data, BUY_EVENT_DISC)
                if fees:
                    fee_events.append(("buy", fees))
            elif disc == SELL_EVENT_DISC:
                fees = _parse_fees_from_event(data, SELL_EVENT_DISC)
                if fees:
                    fee_events.append(("sell", fees))

        if not fee_events:
            continue

        # Resolve pool address and token mint from token balances
        pool_address, token_mint = extract_pool_and_mint(tx_info)
        if not pool_address:
            stats.decode_failures += 1
            continue

        signature_b58 = signature_to_base58(tx_info.signature)

        # Filter: drop events whose embedded pool pubkey doesn't match the
        # tx's primary pool. Multi-hop txs (e.g., Jupiter routes) emit events
        # from multiple pools in one tx log; without this filter every event
        # gets attributed to whichever pool extract_pool_and_mint picked,
        # corrupting peak/price calculations downstream.
        kept_events = []
        for event_type, fees in fee_events:
            evt_pool = fees.get("event_pool_address")
            if evt_pool and evt_pool != pool_address:
                stats.events_filtered_non_primary_pool += 1
                if POOL_FILTER_DRY_RUN:
                    logger.debug(
                        "DRY-RUN: would skip event from non-primary pool %s "
                        "(primary: %s) in tx %s",
                        evt_pool, pool_address, signature_b58,
                    )
                    kept_events.append((event_type, fees))
                else:
                    logger.debug(
                        "Skipping event from non-primary pool %s "
                        "(primary: %s) in tx %s",
                        evt_pool, pool_address, signature_b58,
                    )
                continue
            kept_events.append((event_type, fees))

        if not kept_events:
            continue
        fee_events = kept_events

        # Tx-level fields — computed once, attached to first event row only
        cu_consumed = extract_compute_units_consumed(tx_info)
        cu_price_micro = extract_priority_fee_micro_lamports(tx_info)
        priority_fee_lamports = calc_priority_fee_lamports(cu_price_micro, cu_consumed)
        jito_tip_lamports = extract_jito_tip_lamports(tx_info)
        base_fee_lamports = extract_base_fee_lamports(tx_info)
        sig_count = extract_signature_count(tx_info)

        # Ante Phase 1 verbose sampling (first event row, tx-level Ante burn)
        if base_fee_lamports is not None:
            stats.ante_samples += 1
            ante_lamports = (base_fee_lamports or 0) + (priority_fee_lamports or 0) + (jito_tip_lamports or 0)
            stats.ante_min = min(stats.ante_min, ante_lamports) if stats.ante_min is not None else ante_lamports
            stats.ante_max = max(stats.ante_max, ante_lamports) if stats.ante_max is not None else ante_lamports
            stats.ante_sum += ante_lamports
            if sig_count is not None and sig_count >= 2:
                stats.multi_sig_txs += 1

        # One DB row per fee event in the tx
        for idx, (event_type, fees) in enumerate(fee_events):
            is_first = (idx == 0)
            event_block_time = float(fees["timestamp"])
            pending.append({
                "signature": signature_b58,
                "slot": slot,
                "block_time": event_block_time,
                "pool_address": pool_address,
                "token_address": token_mint,
                "event_type": event_type,
                "quote_amount": fees["quote_amount"],
                "base_amount": fees.get("base_amount"),
                "lp_fee": fees["lp_fee"],
                "protocol_fee": fees["protocol_fee"],
                "creator_fee": fees.get("creator_fee", 0),
                "user_pubkey": fees.get("user_pubkey"),
                "priority_fee": priority_fee_lamports if is_first else None,
                "jito_tip": jito_tip_lamports if is_first else None,
                "compute_units_consumed": cu_consumed if is_first else None,
                "base_fee": base_fee_lamports if is_first else None,
                "signature_count": sig_count if is_first else None,
            })
            stats.events_decoded += 1

            # Notify observers (e.g. fast_dip_detector). Wrapped to
            # guarantee detector failures never affect the write path.
            if on_event is not None:
                try:
                    on_event(
                        token_address=token_mint,
                        pool_address=pool_address,
                        block_time=event_block_time,
                        quote_amount=fees["quote_amount"],
                        base_amount=fees.get("base_amount"),
                        event_type=event_type,
                        signature=signature_b58,
                    )
                except Exception as cb_err:
                    stats.callback_errors += 1
                    # Log first few + every 100th to avoid log floods.
                    if stats.callback_errors <= 5 or stats.callback_errors % 100 == 0:
                        logger.warning(
                            f"📡 on_event callback failed (#{stats.callback_errors}): "
                            f"{type(cb_err).__name__}: {cb_err}"
                        )

        now = time.time()

        # Flush conditions — trigger when batch fills or ages out.
        if (
            len(pending) >= BATCH_MAX_SIZE
            or (now - last_flush) >= BATCH_MAX_AGE_SECONDS
        ):
            await flush()

        # Periodic stats log
        if now - stats.last_log_time >= STATS_INTERVAL:
            interval = now - stats.last_log_time
            msg_delta = stats.messages_received - stats.last_log_messages
            ev_delta = stats.events_decoded - stats.last_log_events
            msg_rate = msg_delta / interval if interval > 0 else 0
            ev_rate = ev_delta / interval if interval > 0 else 0
            # Ante Phase 1 interval stats
            ante_delta = stats.ante_samples - stats.last_log_ante_samples
            ante_sum_delta = stats.ante_sum - stats.last_log_ante_sum
            if ante_delta > 0:
                avg_ante_sol = (ante_sum_delta / ante_delta) / 1_000_000_000
                ante_fragment = (
                    f" | ante samples={ante_delta} "
                    f"avg={avg_ante_sol:.6f} SOL "
                    f"min={stats.ante_min or 0} max={stats.ante_max or 0} lamp "
                    f"multisig={stats.multi_sig_txs}"
                )
            else:
                ante_fragment = ""
            filt_delta = (
                stats.events_filtered_non_primary_pool
                - stats.last_log_events_filtered
            )
            filt_tag = "dry-run filtered" if POOL_FILTER_DRY_RUN else "filtered"
            cb_delta = stats.callback_errors - stats.last_log_callback_errors
            cb_fragment = f", cb_errors={cb_delta}" if cb_delta else ""
            logger.info(
                f"📡 gRPC: {stats.transactions_seen} txs, "
                f"{stats.events_decoded} events decoded ({stats.events_persisted} persisted), "
                f"{msg_rate:.0f} msg/s, {ev_rate:.1f} ev/s, "
                f"{stats.decode_failures} decode failures, "
                f"{filt_delta} {filt_tag} (non-primary pool, "
                f"total={stats.events_filtered_non_primary_pool})"
                f"{cb_fragment}"
                f"{ante_fragment}"
            )
            stats.last_log_time = now
            stats.last_log_messages = stats.messages_received
            stats.last_log_events = stats.events_decoded
            stats.last_log_events_filtered = stats.events_filtered_non_primary_pool
            stats.last_log_ante_samples = stats.ante_samples
            stats.last_log_ante_sum = stats.ante_sum
            stats.last_log_callback_errors = stats.callback_errors
            # Reset min/max per interval so we see fresh ranges each log
            stats.ante_min = None
            stats.ante_max = None

    # Stream ended - flush whatever's left
    await flush()


async def run_grpc_indexer(on_event: Optional[Callable[..., None]] = None):
    """
    Top-level entry point. Connects, consumes stream, reconnects on failure
    with exponential backoff. Runs forever until cancelled.

    `on_event` is forwarded to `_consume_stream` — see its docstring.
    """
    if not INDEXER_ENABLED:
        logger.info("📡 gRPC indexer disabled (GRPC_INDEXER_ENABLED != true), skipping startup")
        return

    if not ENDPOINT or not XTOKEN:
        logger.error("📡 gRPC indexer: CHAINSTACK_GRPC_ENDPOINT or CHAINSTACK_GRPC_XTOKEN not set in .env")
        return

    logger.info(f"📡 gRPC indexer starting, endpoint: {ENDPOINT}")

    stats = GrpcIndexerStats()
    backoff = INITIAL_BACKOFF

    while True:
        channel = None
        try:
            channel = _build_channel()
            await asyncio.wait_for(channel.channel_ready(), timeout=15.0)
            logger.info("📡 gRPC channel ready, opening subscription stream")
            backoff = INITIAL_BACKOFF  # reset on successful connect

            await _consume_stream(channel, stats, on_event=on_event)

            # _consume_stream returning means the stream ended normally - reconnect
            logger.warning("📡 gRPC stream ended normally, reconnecting...")

        except asyncio.CancelledError:
            logger.info("📡 gRPC indexer cancelled, shutting down")
            raise
        except grpc.aio.AioRpcError as e:
            logger.error(f"📡 gRPC error {e.code().name}: {e.details()}")
        except asyncio.TimeoutError:
            logger.error("📡 gRPC channel did not become ready within 15s")
        except Exception as e:
            logger.exception(f"📡 gRPC indexer unexpected error: {type(e).__name__}: {e}")
        finally:
            if channel is not None:
                try:
                    await channel.close()
                except Exception:
                    pass

        # Backoff before reconnect
        logger.info(f"📡 gRPC indexer reconnecting in {backoff:.1f}s...")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
