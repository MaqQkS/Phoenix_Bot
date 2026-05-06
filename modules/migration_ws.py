"""
modules/migration_ws.py
Real-time migration detection via Helius WebSocket (logsSubscribe).
Subscribes to PumpSwap program logs and filters for migration events.
Never misses a migration — events are streamed as they happen on-chain.

If Dexscreener hasn't indexed a pair yet, the mint goes into a retry queue
that is drained once per outer price-loop cycle (tracking.poll_interval_seconds,
default ~30s) and gives up after 5 minutes.

If Birdeye has no candle data yet (token too fresh), the token goes into an
ATH retry queue. Retry cadence is config-driven (config.yaml → ath_retry):
aggressive during the hot window (default 30s × 10min) and backs off during
the sustained window (default 2min × up to 30min total). Retries stop as
soon as Birdeye succeeds (token.ath_source flips to 'birdeye'); on timeout
the token's ath_source transitions from 'fallback' / 'unseeded' to
'running_max' and the live-poll running max becomes authoritative.
"""

import asyncio
import base64
import json
import logging
import re
import time
from typing import Optional

import aiohttp
import base58
import websockets

import database as db
from models import TrackedToken, TokenStatus
from utils.dexscreener import get_pumpswap_pair, extract_price_data, get_sol_price
from modules.inspection_gate import check_inception_bundle
from modules import ath_refresh_shadow, ath_seeder

logger = logging.getLogger(__name__)


# Programs
PUMP_PROGRAM     = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

# Reconnection settings
MAX_RETRIES = 50
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 60
PING_INTERVAL = 30


def _register_grpc_pool_meta(token: TrackedToken) -> None:
    """Push freshly-fetched pool metadata into the gRPC indexer's in-memory
    cache so price derivation can start on the very first event for this
    pool, without waiting for the 60s periodic refresh. No-ops when either
    field is None or when the indexer module hasn't been imported yet."""
    if token.pool_orientation is None or token.token_decimals is None:
        return
    try:
        from modules.grpc_indexer import register_pool_meta
        register_pool_meta(
            pool_address=token.pool_address,
            token_address=token.address,
            orientation=token.pool_orientation,
            decimals=token.token_decimals,
        )
    except ImportError:
        pass


async def fetch_pool_metadata(
    session: aiohttp.ClientSession,
    rpc_url: str,
    pool_address: str,
    token_address: str,
) -> tuple[Optional[str], Optional[int]]:
    """Fetch pool orientation + token decimals via getAccountInfo.

    Returns (orientation, decimals); either may be None on failure.
      orientation = 'normal'   → base_mint == token  (meme=base, WSOL=quote)
      orientation = 'inverted' → quote_mint == token (WSOL=base, meme=quote)
      orientation = None       → neither mint matches or RPC failed

    Pool struct offsets (verified in scripts/verify_pool_offset_120.py
    and diagnostics_out/blind_dip_investigation/validate_orientation.py):
      after the 8-byte anchor discriminator,
        +35 base_mint  (32B) → absolute [43:75]
        +67 quote_mint (32B) → absolute [75:107]
    SPL mint decimals live at byte 44 of the mint account data.
    """
    orientation: Optional[str] = None
    decimals: Optional[int] = None

    # Pool account — extract base_mint + quote_mint
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [pool_address, {"encoding": "base64"}],
        }
        async with session.post(
            rpc_url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            data = await resp.json()
        info = (data.get("result") or {}).get("value")
        if info:
            data_field = info.get("data") or []
            b64 = data_field[0] if data_field else None
            if b64:
                raw = base64.b64decode(b64)
                if len(raw) >= 107:
                    base_mint  = base58.b58encode(raw[43:75]).decode("ascii")
                    quote_mint = base58.b58encode(raw[75:107]).decode("ascii")
                    if base_mint == token_address:
                        orientation = "normal"
                    elif quote_mint == token_address:
                        orientation = "inverted"
                    else:
                        logger.warning(
                            f"Pool {pool_address[:8]} orientation unknown: "
                            f"base={base_mint[:8]} quote={quote_mint[:8]} "
                            f"token={token_address[:8]}"
                        )
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        logger.warning(f"Pool metadata fetch timed out for {pool_address[:8]}")
    except Exception as e:
        logger.warning(f"Pool metadata fetch error for {pool_address[:8]}: {e}")

    # SPL mint account — decimals at byte 44
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [token_address, {"encoding": "base64"}],
        }
        async with session.post(
            rpc_url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            data = await resp.json()
        info = (data.get("result") or {}).get("value")
        if info:
            data_field = info.get("data") or []
            b64 = data_field[0] if data_field else None
            if b64:
                raw = base64.b64decode(b64)
                if len(raw) >= 45:
                    decimals = raw[44]
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        logger.warning(f"Mint fetch timed out for {token_address[:8]}")
    except Exception as e:
        logger.warning(f"Mint fetch error for {token_address[:8]}: {e}")

    return orientation, decimals


class MigrationWebSocket:
    def __init__(self, config: dict):
        self.config = config
        self.api_key = config["helius"]["api_key"]
        self.rpc_url = config["helius"]["rpc_url"]
        self.ws_url = f"wss://mainnet.helius-rpc.com/?api-key={self.api_key}"
        self._seen_sigs: set[str] = set()
        self._retry_queue: dict[str, tuple[float, int | None]] = {}  # mint -> (first_seen, slot)
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._retry_count = 0

    async def run(self, session: aiohttp.ClientSession):
        """
        Main loop — connects to websocket, subscribes, and processes events.
        Automatically reconnects on disconnect with exponential backoff.
        """
        self._http_session = session

        while True:
            try:
                await self._connect_and_listen()
            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.ConnectionClosedError,
                ConnectionRefusedError,
                OSError,
            ) as e:
                self._retry_count += 1
                delay = min(
                    INITIAL_RETRY_DELAY * (2 ** (self._retry_count - 1)),
                    MAX_RETRY_DELAY,
                )
                logger.warning(
                    f"WebSocket disconnected: {e} | "
                    f"Reconnecting in {delay}s (attempt {self._retry_count})"
                )
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                logger.info("Migration WebSocket task cancelled")
                raise
            except Exception as e:
                self._retry_count += 1
                delay = min(
                    INITIAL_RETRY_DELAY * (2 ** (self._retry_count - 1)),
                    MAX_RETRY_DELAY,
                )
                logger.error(
                    f"WebSocket unexpected error: {e} | "
                    f"Reconnecting in {delay}s (attempt {self._retry_count})"
                )
                await asyncio.sleep(delay)

    async def _connect_and_listen(self):
        """Connect to Helius WS, subscribe to PumpSwap logs, process messages."""
        logger.info("Connecting to Helius WebSocket...")

        async with websockets.connect(
            self.ws_url,
            ping_interval=PING_INTERVAL,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            subscribe_msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": [PUMPSWAP_PROGRAM]},
                    {"commitment": "confirmed"},
                ],
            }
            await ws.send(json.dumps(subscribe_msg))

            response = await ws.recv()
            resp_data = json.loads(response)
            if "result" in resp_data:
                logger.info(
                    f"Subscribed to PumpSwap logs (subscription ID: {resp_data['result']})"
                )
                self._retry_count = 0
            else:
                logger.error(f"Subscription failed: {resp_data}")
                return

            async for message in ws:
                try:
                    await self._handle_message(message)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error handling WS message: {e}")

    async def _handle_message(self, raw_message: str):
        """Parse a websocket message and check if it's a migration."""
        data = json.loads(raw_message)

        result = data.get("params", {}).get("result", {})
        if not result:
            return

        migration_slot = result.get("context", {}).get("slot")
        value = result.get("value", {})
        signature = value.get("signature", "")
        logs = value.get("logs", [])

        if not signature or not logs:
            return

        if signature in self._seen_sigs:
            return

        # Check if this is a migration event (exact match only)
        is_migration = any(
            log == "Program log: Instruction: Migrate"
            for log in logs
        )
        if not is_migration:
            return

        # Skip "already migrated" duplicate attempts
        already_migrated = any(
            "Bonding curve already migrated" in log
            for log in logs
        )
        if already_migrated:
            return

        # Confirm pump.fun program was involved
        pump_involved = any(PUMP_PROGRAM in log for log in logs)
        if not pump_involved:
            return

        self._seen_sigs.add(signature)

        logger.info(f"Migration event detected! sig: {signature}")

        # Fetch full transaction to extract the mint
        mint = await self._get_mint_from_tx(signature)
        if not mint:
            logger.debug(f"Could not extract mint from {signature}")
            return

        logger.info(f"Mint found: {mint}")

        # Skip if already tracking
        if await db.token_exists(mint):
            return

        # Build and save the token
        token = await self._build_token(mint)
        if not token:
            # Dexscreener might not have indexed yet — queue for retry
            if mint not in self._retry_queue:
                self._retry_queue[mint] = (time.time(), migration_slot)
                logger.info(f"⏳ Queued {mint[:8]}... for retry (Dexscreener not ready)")
            return

        await db.save_token(token)
        _register_grpc_pool_meta(token)
        ath_refresh_shadow.observe_token_created(token)
        ath_refresh_shadow.log_status_transition(
            token.address, None, "tracking", token.migration_time, token.symbol
        )
        logger.info(
            f"🔀 New migration (WS): ${token.symbol} | {mint[:8]}... | "
            f"mcap ${token.migration_mcap:,.0f}"
        )

        # Seed ATH immediately — don't wait for price_tracker loop
        await ath_seeder.seed_ath_for_token(token, self._http_session, self.config)
        ath_refresh_shadow.schedule_reseeds(token.address, token.migration_time, token.symbol)


        # Keep seen set from growing forever
        if len(self._seen_sigs) > 5000:
            self._seen_sigs = set(list(self._seen_sigs)[-2000:])

    async def _get_mint_from_tx(self, signature: str) -> Optional[str]:
        """Fetch the full transaction and extract the token mint address."""
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
            async with self._http_session.post(
                self.rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()

            tx = data.get("result")
            if not tx:
                return None

            accounts = (
                tx.get("transaction", {})
                  .get("message", {})
                  .get("accountKeys", [])
            )

            # Look for pump.fun token mint (ends with "pump")
            for acc in accounts:
                if isinstance(acc, dict):
                    pubkey = acc.get("pubkey", "")
                else:
                    pubkey = str(acc)

                if pubkey.endswith("pump") and len(pubkey) >= 32:
                    return pubkey

            # Fallback: scan logs
            logs = tx.get("meta", {}).get("logMessages") or []
            for log in logs:
                matches = re.findall(r'[1-9A-HJ-NP-Za-km-z]{43,44}pump', log)
                if matches:
                    return matches[0]

            return None

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"getTransaction error ({signature[:16]}): {e}")
            return None

    async def _build_token(self, mint: str) -> Optional[TrackedToken]:
        """Build a TrackedToken for a freshly migrated token."""
        session = self._http_session

        # Dexscreener may need a moment to index — retry up to 3 times
        pair = None
        for attempt in range(3):
            pair = await get_pumpswap_pair(mint, session)
            if pair:
                break
            if attempt < 2:
                await asyncio.sleep(3)

        if not pair:
            logger.info(f"No PumpSwap pair found for {mint[:8]} after retries")
            return None

        data = extract_price_data(pair)
        price = data.get("price_usd", 0)
        mcap = data.get("mcap", 0)

        # price_usd is only a pair-validity check here — its implied
        # supply varies per pool, so it is not stored as migration_price.
        if price <= 0:
            return None

        # Dynamic migration mcap floor
        sol_price = await get_sol_price(session)
        min_migration_mcap = sol_price * 200
        if mcap < min_migration_mcap:
            logger.info(
                f"Skipping {mint[:8]} — mcap ${mcap:,.0f} below migration floor ${min_migration_mcap:,.0f}"
            )
            return None

        # Calculate proper migration mcap from SOL price
        migration_mcap = sol_price * 410
        # Derive migration_price from migration_mcap to share a single
        # supply basis (1B fixed) — keeps pump_multiple consistent.
        migration_price = migration_mcap / 1_000_000_000

        token = TrackedToken(
            address=mint,
            symbol=data.get("symbol", "???"),
            pool_address=data.get("pair_address", ""),
            status=TokenStatus.TRACKING,
            migration_price=migration_price,
            migration_mcap=migration_mcap,
            current_price=price,
            current_mcap=mcap,
            liquidity_usd=data.get("liquidity_usd", 0),
            ath_price=0.0,
            migration_time=time.time(),
            volume_1h=data.get("volume_1h", 0),
            volume_6h=data.get("volume_6h", 0),
            volume_24h=data.get("volume_24h", 0),
        )

        # Attach pool metadata for the gRPC price pipeline. Best-effort: any
        # RPC failure leaves the fields None and the indexer will silently
        # skip price derivation for this token. Never blocks migration.
        orientation, decimals = await self._fetch_pool_metadata(
            token.pool_address, token.address
        )
        token.pool_orientation = orientation
        token.token_decimals = decimals
        if orientation is not None and decimals is not None:
            logger.info(
                f"Pool metadata: ${token.symbol} "
                f"orientation={orientation} decimals={decimals}"
            )

        return token

    async def _fetch_pool_metadata(
        self, pool_address: str, token_address: str
    ) -> tuple[Optional[str], Optional[int]]:
        return await fetch_pool_metadata(
            self._http_session, self.rpc_url, pool_address, token_address
        )

    async def process_retry_queue(self):
        """Retry tokens that Dexscreener wasn't ready for."""
        if not self._retry_queue:
            return

        now = time.time()
        to_remove = []

        for mint, (first_seen, slot) in list(self._retry_queue.items()):
            # Give up after 5 minutes
            if now - first_seen > 300:
                logger.info(f"⏳ Giving up on {mint[:8]}... after 5 min")
                to_remove.append(mint)
                continue

            # Skip if already tracked (another event might have caught it)
            if await db.token_exists(mint):
                to_remove.append(mint)
                continue

            token = await self._build_token(mint)
            if token:
                await db.save_token(token)
                _register_grpc_pool_meta(token)
                ath_refresh_shadow.observe_token_created(token)
                ath_refresh_shadow.log_status_transition(
                    token.address, None, "tracking", token.migration_time, token.symbol
                )
                logger.info(
                    f"🔀 New migration (retry): ${token.symbol} | {mint[:8]}... | "
                    f"mcap ${token.migration_mcap:,.0f}"
                )
                await ath_seeder.seed_ath_for_token(token, self._http_session, self.config)
                ath_refresh_shadow.schedule_reseeds(token.address, token.migration_time, token.symbol)

                to_remove.append(mint)

        for mint in to_remove:
            self._retry_queue.pop(mint, None)

    async def process_ath_retry_queue(self):
        """Thin wrapper. Delegates to ath_seeder module.
        Preserved for main.py compatibility. Each pass is wrapped
        independently so a failure in one does not suppress the other."""
        try:
            await ath_seeder.process_retry_queue(self._http_session, self.config)
        except Exception as e:
            logger.error(f"ATH retry queue error: {e}")
        try:
            await ath_seeder.process_t15m_correction(self._http_session, self.config)
        except Exception as e:
            logger.error(f"T+15m correction error: {e}")