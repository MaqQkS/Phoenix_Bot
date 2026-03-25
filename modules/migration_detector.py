"""
modules/migration_detector.py
Polls Helius for new pump.fun → PumpSwap migration transactions.
When a migration is found, creates a TrackedToken and saves it to the DB.
"""
import asyncio 
import aiohttp
import logging
import time
from typing import Optional

import database as db
from models import TrackedToken, TokenStatus
from utils.dexscreener import get_pumpswap_pair, extract_price_data, get_sol_price

logger = logging.getLogger(__name__)

# pump.fun program that emits migration events
PUMP_PROGRAM    = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

# We look at the last N signatures per poll to catch new migrations
SIG_LIMIT = 50


class MigrationDetector:
    def __init__(self, config: dict):
        self.config    = config
        self.rpc_url   = config["helius"]["rpc_url"]
        self._seen_sigs: set[str] = set()  # avoid reprocessing

    async def check_new_migrations(
        self, session: aiohttp.ClientSession
    ) -> list[TrackedToken]:
        """
        Poll Helius for recent pump.fun migration transactions.
        Returns list of newly created TrackedToken objects.
        """
        sigs = await self._get_recent_signatures(session)
        if not sigs:
            return []

        new_tokens = []
        for sig_info in sigs:
            sig = sig_info.get("signature", "")
            if not sig or sig in self._seen_sigs:
                continue
            self._seen_sigs.add(sig)

            # Parse the transaction to find the migrated token mint
            mint = await self._parse_migration_tx(sig, session)
            if not mint:
                continue

            # Skip if already tracking this token
            if await db.token_exists(mint):
                continue

            # Create a new TrackedToken
            token = await self._build_token(mint, session)
            if token:
                await db.save_token(token)
                new_tokens.append(token)
                logger.info(f"🔀 New migration detected: ${token.symbol} | {mint[:8]}...")

        # Keep seen set from growing forever
        if len(self._seen_sigs) > 5000:
            self._seen_sigs = set(list(self._seen_sigs)[-2000:])

        return new_tokens

    async def _get_recent_signatures(
        self, session: aiohttp.ClientSession
    ) -> list[dict]:
        """Get recent signatures for the pump.fun program."""
        payload = {
            "jsonrpc": "2.0",
            "id":      1,
            "method":  "getSignaturesForAddress",
            "params":  [PUMP_PROGRAM, {"limit": SIG_LIMIT, "commitment": "confirmed"}],
        }
        try:
            async with session.post(
                self.rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
                return data.get("result", []) or []
        except Exception as e:
            logger.error(f"getSignaturesForAddress failed: {e}")
            return []

    async def _parse_migration_tx(
        self, sig: str, session: aiohttp.ClientSession
    ) -> Optional[str]:
        """
        Fetch a transaction and check if it's a pump.fun migration.
        Returns the token mint address if it is, else None.
        """
        payload = {
            "jsonrpc": "2.0",
            "id":      1,
            "method":  "getTransaction",
            "params":  [
                sig,
                {
                    "encoding":                       "jsonParsed",
                    "commitment":                     "confirmed",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
        }
        try:
            async with session.post(
                self.rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()

            tx = data.get("result")
            if not tx:
                return None

            # Check log messages for migration signature
            logs: list[str] = tx.get("meta", {}).get("logMessages") or []
            is_migration = any(
                "migrate" in log.lower() or "MigrateEvent" in log
                for log in logs
            )
            if not is_migration:
                return None

            # Also confirm PumpSwap program was invoked (real migration)
            invoked = any(PUMPSWAP_PROGRAM in log for log in logs)
            if not invoked:
                return None

            # Extract mint from account keys — the token mint is typically
            # the first non-SOL/non-program account
            accounts = (
                tx.get("transaction", {})
                  .get("message", {})
                  .get("accountKeys", [])
            )
            mint = self._extract_mint_from_accounts(accounts, logs)
            return mint

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug(f"parse_migration_tx error ({sig[:8]}): {e}")
            return None

    def _extract_mint_from_accounts(
        self, accounts: list, logs: list[str]
    ) -> Optional[str]:
        """
        Try to extract the token mint from transaction accounts.
        Mints on Solana are 44-char base58 strings ending in 'pump' for pump.fun tokens.
        """
        for acc in accounts:
            # jsonParsed format wraps keys in objects
            if isinstance(acc, dict):
                pubkey = acc.get("pubkey", "")
            else:
                pubkey = str(acc)

            if pubkey.endswith("pump") and len(pubkey) >= 32:
                return pubkey

        # Fallback: scan logs for a mint-like string
        import re
        for log in logs:
            matches = re.findall(r'[1-9A-HJ-NP-Za-km-z]{43,44}pump', log)
            if matches:
                return matches[0]

        return None

    async def _build_token(
        self, mint: str, session: aiohttp.ClientSession
    ) -> Optional[TrackedToken]:
        """
        Build a TrackedToken for a freshly migrated token.
        Fetches current price/mcap from Dexscreener to set migration price.
        """
        # Give Dexscreener a moment to index the new pair
        pair = await get_pumpswap_pair(mint, session)
        if not pair:
            logger.debug(f"No Dexscreener pair yet for {mint[:8]}, will retry next cycle")
            return None

        data = extract_price_data(pair)
        price = data.get("price_usd", 0)
        mcap  = data.get("mcap", 0)

        if price <= 0:
            return None
        
        # Pump.fun bonding curve completes at ~85 SOL
        # Dynamic floor scales with SOL price
        sol_price = await get_sol_price(session)
        min_migration_mcap = sol_price * 420
        if mcap < min_migration_mcap:
            logger.debug(
                f"Skipping {mint[:8]} — mcap ${mcap:,.0f} below migration floor ${min_migration_mcap:,.0f}"
            )
            return None

        token = TrackedToken(
            address          = mint,
            symbol           = data.get("symbol", "???"),
            pool_address     = data.get("pair_address", ""),
            status           = TokenStatus.TRACKING,
            migration_price  = price,
            migration_mcap   = mcap,
            current_price    = price,
            current_mcap     = mcap,
            liquidity_usd    = data.get("liquidity_usd", 0),
            ath_price        = 0.0,   # will be seeded by price_tracker on first poll
            migration_time   = time.time(),
            volume_1h        = data.get("volume_1h", 0),
            volume_6h        = data.get("volume_6h", 0),
            volume_24h       = data.get("volume_24h", 0),
        )
        return token