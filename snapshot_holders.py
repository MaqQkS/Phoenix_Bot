"""
snapshot_holders.py — Snapshot top token holders at Tier 1 alert time.

At the moment a Tier 1 ping fires, snapshots the top 50 owner wallets of
a Solana token mint with native SOL balances and classification, for
downstream scam-vs-organic research.

Architecture:
  - Holder data:  Helius DAS (primary), getTokenLargestAccounts fallback.
  - SOL balances: separate Solana RPC via getMultipleAccounts (batch 100).
  - Modular providers via HolderProvider / SolBalanceProvider base classes.

Shadow mode — never blocks alerts. Persists to SQLite + JSON.
"""

import asyncio
import base64
import json
import logging
import os
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Optional

import aiohttp
import aiosqlite
import yaml

from database import db_connect

logger = logging.getLogger("phoenix.snapshot_holders")

# ── Constants ────────────────────────────────────────────────────────────────

LAMPORTS_PER_SOL = 1_000_000_000
CONFIG_PATH = "config.yaml"
DB_PATH = "data/bot.db"
SNAPSHOT_DIR = "data/holder_snapshots"
RPC_TIMEOUT = aiohttp.ClientTimeout(total=10)
BALANCE_BATCH_SIZE = 100
MAX_RETRIES = 3
RETRY_BASE_S = 0.5

SYSTEM_PROGRAM = "11111111111111111111111111111111"
SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

NON_USER_OWNER_PROGRAMS: set[str] = {
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",   # PumpSwap
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", # Raydium AMM v4
    "CAMMCzo5YL8w4VFF8KVHr7wifgkKNsBo3TyqxjMYYSbp", # Raydium CLMM
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", # Raydium CP-AMM
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",   # Orca Whirlpools
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",   # Meteora DLMM v1
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", # Meteora Pools v2
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",   # Metaplex
    SPL_TOKEN_PROGRAM,
    SPL_TOKEN_2022,
}


# ── Inline base58 (no external dep) ─────────────────────────────────────────

_B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(data: bytes) -> str:
    """Encode bytes to base58 (Solana pubkey format)."""
    n = int.from_bytes(data, "big")
    chars: list[int] = []
    while n > 0:
        n, r = divmod(n, 58)
        chars.append(_B58_ALPHABET[r])
    # Preserve leading zero bytes as '1'
    for b in data:
        if b == 0:
            chars.append(ord("1"))
        else:
            break
    return bytes(reversed(chars)).decode("ascii")


# ── Dataclasses ──────────────────────────────────────────────────────────────


@dataclass
class HolderRow:
    """Single holder row within a snapshot."""
    token_mint: str
    ping_time: float
    tier: str
    rank: int
    owner_wallet: str
    token_account: str
    token_amount: float
    token_percent_of_supply: Optional[float]
    sol_balance: Optional[float]
    holder_type: str                # user_wallet | lp_pool | vault | program_owned | unknown
    exclude_from_wallet_stats: bool
    source_used: str                # helius_das | helius_rpc_fallback
    balance_source: str             # solana_rpc
    fetch_slot: Optional[int]
    fetch_time: float
    status: str                     # ok | partial | error
    error: Optional[str]


@dataclass
class Snapshot:
    """Full snapshot result."""
    token_mint: str
    tier: str
    ping_time: float
    fetch_time: float
    fetch_slot: Optional[int]
    snapshot_status: str            # ok | partial | error
    alert_id: Optional[str]
    symbol: Optional[str]
    pool_address: Optional[str]
    holder_source: str
    balance_source: str
    total_supply: Optional[float] = None
    holders: list[HolderRow] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ── Abstract providers ───────────────────────────────────────────────────────


class HolderProvider(ABC):
    """Base class for holder data sources."""
    name: str = "base"

    @abstractmethod
    async def get_top_holders(
        self, token_mint: str, limit: int, session: aiohttp.ClientSession,
    ) -> tuple[list[dict], Optional[int]]:
        """
        Return (holders, slot).
        Each holder dict: {token_account, owner, amount_raw}.
        """
        ...


class SolBalanceProvider(ABC):
    """Base class for SOL balance + account-owner data."""
    name: str = "base"

    @abstractmethod
    async def get_balances_and_owners(
        self, wallets: list[str], session: aiohttp.ClientSession,
    ) -> tuple[dict[str, dict], Optional[int]]:
        """
        Return ({wallet: {lamports: int|None, owner_program: str|None}}, slot).
        """
        ...


# ── Retry helper ─────────────────────────────────────────────────────────────


async def _rpc_post(
    session: aiohttp.ClientSession,
    url: str,
    payload: dict,
    label: str = "rpc",
) -> dict:
    """POST JSON-RPC with exponential-backoff retries (3 attempts, 0.5 s base)."""
    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(url, json=payload, timeout=RPC_TIMEOUT) as resp:
                if resp.status == 429:
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history,
                        status=429, message="rate limited",
                    )
                body = await resp.json()
                if "error" in body:
                    raise ValueError(f"RPC error: {body['error']}")
                return body
        except (aiohttp.ClientError, ValueError, asyncio.TimeoutError) as e:
            last_err = e
            delay = RETRY_BASE_S * (2 ** attempt)
            logger.debug(
                f"{label} attempt {attempt + 1}/{MAX_RETRIES} failed: {e}, "
                f"retry in {delay}s"
            )
            await asyncio.sleep(delay)
    raise last_err or RuntimeError(f"{label} failed after {MAX_RETRIES} retries")


# ── Concrete: HeliusDASHolders ───────────────────────────────────────────────


class HeliusDASHolders(HolderProvider):
    """Primary holder provider — Helius DAS getTokenAccounts."""
    name = "helius_das"

    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url

    async def get_top_holders(
        self, token_mint: str, limit: int, session: aiohttp.ClientSession,
    ) -> tuple[list[dict], Optional[int]]:
        # Over-fetch so we can aggregate by owner and still have >= limit
        fetch_limit = max(limit * 3, 150)
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenAccounts",
            "params": {
                "mint": token_mint,
                "limit": fetch_limit,
                "options": {"showZeroBalance": False},
            },
        }
        data = await _rpc_post(
            session, self.rpc_url, payload, "DAS.getTokenAccounts",
        )
        accounts = data.get("result", {}).get("token_accounts", [])

        holders: list[dict] = []
        for acc in accounts:
            amount = int(acc.get("amount", 0) or 0)
            if amount <= 0:
                continue
            holders.append({
                "token_account": acc.get("address", ""),
                "owner": acc.get("owner", ""),
                "amount_raw": amount,
            })

        holders.sort(key=lambda h: h["amount_raw"], reverse=True)
        return holders[:limit], None  # DAS doesn't surface slot


# ── Concrete: HeliusRPCFallbackHolders ───────────────────────────────────────


class HeliusRPCFallbackHolders(HolderProvider):
    """Fallback — getTokenLargestAccounts + SPL owner resolve (max ~20)."""
    name = "helius_rpc_fallback"

    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url

    async def get_top_holders(
        self, token_mint: str, limit: int, session: aiohttp.ClientSession,
    ) -> tuple[list[dict], Optional[int]]:
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenLargestAccounts",
            "params": [token_mint],
        }
        data = await _rpc_post(
            session, self.rpc_url, payload, "getTokenLargestAccounts",
        )
        result = data.get("result", {})
        slot = result.get("context", {}).get("slot")
        accounts = result.get("value", [])

        if not accounts:
            return [], slot

        # Resolve token-account address -> owner via SPL layout
        addresses = [a["address"] for a in accounts]
        owners = await self._resolve_owners(addresses, session)

        holders: list[dict] = []
        for acc in accounts:
            addr = acc["address"]
            amount = int(acc.get("amount", "0") or "0")
            owner = owners.get(addr, "")
            if amount <= 0 or not owner:
                continue
            holders.append({
                "token_account": addr,
                "owner": owner,
                "amount_raw": amount,
            })

        holders.sort(key=lambda h: h["amount_raw"], reverse=True)
        return holders[:limit], slot

    async def _resolve_owners(
        self, token_accounts: list[str], session: aiohttp.ClientSession,
    ) -> dict[str, str]:
        """Parse SPL token account layout (offset 32..64) to extract owner."""
        owners: dict[str, str] = {}
        for i in range(0, len(token_accounts), BALANCE_BATCH_SIZE):
            batch = token_accounts[i : i + BALANCE_BATCH_SIZE]
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getMultipleAccounts",
                "params": [
                    batch,
                    {"encoding": "base64", "commitment": "confirmed"},
                ],
            }
            try:
                data = await _rpc_post(
                    session, self.rpc_url, payload, "resolveOwners",
                )
                values = data.get("result", {}).get("value", [])
                for addr, val in zip(batch, values):
                    if val is None:
                        continue
                    raw = base64.b64decode(val["data"][0])
                    if len(raw) >= 64:
                        owners[addr] = _b58encode(raw[32:64])
            except Exception as e:
                logger.warning(f"Owner resolve batch failed: {e}")
        return owners


# ── Concrete: SolanaRPCBalances ──────────────────────────────────────────────


class SolanaRPCBalances(SolBalanceProvider):
    """SOL balance + owner-program via standard Solana RPC (NOT Helius)."""
    name = "solana_rpc"

    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url

    async def get_balances_and_owners(
        self, wallets: list[str], session: aiohttp.ClientSession,
    ) -> tuple[dict[str, dict], Optional[int]]:
        result: dict[str, dict] = {}
        slot: int | None = None

        for i in range(0, len(wallets), BALANCE_BATCH_SIZE):
            batch = wallets[i : i + BALANCE_BATCH_SIZE]
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getMultipleAccounts",
                "params": [
                    batch,
                    {"encoding": "base64", "commitment": "confirmed"},
                ],
            }
            try:
                data = await _rpc_post(
                    session, self.rpc_url, payload, "getMultipleAccounts",
                )
                resp = data.get("result", {})
                if slot is None:
                    slot = resp.get("context", {}).get("slot")
                values = resp.get("value", [])
                for addr, val in zip(batch, values):
                    if val is None:
                        result[addr] = {
                            "lamports": None, "owner_program": None,
                        }
                    else:
                        result[addr] = {
                            "lamports": val.get("lamports"),
                            "owner_program": val.get("owner"),
                        }
            except Exception as e:
                logger.warning(f"Balance batch failed ({len(batch)} wallets): {e}")
                for addr in batch:
                    result.setdefault(
                        addr, {"lamports": None, "owner_program": None},
                    )

        return result, slot


# ── Classification ───────────────────────────────────────────────────────────


def _classify_holder(
    owner_wallet: str,
    owner_program: str | None,
    pool_address: str | None,
    known_vaults: set[str],
) -> tuple[str, bool]:
    """
    Classify a holder and decide whether to exclude from wallet-level stats.
    Returns (holder_type, exclude_from_wallet_stats).
    """
    if pool_address and owner_wallet == pool_address:
        return "lp_pool", True
    if owner_wallet in known_vaults:
        return "vault", True
    if owner_program == SYSTEM_PROGRAM:
        return "user_wallet", False
    if owner_program and owner_program in NON_USER_OWNER_PROGRAMS:
        return "program_owned", True
    # owner_program unknown or unrecognised — don't guess
    return "unknown", True


# ── Supply helper ────────────────────────────────────────────────────────────


async def _fetch_total_supply(
    token_mint: str,
    rpc_url: str,
    session: aiohttp.ClientSession,
) -> Optional[float]:
    """Fetch total supply via getTokenSupply RPC. Returns uiAmount (already decimal-adjusted)."""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenSupply",
        "params": [token_mint],
    }
    try:
        data = await _rpc_post(session, rpc_url, payload, "getTokenSupply")
        ui = data.get("result", {}).get("value", {}).get("uiAmount")
        if ui is not None and ui > 0:
            return float(ui)
        return None
    except Exception as e:
        logger.warning(f"getTokenSupply failed for {token_mint[:8]}: {e}")
        return None


# ── Config ───────────────────────────────────────────────────────────────────


def _load_config() -> tuple[str, str]:
    """
    Return (helius_rpc_url, solana_rpc_url).
    Env vars HELIUS_RPC_URL / SOLANA_RPC_URL take precedence over config.yaml.
    """
    cfg: dict = {}
    try:
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f) or {}
    except FileNotFoundError:
        pass

    helius_url = (
        os.environ.get("HELIUS_RPC_URL")
        or cfg.get("helius", {}).get("rpc_url", "")
    )
    solana_url = (
        os.environ.get("SOLANA_RPC_URL")
        or cfg.get("solana_rpc", {}).get("url", "")
    )

    if not helius_url:
        raise RuntimeError(
            "No Helius RPC URL (set HELIUS_RPC_URL env or helius.rpc_url in config.yaml)"
        )
    if not solana_url:
        solana_url = "https://api.mainnet-beta.solana.com"
        logger.info(
            "Using public Solana RPC for balances — "
            "set solana_rpc.url in config.yaml for production"
        )

    return helius_url, solana_url


# ── Persistence ──────────────────────────────────────────────────────────────


async def _ensure_table(db_conn: aiosqlite.Connection) -> None:
    """CREATE TABLE IF NOT EXISTS for holder_snapshots."""
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS holder_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            token_mint      TEXT    NOT NULL,
            tier            TEXT    NOT NULL,
            ping_time       REAL    NOT NULL,
            fetch_time      REAL    NOT NULL,
            fetch_slot      INTEGER,
            snapshot_status  TEXT    NOT NULL,
            alert_id        TEXT,
            symbol          TEXT,
            pool_address    TEXT,
            holder_source   TEXT,
            balance_source  TEXT,
            payload_json    TEXT    NOT NULL
        )
    """)
    await db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_hs_mint ON holder_snapshots(token_mint)"
    )
    await db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_hs_ping ON holder_snapshots(ping_time)"
    )
    await db_conn.commit()


async def _persist_snapshot(snapshot: Snapshot) -> None:
    """Write snapshot to SQLite and drop a JSON audit file."""
    payload = {
        "token_mint": snapshot.token_mint,
        "tier": snapshot.tier,
        "ping_time": snapshot.ping_time,
        "fetch_time": snapshot.fetch_time,
        "fetch_slot": snapshot.fetch_slot,
        "snapshot_status": snapshot.snapshot_status,
        "alert_id": snapshot.alert_id,
        "symbol": snapshot.symbol,
        "pool_address": snapshot.pool_address,
        "holder_source": snapshot.holder_source,
        "balance_source": snapshot.balance_source,
        "total_supply": snapshot.total_supply,
        "holders": [asdict(h) for h in snapshot.holders],
        "errors": snapshot.errors,
    }
    payload_json = json.dumps(payload, default=str)

    # ── SQLite ────────────────────────────────────────────────────────────
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        async with db_connect(DB_PATH) as db_conn:
            await _ensure_table(db_conn)
            await db_conn.execute(
                """
                INSERT INTO holder_snapshots (
                    token_mint, tier, ping_time, fetch_time, fetch_slot,
                    snapshot_status, alert_id, symbol, pool_address,
                    holder_source, balance_source, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.token_mint, snapshot.tier, snapshot.ping_time,
                    snapshot.fetch_time, snapshot.fetch_slot,
                    snapshot.snapshot_status, snapshot.alert_id,
                    snapshot.symbol, snapshot.pool_address,
                    snapshot.holder_source, snapshot.balance_source,
                    payload_json,
                ),
            )
            await db_conn.commit()
    except Exception as e:
        logger.error(f"Snapshot DB persist failed: {e}")

    # ── JSON audit file ──────────────────────────────────────────────────
    try:
        os.makedirs(SNAPSHOT_DIR, exist_ok=True)
        fname = f"{int(snapshot.ping_time)}_{snapshot.token_mint[:8]}.json"
        fpath = os.path.join(SNAPSHOT_DIR, fname)
        with open(fpath, "w") as f:
            json.dump(payload, f, indent=2, default=str)
        logger.info(f"Snapshot saved to {fpath}")
    except Exception as e:
        logger.error(f"Snapshot JSON persist failed: {e}")


# ── Main Entrypoint ──────────────────────────────────────────────────────────


async def snapshot_top_holders(
    token_mint: str,
    tier: str,
    ping_time: float,
    pool_address: Optional[str] = None,
    alert_id: Optional[str] = None,
    symbol: Optional[str] = None,
    name: Optional[str] = None,
    total_supply: Optional[float] = None,
    decimals: int = 6,
    extra_known_vaults: Optional[set[str]] = None,
    limit: int = 50,
    persist: bool = True,
    holder_provider: Optional[HolderProvider] = None,
    balance_provider: Optional[SolBalanceProvider] = None,
) -> dict:
    """
    Snapshot top holders of a Solana token at alert time.

    Returns a dict representation of the Snapshot (also persisted to
    SQLite + JSON when persist=True).
    """
    helius_url, solana_url = _load_config()
    known_vaults = extra_known_vaults or set()

    if holder_provider is None:
        holder_provider = HeliusDASHolders(helius_url)
    if balance_provider is None:
        balance_provider = SolanaRPCBalances(solana_url)
    fallback_provider = HeliusRPCFallbackHolders(helius_url)

    snapshot = Snapshot(
        token_mint=token_mint,
        tier=tier,
        ping_time=ping_time,
        fetch_time=0.0,
        fetch_slot=None,
        snapshot_status="ok",
        alert_id=alert_id,
        symbol=symbol,
        pool_address=pool_address,
        holder_source=holder_provider.name,
        balance_source=balance_provider.name,
    )

    try:
        async with aiohttp.ClientSession() as session:
            # ── Step 1: fetch holders ─────────────────────────────────────
            raw_holders: list[dict] = []
            holder_slot: int | None = None

            try:
                raw_holders, holder_slot = await holder_provider.get_top_holders(
                    token_mint, limit, session,
                )
                snapshot.holder_source = holder_provider.name
            except Exception as e:
                logger.warning(f"Primary holder provider failed: {e}")
                snapshot.errors.append(f"primary_holder_fail: {e}")
                try:
                    raw_holders, holder_slot = await fallback_provider.get_top_holders(
                        token_mint, limit, session,
                    )
                    snapshot.holder_source = fallback_provider.name
                except Exception as e2:
                    logger.error(f"Fallback holder provider also failed: {e2}")
                    snapshot.errors.append(f"fallback_holder_fail: {e2}")
                    snapshot.snapshot_status = "error"

            if not raw_holders and snapshot.snapshot_status != "error":
                snapshot.snapshot_status = "error"
                snapshot.errors.append("no_holders_found")

            # ── Step 2: aggregate by owner ────────────────────────────────
            owner_agg: dict[str, dict] = {}
            for h in raw_holders:
                owner = h["owner"]
                if owner in owner_agg:
                    owner_agg[owner]["amount_raw"] += h["amount_raw"]
                    # Keep largest token account as representative
                    if h["amount_raw"] > owner_agg[owner]["_max"]:
                        owner_agg[owner]["token_account"] = h["token_account"]
                        owner_agg[owner]["_max"] = h["amount_raw"]
                else:
                    owner_agg[owner] = {
                        "owner": owner,
                        "token_account": h["token_account"],
                        "amount_raw": h["amount_raw"],
                        "_max": h["amount_raw"],
                    }

            sorted_owners = sorted(
                owner_agg.values(),
                key=lambda x: x["amount_raw"],
                reverse=True,
            )[:limit]

            # ── Step 3: SOL balances + owner programs ─────────────────────
            unique_wallets = [o["owner"] for o in sorted_owners]
            balance_data: dict[str, dict] = {}
            balance_slot: int | None = None

            if unique_wallets and snapshot.snapshot_status != "error":
                try:
                    balance_data, balance_slot = (
                        await balance_provider.get_balances_and_owners(
                            unique_wallets, session,
                        )
                    )
                except Exception as e:
                    logger.warning(f"Balance fetch failed: {e}")
                    snapshot.errors.append(f"balance_fail: {e}")
                    if snapshot.snapshot_status == "ok":
                        snapshot.snapshot_status = "partial"

            snapshot.fetch_slot = balance_slot or holder_slot

            # ── Step 3b: fetch total supply if not provided ──────────────
            if total_supply is None and snapshot.snapshot_status != "error":
                total_supply = await _fetch_total_supply(
                    token_mint, solana_url, session,
                )
            snapshot.total_supply = total_supply

            # ── Step 4: build rows ────────────────────────────────────────
            divisor = 10 ** decimals
            fetch_time = time.time()
            snapshot.fetch_time = fetch_time

            for rank, entry in enumerate(sorted_owners, start=1):
                owner = entry["owner"]
                amount_ui = entry["amount_raw"] / divisor
                pct = (
                    (amount_ui / total_supply * 100)
                    if total_supply and total_supply > 0
                    else None
                )

                bal = balance_data.get(owner, {})
                lamports = bal.get("lamports")
                owner_program = bal.get("owner_program")
                sol_bal = (
                    lamports / LAMPORTS_PER_SOL
                    if lamports is not None
                    else None
                )

                h_type, exclude = _classify_holder(
                    owner, owner_program, pool_address, known_vaults,
                )

                row_status = "ok"
                row_error: str | None = None
                if lamports is None and snapshot.snapshot_status != "error":
                    row_status = "partial"
                    row_error = "sol_balance_unavailable"

                snapshot.holders.append(
                    HolderRow(
                        token_mint=token_mint,
                        ping_time=ping_time,
                        tier=tier,
                        rank=rank,
                        owner_wallet=owner,
                        token_account=entry["token_account"],
                        token_amount=amount_ui,
                        token_percent_of_supply=pct,
                        sol_balance=sol_bal,
                        holder_type=h_type,
                        exclude_from_wallet_stats=exclude,
                        source_used=snapshot.holder_source,
                        balance_source=snapshot.balance_source,
                        fetch_slot=snapshot.fetch_slot,
                        fetch_time=fetch_time,
                        status=row_status,
                        error=row_error,
                    )
                )

    except Exception as e:
        logger.exception(f"snapshot_top_holders unhandled error: {e}")
        snapshot.snapshot_status = "error"
        snapshot.errors.append(f"unhandled: {type(e).__name__}: {str(e)[:200]}")
        snapshot.fetch_time = time.time()

    # ── Step 5: persist ───────────────────────────────────────────────────
    if persist:
        await _persist_snapshot(snapshot)

    logger.info(
        f"Snapshot ${symbol or token_mint[:8]}: "
        f"{len(snapshot.holders)} holders, status={snapshot.snapshot_status}"
    )
    return asdict(snapshot)


# ── CLI Smoke Test ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python snapshot_holders.py <mint> [pool_address]")
        sys.exit(1)

    _mint = sys.argv[1]
    _pool = sys.argv[2] if len(sys.argv) > 2 else None

    async def _main():
        result = await snapshot_top_holders(
            token_mint=_mint,
            tier="CLI",
            ping_time=time.time(),
            pool_address=_pool,
        )
        print(json.dumps(result, indent=2, default=str))

    asyncio.run(_main())
