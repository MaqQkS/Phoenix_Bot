"""
database.py — SQLite persistence via aiosqlite.
Stores and retrieves TrackedToken objects.
"""

import aiosqlite
import json
import logging
import os
import time

from models import TrackedToken, TokenStatus

logger = logging.getLogger(__name__)

DB_PATH = "data/bot.db"


async def init_db(db_path: str = DB_PATH):
    """Create tables if they don't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                address         TEXT PRIMARY KEY,
                symbol          TEXT,
                pool_address    TEXT,
                status          TEXT,
                migration_price REAL,
                migration_mcap  REAL,
                current_price   REAL,
                current_mcap    REAL,
                liquidity_usd   REAL,
                ath_price       REAL,
                ath_mcap        REAL,
                ath_time        REAL,
                volume_1h       REAL,
                volume_6h       REAL,
                volume_24h      REAL,
                migration_time  REAL,
                last_price_update REAL,
                last_alerted_tier INTEGER
            )
        """)
        await db.commit()
    logger.info(f"Database initialised at {db_path}")


async def save_token(token: TrackedToken, db_path: str = DB_PATH):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            INSERT OR REPLACE INTO tokens VALUES (
                :address, :symbol, :pool_address, :status,
                :migration_price, :migration_mcap,
                :current_price, :current_mcap, :liquidity_usd,
                :ath_price, :ath_mcap, :ath_time,
                :volume_1h, :volume_6h, :volume_24h,
                :migration_time, :last_price_update,
                :last_alerted_tier
            )
        """, {
            "address":           token.address,
            "symbol":            token.symbol,
            "pool_address":      token.pool_address,
            "status":            token.status.value,
            "migration_price":   token.migration_price,
            "migration_mcap":    token.migration_mcap,
            "current_price":     token.current_price,
            "current_mcap":      token.current_mcap,
            "liquidity_usd":     token.liquidity_usd,
            "ath_price":         token.ath_price,
            "ath_mcap":          token.ath_mcap,
            "ath_time":          token.ath_time,
            "volume_1h":         token.volume_1h,
            "volume_6h":         token.volume_6h,
            "volume_24h":        token.volume_24h,
            "migration_time":    token.migration_time,
            "last_price_update": token.last_price_update,
            "last_alerted_tier": token.last_alerted_tier,
        })
        await db.commit()


async def load_all_tokens(db_path: str = DB_PATH) -> list[TrackedToken]:
    """Load all non-expired tokens from the database."""
    if not os.path.exists(db_path):
        return []
    tokens = []
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM tokens WHERE status != 'expired'"
        ) as cursor:
            async for row in cursor:
                tokens.append(_row_to_token(row))
    return tokens


async def get_token(address: str, db_path: str = DB_PATH) -> TrackedToken | None:
    if not os.path.exists(db_path):
        return None
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM tokens WHERE address = ?", (address,)
        ) as cursor:
            row = await cursor.fetchone()
            return _row_to_token(row) if row else None


async def token_exists(address: str, db_path: str = DB_PATH) -> bool:
    return await get_token(address, db_path) is not None


def _row_to_token(row) -> TrackedToken:
    return TrackedToken(
        address           = row["address"],
        symbol            = row["symbol"] or "???",
        pool_address      = row["pool_address"] or "",
        status            = TokenStatus(row["status"]),
        migration_price   = row["migration_price"] or 0.0,
        migration_mcap    = row["migration_mcap"] or 0.0,
        current_price     = row["current_price"] or 0.0,
        current_mcap      = row["current_mcap"] or 0.0,
        liquidity_usd     = row["liquidity_usd"] or 0.0,
        ath_price         = row["ath_price"] or 0.0,
        ath_mcap          = row["ath_mcap"] or 0.0,
        ath_time          = row["ath_time"] or 0.0,
        volume_1h         = row["volume_1h"] or 0.0,
        volume_6h         = row["volume_6h"] or 0.0,
        volume_24h        = row["volume_24h"] or 0.0,
        migration_time    = row["migration_time"] or 0.0,
        last_price_update = row["last_price_update"] or 0.0,
        last_alerted_tier = row["last_alerted_tier"] if row["last_alerted_tier"] is not None else -1,
    )