"""One-shot migration: adds quote_amount column to pumpswap_fees."""
import asyncio, aiosqlite
import sys
from pathlib import Path

# Allow running from either repo root or scripts/
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from database import db_connect

DB_PATH = "data/bot.db"

async def main():
    async with db_connect(DB_PATH) as db:
        cur = await db.execute("PRAGMA table_info(pumpswap_fees)")
        cols = [r[1] for r in await cur.fetchall()]
        if "quote_amount" in cols:
            print("quote_amount column already exists, skipping")
            return
        await db.execute("ALTER TABLE pumpswap_fees ADD COLUMN quote_amount INTEGER DEFAULT 0")
        await db.commit()
        print("✓ added quote_amount column")

if __name__ == "__main__":
    asyncio.run(main())