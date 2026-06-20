import asyncio
import aiosqlite
import sys
from pathlib import Path

# Allow running from either repo root or scripts/
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from database import db_connect

DB_PATH = "data/bot.db"

MIGRATIONS = [
    "ALTER TABLE ante_log ADD COLUMN label_5m TEXT",
    "ALTER TABLE ante_log ADD COLUMN rule_hit_5m INTEGER",
    "ALTER TABLE ante_log ADD COLUMN label_20sw TEXT",
    "ALTER TABLE ante_log ADD COLUMN rule_hit_20sw INTEGER",
]

async def main():
    async with db_connect(DB_PATH) as db:
        for sql in MIGRATIONS:
            try:
                await db.execute(sql)
                print(f"OK: {sql}")
            except Exception as e:
                print(f"SKIP ({e}): {sql}")
        await db.commit()

if __name__ == "__main__":
    asyncio.run(main())
