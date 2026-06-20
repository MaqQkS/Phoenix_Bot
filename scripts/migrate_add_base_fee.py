"""
scripts/migrate_add_base_fee.py — Ante Phase 1 schema migration.

Adds:
  1. pumpswap_fees.base_fee             INTEGER (nullable, forward-only)
  2. pumpswap_fees.signature_count      INTEGER (nullable, forward-only)
  3. ante_log                           new shadow-log table + indexes
  4. ante_log.ante_n20_width_ratio      REAL (Phase 1.1, nullable)
  5. ante_log.ante_5m_width_ratio       REAL (Phase 1.1, nullable)

Safe to re-run — every step is guarded by existence checks. Does not touch
existing rows. Historical rows keep NULL for the two new columns. The
width-ratio columns are added via ALTER for DBs that already have an older
ante_log table; fresh DBs get the full schema directly via the CREATE.

Run:
    python scripts/migrate_add_base_fee.py
"""
import asyncio
import aiosqlite
import sys
from pathlib import Path

# Allow running from either repo root or scripts/
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from database import db_connect

DB_PATH = "data/bot.db"

ANTE_LOG_SQL = """
CREATE TABLE IF NOT EXISTS ante_log (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    token_address         TEXT NOT NULL,
    symbol                TEXT,
    alert_tier            INTEGER,
    tier_name             TEXT,
    alert_time            REAL NOT NULL,
    ante_n20_count        INTEGER,
    ante_n20_median_sol   REAL,
    ante_n20_p25_sol      REAL,
    ante_n20_p75_sol      REAL,
    ante_n20_width_ratio  REAL,
    ante_5m_count         INTEGER,
    ante_5m_median_sol    REAL,
    ante_5m_p25_sol       REAL,
    ante_5m_p75_sol       REAL,
    ante_5m_width_ratio   REAL,
    base_fee_coverage     REAL,
    manual_verdict        TEXT,
    reviewed_at           REAL
);
"""

ANTE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_ante_token ON ante_log(token_address);",
    "CREATE INDEX IF NOT EXISTS idx_ante_time  ON ante_log(alert_time);",
    "CREATE INDEX IF NOT EXISTS idx_ante_tier  ON ante_log(alert_tier);",
]


async def main(db_path: str = DB_PATH):
    async with db_connect(db_path) as db:
        # ── pumpswap_fees column additions ───────────────────────────────
        async with db.execute("PRAGMA table_info(pumpswap_fees)") as cur:
            cols = {r[1] for r in await cur.fetchall()}

        if "base_fee" in cols:
            print("[=] pumpswap_fees.base_fee already present — skipped")
        else:
            await db.execute("ALTER TABLE pumpswap_fees ADD COLUMN base_fee INTEGER")
            await db.commit()
            print("[+] pumpswap_fees.base_fee added (nullable)")

        if "signature_count" in cols:
            print("[=] pumpswap_fees.signature_count already present — skipped")
        else:
            await db.execute("ALTER TABLE pumpswap_fees ADD COLUMN signature_count INTEGER")
            await db.commit()
            print("[+] pumpswap_fees.signature_count added (nullable)")

        # ── ante_log table + indexes ─────────────────────────────────────
        async with db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ante_log'"
        ) as cur:
            existed = await cur.fetchone() is not None

        await db.execute(ANTE_LOG_SQL)
        for idx_sql in ANTE_INDEX_SQL:
            await db.execute(idx_sql)
        await db.commit()

        if existed:
            print("[=] ante_log already existed — schema not modified by CREATE")
        else:
            print("[+] ante_log table + 3 indexes created")

        # Phase 1.1: add width-ratio columns to ante_log if missing.
        # Fresh CREATE includes them; older DBs need ALTER.
        async with db.execute("PRAGMA table_info(ante_log)") as cur:
            ante_cols = {r[1] for r in await cur.fetchall()}
        if "ante_n20_width_ratio" in ante_cols:
            print("[=] ante_log.ante_n20_width_ratio already present — skipped")
        else:
            await db.execute("ALTER TABLE ante_log ADD COLUMN ante_n20_width_ratio REAL")
            await db.commit()
            print("[+] ante_log.ante_n20_width_ratio added (nullable)")
        if "ante_5m_width_ratio" in ante_cols:
            print("[=] ante_log.ante_5m_width_ratio already present — skipped")
        else:
            await db.execute("ALTER TABLE ante_log ADD COLUMN ante_5m_width_ratio REAL")
            await db.commit()
            print("[+] ante_log.ante_5m_width_ratio added (nullable)")

        # ── Report final state ───────────────────────────────────────────
        async with db.execute("PRAGMA table_info(ante_log)") as cur:
            ante_cols = await cur.fetchall()
        async with db.execute("PRAGMA index_list(ante_log)") as cur:
            ante_idxs = await cur.fetchall()
        async with db.execute("PRAGMA table_info(pumpswap_fees)") as cur:
            fees_cols = [r[1] for r in await cur.fetchall()]

    print()
    print(f"ante_log columns ({len(ante_cols)}):")
    for col in ante_cols:
        nullable = "" if col[3] else " NULL"
        default = f" DEFAULT {col[4]}" if col[4] is not None else ""
        print(f"  {col[1]:21s} {col[2]:10s}{nullable}{default}")

    print(f"\nante_log indexes ({len(ante_idxs)}):")
    for idx in ante_idxs:
        print(f"  {idx[1]}")

    print(f"\npumpswap_fees columns now ({len(fees_cols)}):")
    print("  " + ", ".join(fees_cols))


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    asyncio.run(main(db))
