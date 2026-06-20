"""
scripts/migrate_add_ath_source.py

One-shot schema migration: add tokens.ath_source column with backfill.

Runs the same ALTER TABLE + backfill logic that database.init_db performs
on startup, but as a standalone script so Maq can migrate the live DB
with the bot stopped, verify the result, and then restart.

Usage:
    # Stop the bot first.
    python scripts/migrate_add_ath_source.py [path/to/bot.db]

If no path is given, defaults to data/bot.db.

Safe to re-run: the ALTER TABLE is guarded on PRAGMA table_info; if
ath_source already exists, the script exits without changes.

Backfill policy (matches database.init_db):
    ath_price > 0                        → ath_source = 'running_max'
    ath_price IS NULL OR ath_price <= 0  → ath_source = 'unseeded'

'running_max' is the safe default for pre-migration rows — their exact
provenance is unknowable, and marking them 'running_max' keeps them OUT
of the Birdeye retry queue on first boot after deploy (avoids a retry
storm against thousands of historical rows).
"""

import sqlite3
import sys
from pathlib import Path

DEFAULT_DB_PATH = "data/bot.db"


def migrate(db_path: str) -> None:
    p = Path(db_path)
    if not p.exists():
        print(f"ERROR: database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Opening {db_path}")
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        # Pre-check: total rows (for post-migration row-count verification)
        cur.execute("SELECT COUNT(*) FROM tokens")
        pre_count = cur.fetchone()[0]
        print(f"  tokens row count before: {pre_count}")

        # Guarded ALTER TABLE
        cur.execute("PRAGMA table_info(tokens)")
        cols = [row[1] for row in cur.fetchall()]
        if "ath_source" in cols:
            print("  ath_source column already present — no changes needed")
            return

        print("  adding ath_source column...")
        cur.execute("ALTER TABLE tokens ADD COLUMN ath_source TEXT DEFAULT 'unseeded'")

        print("  backfilling ath_source = 'running_max' for rows with ath_price > 0...")
        cur.execute(
            "UPDATE tokens SET ath_source = 'running_max' WHERE ath_price > 0"
        )
        running_max_updated = cur.rowcount

        print("  backfilling ath_source = 'unseeded' for rows with ath_price <= 0 / NULL...")
        cur.execute(
            "UPDATE tokens SET ath_source = 'unseeded' "
            "WHERE ath_price IS NULL OR ath_price <= 0"
        )
        unseeded_updated = cur.rowcount

        conn.commit()

        # Verification: row count unchanged
        cur.execute("SELECT COUNT(*) FROM tokens")
        post_count = cur.fetchone()[0]
        if post_count != pre_count:
            print(
                f"  WARNING: row count changed ({pre_count} → {post_count})",
                file=sys.stderr,
            )

        # Verification: source distribution
        cur.execute(
            "SELECT ath_source, COUNT(*) FROM tokens GROUP BY ath_source ORDER BY ath_source"
        )
        dist = cur.fetchall()

        print()
        print("  migration complete")
        print(f"    row count before → after : {pre_count} → {post_count}")
        print(f"    backfilled 'running_max' : {running_max_updated}")
        print(f"    backfilled 'unseeded'    : {unseeded_updated}")
        print("    ath_source distribution  :")
        for src, cnt in dist:
            print(f"      {src!r:14s} {cnt}")

    finally:
        conn.close()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH
    migrate(path)
