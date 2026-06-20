"""
scripts/migrate_fees_index.py — UNIQUE index recomposition on pumpswap_fees.

Drops the old single-column unique index on pumpswap_fees(signature) and
re-creates it as a composite unique index on (signature, event_type).

Why: multi-event transactions (Buy + Sell in the same tx share one signature)
were collapsing to a single row under INSERT OR IGNORE — losing one event per
such tx. Composite key preserves both events while still preventing true
duplicates from the indexer's at-least-once delivery.

=============================================================================
⚠️  WARNING — BOT MUST BE STOPPED BEFORE RUNNING THIS SCRIPT
=============================================================================
This is a one-shot sync sqlite3 migration. It will FAIL if the DB is locked
by a running writer (WAL lock). Stop the bot (Ctrl+C on main.py) first.

Run:
    python scripts/migrate_fees_index.py
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = "data/bot.db"


def main(db_path: str = DB_PATH):
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"[!] DB not found at {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # ── Pre-migration state ──────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM pumpswap_fees")
    rows_before = cur.fetchone()[0]
    print(f"pumpswap_fees rows BEFORE migration: {rows_before}")

    # ── DROP + CREATE ────────────────────────────────────────────────────
    try:
        cur.execute("DROP INDEX IF EXISTS idx_pumpswap_fees_signature")
        cur.execute(
            "CREATE UNIQUE INDEX idx_pumpswap_fees_signature "
            "ON pumpswap_fees(signature, event_type)"
        )
        conn.commit()
    except sqlite3.IntegrityError as e:
        # Would happen only if existing rows already have duplicate
        # (signature, event_type) pairs — shouldn't happen since old index
        # enforced uniqueness on signature alone, but guard anyway.
        conn.rollback()
        print()
        print("=" * 70)
        print("[!] MIGRATION FAILED — duplicate (signature, event_type) rows exist")
        print("=" * 70)
        print(f"sqlite3.IntegrityError: {e}")
        print()
        print("Diagnostic query to find duplicates:")
        print("  SELECT signature, event_type, COUNT(*) FROM pumpswap_fees")
        print("  GROUP BY signature, event_type HAVING COUNT(*) > 1;")
        print()
        print("The old index has been dropped but the new one was NOT created.")
        print("Resolve duplicates, then re-run this script.")
        conn.close()
        sys.exit(1)
    except sqlite3.OperationalError as e:
        conn.rollback()
        print()
        print("=" * 70)
        print("[!] MIGRATION FAILED — operational error")
        print("=" * 70)
        print(f"sqlite3.OperationalError: {e}")
        print()
        print("If this says 'database is locked', the bot is still running.")
        print("Stop the bot (Ctrl+C on main.py) and try again.")
        conn.close()
        sys.exit(1)

    # ── Post-migration verification ──────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM pumpswap_fees")
    rows_after = cur.fetchone()[0]
    print(f"pumpswap_fees rows AFTER migration:  {rows_after}")

    if rows_before != rows_after:
        print()
        print("=" * 70)
        print(f"[!] ROW COUNT MISMATCH: before={rows_before} after={rows_after}")
        print("=" * 70)
        conn.close()
        sys.exit(1)

    conn.close()

    print()
    print("=" * 70)
    print("✅ MIGRATION COMPLETE")
    print("=" * 70)
    print("idx_pumpswap_fees_signature is now UNIQUE on (signature, event_type)")
    print(f"Row count preserved: {rows_after} rows")
    print()
    print("Next step: restart the bot")
    print("    python main.py")
    print()


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    main(db)
