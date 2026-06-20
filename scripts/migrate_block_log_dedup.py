"""
scripts/migrate_block_log_dedup.py — alert_block_log dedup migration.

Makes alert_block_log idempotent on (token_address, would_have_tier,
block_reason). Transient blocks like no_fee_data used to write a new row
per retry tick, spamming the log (see the $Bro incident: 11 rows for one
token in 5.5 minutes). After this migration, the writer UPSERTs — first
block creates a row, retries bump retry_count and last_retry_at.

What this script does, in order:
  1. Idempotency guard: if columns + unique index already exist AND no
     duplicate (token, tier, reason) groups remain, exit 0.
  2. Snapshot pre-migration row count.
  3. ALTER TABLE to add retry_count (DEFAULT 1) and last_retry_at (REAL).
     Skipped if columns already exist (partial prior run).
  4. Backfill last_retry_at = block_time for any NULL rows.
  5. Collapse duplicate groups: for each (token, tier, reason) group with
     >1 rows, keep the MIN(id) row, set its retry_count = group COUNT(*)
     and last_retry_at = MAX(block_time), and DELETE the rest.
  6. Sanity check: SUM(retry_count) after collapse must equal the
     pre-migration row count. If mismatched, abort BEFORE creating the
     unique index — the schema stays recoverable.
  7. CREATE UNIQUE INDEX idx_alert_block_dedup
         ON alert_block_log(token_address, would_have_tier, block_reason)

=============================================================================
⚠️  WARNING — BOT MUST BE STOPPED BEFORE RUNNING THIS SCRIPT
=============================================================================
This is a one-shot sync sqlite3 migration. ALTER TABLE takes a schema-level
lock that will fail (not wait) against a running writer on a WAL database.
Stop the bot (Ctrl+C on main.py) first.

Run:
    python scripts/migrate_block_log_dedup.py
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = "data/bot.db"
INDEX_NAME = "idx_alert_block_dedup"


def column_exists(cur: sqlite3.Cursor, table: str, column: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def index_exists(cur: sqlite3.Cursor, index: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
        (index,),
    )
    return cur.fetchone() is not None


def find_duplicate_groups(cur: sqlite3.Cursor) -> list[tuple]:
    cur.execute(
        """
        SELECT token_address, would_have_tier, block_reason, COUNT(*)
        FROM alert_block_log
        GROUP BY token_address, would_have_tier, block_reason
        HAVING COUNT(*) > 1
        """
    )
    return cur.fetchall()


def abort(conn: sqlite3.Connection, msg: str) -> None:
    conn.rollback()
    conn.close()
    print()
    print("=" * 70)
    print(f"[!] MIGRATION ABORTED — {msg}")
    print("=" * 70)
    sys.exit(1)


def main(db_path: str = DB_PATH) -> None:
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"[!] DB not found at {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # ── Step 1: idempotency guard ────────────────────────────────────────
    has_retry_count = column_exists(cur, "alert_block_log", "retry_count")
    has_last_retry = column_exists(cur, "alert_block_log", "last_retry_at")
    has_index = index_exists(cur, INDEX_NAME)
    dup_groups = find_duplicate_groups(cur)

    if has_retry_count and has_last_retry and has_index and not dup_groups:
        conn.close()
        print("✅ Already migrated — columns, index, and dedup state all in place.")
        print("   Nothing to do.")
        return

    # ── Step 2: pre-migration snapshot ───────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM alert_block_log")
    rows_before = cur.fetchone()[0]
    print(f"alert_block_log rows BEFORE migration: {rows_before}")
    print(f"duplicate (token, tier, reason) groups: {len(dup_groups)}")
    if dup_groups:
        total_dup_rows = sum(g[3] for g in dup_groups)
        print(f"  → {total_dup_rows} rows across {len(dup_groups)} groups will collapse")

    try:
        # ── Step 3: ALTER TABLE (guarded for re-runs) ─────────────────────
        if not has_retry_count:
            cur.execute(
                "ALTER TABLE alert_block_log "
                "ADD COLUMN retry_count INTEGER DEFAULT 1"
            )
            print("  ✓ added column: retry_count INTEGER DEFAULT 1")
        else:
            print("  · retry_count column already present — skipping ALTER")

        if not has_last_retry:
            cur.execute(
                "ALTER TABLE alert_block_log ADD COLUMN last_retry_at REAL"
            )
            print("  ✓ added column: last_retry_at REAL")
        else:
            print("  · last_retry_at column already present — skipping ALTER")

        # ── Step 4: backfill last_retry_at ────────────────────────────────
        cur.execute(
            "UPDATE alert_block_log "
            "SET last_retry_at = block_time "
            "WHERE last_retry_at IS NULL"
        )
        backfilled = cur.rowcount
        if backfilled > 0:
            print(f"  ✓ backfilled last_retry_at on {backfilled} row(s)")

        # ── Step 5: collapse duplicate groups ─────────────────────────────
        # Re-query duplicates now that columns exist so we can write them
        # in a single UPDATE.
        dup_groups = find_duplicate_groups(cur)
        collapsed_rows_deleted = 0
        for token_address, would_have_tier, block_reason, count in dup_groups:
            cur.execute(
                """
                SELECT MIN(id), MAX(block_time)
                FROM alert_block_log
                WHERE token_address = ?
                  AND would_have_tier = ?
                  AND block_reason = ?
                """,
                (token_address, would_have_tier, block_reason),
            )
            keeper_id, max_block_time = cur.fetchone()

            cur.execute(
                """
                UPDATE alert_block_log
                SET retry_count = ?, last_retry_at = ?
                WHERE id = ?
                """,
                (count, max_block_time, keeper_id),
            )

            cur.execute(
                """
                DELETE FROM alert_block_log
                WHERE token_address = ?
                  AND would_have_tier = ?
                  AND block_reason = ?
                  AND id != ?
                """,
                (token_address, would_have_tier, block_reason, keeper_id),
            )
            collapsed_rows_deleted += cur.rowcount
            print(
                f"  ✓ collapsed ({token_address[:16]}…, tier={would_have_tier}, "
                f"{block_reason}) — kept id={keeper_id}, retry_count={count}, "
                f"deleted {cur.rowcount} duplicate row(s)"
            )

        # ── Step 6: sanity check — SUM(retry_count) == rows_before ────────
        cur.execute("SELECT COALESCE(SUM(retry_count), 0) FROM alert_block_log")
        sum_retry_count = cur.fetchone()[0]

        if sum_retry_count != rows_before:
            abort(
                conn,
                f"SUM(retry_count)={sum_retry_count} != rows_before={rows_before}. "
                f"Collapse would have lost or duplicated attempts — NOT creating "
                f"UNIQUE INDEX. Transaction will be rolled back.",
            )

        print(
            f"  ✓ sanity check: SUM(retry_count)={sum_retry_count} "
            f"== pre-migration rows={rows_before}"
        )

        # Verify the dup groups are actually gone before indexing.
        remaining_dups = find_duplicate_groups(cur)
        if remaining_dups:
            abort(
                conn,
                f"{len(remaining_dups)} duplicate group(s) still present after "
                f"collapse. UNIQUE INDEX would fail. Transaction rolled back.",
            )

        # ── Step 7: CREATE UNIQUE INDEX ───────────────────────────────────
        cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {INDEX_NAME} "
            f"ON alert_block_log(token_address, would_have_tier, block_reason)"
        )
        print(f"  ✓ created unique index: {INDEX_NAME}")

        conn.commit()

    except sqlite3.IntegrityError as e:
        conn.rollback()
        print()
        print("=" * 70)
        print("[!] MIGRATION FAILED — integrity error")
        print("=" * 70)
        print(f"sqlite3.IntegrityError: {e}")
        print()
        print("Diagnostic query to find duplicates:")
        print("  SELECT token_address, would_have_tier, block_reason, COUNT(*)")
        print("  FROM alert_block_log")
        print("  GROUP BY token_address, would_have_tier, block_reason")
        print("  HAVING COUNT(*) > 1;")
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
    cur.execute("SELECT COUNT(*) FROM alert_block_log")
    rows_after = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(retry_count), 0) FROM alert_block_log")
    final_sum = cur.fetchone()[0]
    conn.close()

    print()
    print("=" * 70)
    print("✅ MIGRATION COMPLETE")
    print("=" * 70)
    print(f"  rows before:           {rows_before}")
    print(f"  rows after:            {rows_after}")
    print(f"  SUM(retry_count):      {final_sum}  (must equal rows_before)")
    print(f"  rows collapsed:        {rows_before - rows_after}")
    print(f"  unique index:          {INDEX_NAME}")
    print()
    print("Verify:")
    print("    sqlite3 data/bot.db \"SELECT symbol, would_have_tier, block_reason, \\")
    print("      retry_count, datetime(block_time,'unixepoch') as first_seen, \\")
    print("      datetime(last_retry_at,'unixepoch') as last_retry \\")
    print("      FROM alert_block_log WHERE retry_count > 1;\"")
    print()
    print("Next step: restart the bot")
    print("    python main.py")
    print()


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    main(db)
