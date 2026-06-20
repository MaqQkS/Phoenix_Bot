"""
db_cleanup.py — Prune pumpswap_fees to control DB size.

Retention policy:
  1. Keep last 48h of ALL fee events (live alert window)
  2. Keep ALL fee events for tokens that ever hit an alert (performance tracking)
  3. Delete everything else (id-bounded batches, WAL checkpoint per batch)
  4. VACUUM to reclaim disk (default; --no-vacuum to skip)

Run from project root:
  python db_cleanup.py                       # dry run — shows what would delete
  python db_cleanup.py --execute             # delete + VACUUM (default)
  python db_cleanup.py --execute --no-vacuum # delete only, defer reclaim
  python db_cleanup.py --execute --force     # bypass the 2GB-WAL safety check

Delete phase is safe with the bot live (id-range deletes + per-batch checkpoint
keep the WAL bounded). VACUUM still wants the bot stopped — it takes an
exclusive lock and may time out otherwise.
"""

import os
import sqlite3
import sys
import time

DB_PATH = "data/bot.db"
WAL_PATH = DB_PATH + "-wal"
RETENTION_HOURS = 48
BATCH_SIZE = 100_000
WAL_BAD_STATE_BYTES = 2 * 1024 * 1024 * 1024   # 2 GB — refuse to run if WAL exceeds this
JOURNAL_SIZE_LIMIT = 1024 * 1024 * 1024        # 1 GB cap on WAL


def human_bytes(n):
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def main():
    execute   = "--execute" in sys.argv
    no_vacuum = "--no-vacuum" in sys.argv
    force     = "--force" in sys.argv

    print(f"\n{'='*70}")
    mode = "EXECUTE" if execute else "DRY RUN"
    if execute and no_vacuum:
        mode += " (no vacuum)"
    print(f"Phoenix Bot — pumpswap_fees cleanup")
    print(f"Mode: {mode}")
    print(f"{'='*70}\n")

    # Sanity: refuse to run on a blown-up WAL.
    # We hit this twice — cleanup ran, the subquery DELETE held long locks,
    # auto-checkpoint never fired, WAL ballooned past 100 GB. Refuse to make
    # it worse: operator must stop the bot, checkpoint, and re-run (or pass
    # --force if they really know what they're doing).
    if os.path.exists(WAL_PATH):
        wal_size = os.path.getsize(WAL_PATH)
        if wal_size > WAL_BAD_STATE_BYTES and not force:
            print(f"⚠️  WAL is {human_bytes(wal_size)} (limit {human_bytes(WAL_BAD_STATE_BYTES)}).")
            print(f"   Already in a bad state — running cleanup would make it worse.")
            print(f"   Stop the bot, run `PRAGMA wal_checkpoint(TRUNCATE)`, then re-run.")
            print(f"   To override (not recommended): --force")
            sys.exit(1)

    if not os.path.exists(DB_PATH):
        print(f"⚠️  No database at {DB_PATH}. Nothing to do.")
        return

    size_before = os.path.getsize(DB_PATH)
    print(f"DB size before: {human_bytes(size_before)}")

    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA journal_size_limit = {JOURNAL_SIZE_LIMIT}")
    cur = conn.cursor()

    now = time.time()
    cutoff = now - (RETENTION_HOURS * 3600)
    print(f"Cutoff: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cutoff))} "
          f"({RETENTION_HOURS}h ago)\n")

    # ── Step 1: Find alerted token addresses (keep forever) ──
    print("Finding alerted tokens (keep forever)...")
    alerted = cur.execute("SELECT DISTINCT address FROM alerts").fetchall()
    alerted_addrs = {row[0] for row in alerted}
    print(f"  → {len(alerted_addrs):,} alerted tokens protected\n")

    # ── Step 2: Count rows by category ──
    print("Analyzing pumpswap_fees...")
    total_rows = cur.execute("SELECT COUNT(*) FROM pumpswap_fees").fetchone()[0]
    print(f"  Total rows: {total_rows:,}")

    recent_rows = cur.execute(
        "SELECT COUNT(*) FROM pumpswap_fees WHERE received_at >= ?", (cutoff,)
    ).fetchone()[0]
    print(f"  Recent (<{RETENTION_HOURS}h):  {recent_rows:,} (KEEP)")

    old_rows = total_rows - recent_rows
    print(f"  Old (>{RETENTION_HOURS}h):     {old_rows:,}")

    if alerted_addrs:
        placeholders = ",".join("?" for _ in alerted_addrs)
        old_alerted = cur.execute(
            f"SELECT COUNT(*) FROM pumpswap_fees "
            f"WHERE received_at < ? AND token_address IN ({placeholders})",
            [cutoff] + list(alerted_addrs),
        ).fetchone()[0]
    else:
        old_alerted = 0
    print(f"    of which alerted:  {old_alerted:,} (KEEP)")

    to_delete = old_rows - old_alerted
    print(f"    to delete:         {to_delete:,}\n")

    if to_delete == 0:
        print("✅ Nothing to delete.")
        conn.close()
        return

    pct = (to_delete / total_rows) * 100
    print(f"Will delete {to_delete:,} rows ({pct:.1f}% of table)")

    if not execute:
        print("\n💡 Dry run only. Re-run with --execute to delete + VACUUM.")
        print("💡 Add --no-vacuum to defer the disk reclaim.")
        conn.close()
        return

    # ── Step 3: Find the high-water id for the retention cutoff ──
    # Approach (b): id-bounded range delete.
    #
    # Why not the old subquery DELETE? "DELETE WHERE id IN (SELECT id ...
    # LIMIT N)" holds locks for the duration of the inner scan and blocks
    # the WAL auto-checkpoint — exactly how we got the 154 GB WAL.
    #
    # Why id range works: pumpswap_fees.id is INTEGER PRIMARY KEY
    # AUTOINCREMENT, so id increases monotonically with received_at within
    # any single bot process. Across bot restarts there can be sub-second
    # inversions (a row inserted right after restart can pre-date the last
    # in-flight rows of the previous process), but those windows are far
    # smaller than the 48h retention bucket — never large enough to
    # misclassify a row.
    #
    # Tradeoff vs (a) "materialize ids in a temp table": (b) needs no temp
    # table and no second pass; the per-batch DELETE is a bounded primary-
    # key range scan with an IN/NOT-IN filter, which keeps each lock
    # window short enough for the post-batch checkpoint to truncate the WAL.
    print("Finding high-water id for cutoff...")
    cutoff_id_row = cur.execute(
        "SELECT MAX(id) FROM pumpswap_fees WHERE received_at < ?", (cutoff,)
    ).fetchone()
    cutoff_id = cutoff_id_row[0] if cutoff_id_row and cutoff_id_row[0] is not None else 0
    print(f"  cutoff id: {cutoff_id:,}\n")

    # Build the per-batch protect clause. SQLite barfs on `NOT IN ()`.
    if alerted_addrs:
        addr_placeholders = ",".join("?" for _ in alerted_addrs)
        protect_clause = f"AND token_address NOT IN ({addr_placeholders})"
        addr_params = list(alerted_addrs)
    else:
        protect_clause = ""
        addr_params = []

    # ── Step 4: Delete in id-bounded batches, checkpoint per batch ──
    print(f"Deleting in batches of {BATCH_SIZE:,} (id-range, checkpoint per batch)...")

    deleted_total = 0
    last_id = 0
    start = time.time()

    while last_id < cutoff_id:
        batch_end = min(last_id + BATCH_SIZE, cutoff_id)
        cur.execute(
            f"DELETE FROM pumpswap_fees "
            f"WHERE id > ? AND id <= ? {protect_clause}",
            [last_id, batch_end] + addr_params,
        )
        batch_deleted = cur.rowcount
        conn.commit()
        # Truncating checkpoint after every batch is the load-bearing change.
        # Without it, the WAL grows unbounded during cleanup itself —
        # which is how we ended up here twice.
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        deleted_total += batch_deleted
        last_id = batch_end
        elapsed = time.time() - start
        rate = deleted_total / elapsed if elapsed > 0 else 0
        progress_pct = (last_id / cutoff_id * 100) if cutoff_id else 100
        print(f"  Deleted {deleted_total:,} / {to_delete:,} "
              f"(scanned {progress_pct:.0f}% of id range) — {rate:,.0f} rows/sec")

    print(f"\n✅ Deleted {deleted_total:,} rows in {time.time()-start:.1f}s")

    conn.close()

    # ── Step 5: VACUUM ──
    # Default behaviour. Pass --no-vacuum to skip if the bot is live and
    # you can't afford the exclusive-lock contention.
    if not no_vacuum:
        print("\nRunning VACUUM to reclaim disk space...")
        print("⚠️  This takes an exclusive lock — bot should be stopped.")
        try:
            conn = sqlite3.connect(DB_PATH, timeout=300.0)
            start = time.time()
            conn.execute("VACUUM")
            conn.close()
            print(f"✅ VACUUM complete in {time.time()-start:.1f}s")
        except sqlite3.OperationalError as e:
            print(f"❌ VACUUM failed: {e}")
            print(f"   Stop the bot and re-run with --execute --no-vacuum=false,")
            print(f"   or run `VACUUM;` manually via sqlite3 CLI.")
    else:
        print("\n💡 Skipped VACUUM (--no-vacuum). Disk space remains allocated.")
        print("   Re-run without --no-vacuum once the bot is stopped to reclaim.")

    size_after = os.path.getsize(DB_PATH)
    saved = size_before - size_after
    print(f"\nDB size after:  {human_bytes(size_after)}")
    print(f"Reclaimed:      {human_bytes(saved)}")


if __name__ == "__main__":
    main()
