"""
block_stats.py — Show alert_gate block decisions.
Run: python block_stats.py
     python block_stats.py --days 14
"""

import argparse
import os
import sqlite3
import statistics
import time

DB_PATH = "data/bot.db"


def fmt_duration(seconds: float) -> str:
    if seconds is None or seconds < 0:
        return "?"
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def show_block_stats(conn, days: int):
    """Full block breakdown over the last `days` days."""
    table_check = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='alert_block_log'"
    ).fetchone()
    if not table_check:
        print("No alert_block_log table found. Run the bot once to create the schema.")
        return

    since = time.time() - days * 86400
    total = conn.execute(
        "SELECT COUNT(*) FROM alert_block_log WHERE block_time > ?", (since,)
    ).fetchone()[0]

    if total == 0:
        print(
            "No blocks recorded yet. blocking_enabled may be false "
            "or no SCAM Likely alerts have fired."
        )
        return

    period = f"last {days} day(s)" if days > 1 else "last 24 hours"
    print()
    print("═" * 72)
    print(f"  BLOCK STATS — {period}")
    print("═" * 72)
    print()
    print(f"  Total blocks: {total}")

    # ── Breakdown by reason ──────────────────────────────────────────────
    by_reason = conn.execute("""
        SELECT block_reason, COUNT(*) AS cnt
        FROM alert_block_log
        WHERE block_time > ?
        GROUP BY block_reason
        ORDER BY cnt DESC
    """, (since,)).fetchall()

    print()
    print(f"  📋 Breakdown by Reason:")
    print(f"  {'Reason':<24}│ {'Blocks':<8}│ % of total")
    print(f"  {'─' * 24}┼{'─' * 8}┼{'─' * 12}")
    for row in by_reason:
        pct = (row["cnt"] / total * 100) if total else 0
        print(f"  {row['block_reason']:<24}│ {row['cnt']:<8}│ {pct:.1f}%")

    # ── Daily breakdown by reason ────────────────────────────────────────
    daily = conn.execute("""
        SELECT DATE(block_time, 'unixepoch') AS day,
               block_reason,
               COUNT(*) AS cnt
        FROM alert_block_log
        WHERE block_time > ?
        GROUP BY day, block_reason
        ORDER BY day DESC, cnt DESC
    """, (since,)).fetchall()

    print()
    print(f"  📅 Daily Breakdown:")
    print(f"  {'Day':<12}│ {'Reason':<24}│ Count")
    print(f"  {'─' * 12}┼{'─' * 24}┼{'─' * 8}")
    for row in daily:
        print(f"  {row['day']:<12}│ {row['block_reason']:<24}│ {row['cnt']}")

    # ── Top 10 most-blocked symbols ──────────────────────────────────────
    top_symbols = conn.execute("""
        SELECT symbol, COUNT(*) AS cnt
        FROM alert_block_log
        WHERE block_time > ?
        GROUP BY symbol
        ORDER BY cnt DESC
        LIMIT 10
    """, (since,)).fetchall()

    print()
    print(f"  🔝 Top 10 Most-Blocked Symbols:")
    print(f"  {'Symbol':<14}│ Blocks")
    print(f"  {'─' * 14}┼{'─' * 10}")
    for row in top_symbols:
        symbol = f"${row['symbol']}" if row["symbol"] else "???"
        print(f"  {symbol:<14}│ {row['cnt']}")

    # ── Resolution rate for no_fee_data blocks ───────────────────────────
    resolution = conn.execute("""
        SELECT
            SUM(CASE WHEN EXISTS (
                SELECT 1 FROM fee_gate_log fgl
                WHERE fgl.token_address = abl.token_address
                  AND fgl.alert_time > abl.block_time
            ) THEN 1 ELSE 0 END) AS resolved,
            SUM(CASE WHEN NOT EXISTS (
                SELECT 1 FROM fee_gate_log fgl
                WHERE fgl.token_address = abl.token_address
                  AND fgl.alert_time > abl.block_time
            ) THEN 1 ELSE 0 END) AS pending
        FROM alert_block_log abl
        WHERE abl.no_fee_data = 1
          AND abl.block_time > ?
    """, (since,)).fetchone()
    resolved = (resolution["resolved"] or 0) if resolution else 0
    pending = (resolution["pending"] or 0) if resolution else 0
    nfd_total = resolved + pending

    print()
    print(f"  🔄 No-Fee-Data Resolution:")
    if nfd_total > 0:
        rate = resolved / nfd_total * 100
        print(f"  Resolved: {resolved} / {nfd_total} ({rate:.0f}%) — fee data arrived post-block")
        print(f"  Pending:  {pending} — still awaiting any fee_gate_log entry")
    else:
        print(f"  No no_fee_data blocks in window.")

    # ── Median time from migration to first block ────────────────────────
    times = conn.execute("""
        SELECT MIN(abl.block_time) - t.migration_time AS time_to_first_block
        FROM alert_block_log abl
        JOIN tokens t ON t.address = abl.token_address
        WHERE abl.block_time > ?
          AND t.migration_time > 0
        GROUP BY abl.token_address, t.migration_time
    """, (since,)).fetchall()
    durations = [
        row["time_to_first_block"] for row in times
        if row["time_to_first_block"] is not None
    ]

    print()
    print(f"  ⏱️  Migration → First Block:")
    if len(durations) >= 2:
        med = statistics.median(durations)
        q = statistics.quantiles(durations, n=4)
        print(
            f"  Median: {fmt_duration(med)} │ "
            f"p25: {fmt_duration(q[0])} │ "
            f"p75: {fmt_duration(q[2])}"
        )
        print(f"  Based on {len(durations)} distinct token(s).")
    elif durations:
        print(f"  Only one sample: {fmt_duration(durations[0])}")
    else:
        print(f"  No migration timestamps available for blocked tokens.")

    print()
    print("═" * 72)
    print()


def main():
    parser = argparse.ArgumentParser(description="Phoenix Bot Block Stats")
    parser.add_argument("--days", type=int, default=7, help="Window in days (default: 7)")
    args = parser.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    show_block_stats(conn, args.days)
    conn.close()


if __name__ == "__main__":
    main()
