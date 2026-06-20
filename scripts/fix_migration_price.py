"""
scripts/fix_migration_price.py

One-shot backfill: align migration_price with migration_mcap on the
fixed 1B pump.fun supply basis.

Why:
    migration_price was historically captured from Dexscreener's early
    pair fetch, which encodes a variable implied supply. migration_mcap
    is computed from sol_price × 410 (reliable). This script recomputes
    migration_price = migration_mcap / 1_000_000_000 for every row with
    migration_mcap > 0, so pump_multiple = ath_price / migration_price
    is internally consistent.

Usage:
    # Preview (no writes):
    python scripts/fix_migration_price.py --dry-run

    # Apply (single transaction):
    python scripts/fix_migration_price.py

    # Custom DB:
    python scripts/fix_migration_price.py --db path/to/bot.db [--dry-run]

Requires manual execution. Not invoked automatically.
"""

import argparse
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB_PATH = "data/bot.db"
FIXED_SUPPLY = 1_000_000_000


def run(db_path: str, dry_run: bool) -> int:
    p = Path(db_path)
    if not p.exists():
        print(f"ERROR: database not found at {db_path}", file=sys.stderr)
        return 1

    mode = "DRY RUN" if dry_run else "APPLY"
    print(f"[{mode}] Opening {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM tokens WHERE migration_mcap > 0")
        eligible = cur.fetchone()[0]
        print(f"  eligible rows (migration_mcap > 0): {eligible}")

        cur.execute(
            "SELECT address, symbol, migration_price, migration_mcap "
            "FROM tokens WHERE migration_mcap > 0 "
            "ORDER BY migration_time DESC LIMIT 5"
        )
        samples = cur.fetchall()
        if samples:
            print()
            print("  sample (5 most-recent tokens):")
            print(
                f"    {'address':<44} {'symbol':<10} "
                f"{'old_price':>14} {'new_price':>14} "
                f"{'migration_mcap':>14}"
            )
            for row in samples:
                old_p = row["migration_price"] or 0.0
                new_p = row["migration_mcap"] / FIXED_SUPPLY
                print(
                    f"    {row['address']:<44} "
                    f"{(row['symbol'] or '???'):<10} "
                    f"{old_p:>14.10f} {new_p:>14.10f} "
                    f"{row['migration_mcap']:>14.2f}"
                )

        if dry_run:
            print()
            print(f"  DRY RUN: would update {eligible} row(s). No changes written.")
            return 0

        # Single transaction; roll back on any error.
        cur.execute("BEGIN")
        try:
            cur.execute(
                "UPDATE tokens "
                "SET migration_price = migration_mcap / ? "
                "WHERE migration_mcap > 0",
                (float(FIXED_SUPPLY),),
            )
            updated = cur.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        print()
        print(f"  updated {updated} row(s). committed.")

        addrs = [s["address"] for s in samples]
        if addrs:
            placeholders = ",".join("?" * len(addrs))
            cur.execute(
                f"SELECT address, symbol, migration_price, migration_mcap "
                f"FROM tokens WHERE address IN ({placeholders})",
                addrs,
            )
            print()
            print("  post-update verification (same samples):")
            for row in cur.fetchall():
                print(
                    f"    {row['address']:<44} "
                    f"{(row['symbol'] or '???'):<10} "
                    f"migration_price={row['migration_price']:.10f} "
                    f"migration_mcap={row['migration_mcap']:.2f}"
                )

        return 0

    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill migration_price = migration_mcap / 1B."
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="path to bot.db")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="preview changes without writing"
    )
    args = parser.parse_args()
    return run(args.db, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
