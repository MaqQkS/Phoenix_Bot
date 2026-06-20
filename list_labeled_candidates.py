"""
list_labeled_candidates.py — Dump Phoenix-called tokens with clean priority/jito coverage.

Only includes tokens that were:
  1. Alerted on by Phoenix (in the alerts table)
  2. Migrated after the gRPC priority indexer came online
  3. Have full fee coverage from migration onward (first fee event within
     10 min of migration_time)

Includes outcome_x (peak_mcap_after / first_alert_mcap) so you can auto-label
winners vs losers.

Usage:
    python list_labeled_candidates.py
    python list_labeled_candidates.py --min-txs 50
    python list_labeled_candidates.py --out my_labels.csv
"""

import argparse
import asyncio
import csv
from datetime import datetime, timezone

import aiosqlite

from database import db_connect

DB_PATH = "data/bot.db"
LAMPORTS_PER_SOL = 1_000_000_000


async def dump(min_txs: int, out_path: str):
    query = """
        SELECT
            f.token_address,
            COUNT(DISTINCT f.signature) AS tx_count,
            COUNT(*) AS event_count,
            MIN(f.block_time) AS first_seen,
            MAX(f.block_time) AS last_seen,
            COALESCE(SUM(f.lp_fee), 0) AS lp_total,
            COALESCE(SUM(f.protocol_fee), 0) AS proto_total,
            COALESCE(SUM(f.creator_fee), 0) AS creator_total,
            COALESCE(SUM(f.priority_fee), 0) AS priority_total,
            COALESCE(SUM(f.jito_tip), 0) AS jito_total,
            COALESCE(SUM(f.compute_units_consumed), 0) AS cu_total,
            t.symbol,
            t.migration_time,
            (SELECT MIN(a.tier_index) FROM alerts a WHERE a.address = f.token_address) AS first_tier,
            (SELECT MAX(a.peak_mcap_after) FROM alerts a WHERE a.address = f.token_address) AS peak_mcap_after,
            (SELECT MIN(a.alert_mcap) FROM alerts a WHERE a.address = f.token_address) AS first_alert_mcap,
            (SELECT label FROM fee_gate_log WHERE token_address = f.token_address ORDER BY alert_time DESC LIMIT 1) AS fg_label,
            (SELECT score FROM fee_gate_log WHERE token_address = f.token_address ORDER BY alert_time DESC LIMIT 1) AS fg_score,
            (SELECT flags FROM fee_gate_log WHERE token_address = f.token_address ORDER BY alert_time DESC LIMIT 1) AS fg_flags,
            (SELECT fee_per_event FROM fee_gate_log WHERE token_address = f.token_address ORDER BY alert_time DESC LIMIT 1) AS fg_bribes_pct,
            (SELECT proto_to_lp FROM fee_gate_log WHERE token_address = f.token_address ORDER BY alert_time DESC LIMIT 1) AS fg_bribes_per_tx,
            (SELECT creator_share FROM fee_gate_log WHERE token_address = f.token_address ORDER BY alert_time DESC LIMIT 1) AS fg_creator_share
        FROM pumpswap_fees f
        JOIN tokens t ON t.address = f.token_address
        WHERE f.token_address IS NOT NULL
          AND f.priority_fee IS NOT NULL
          AND f.token_address IN (SELECT DISTINCT address FROM alerts)
          AND t.migration_time IS NOT NULL
        GROUP BY f.token_address
        HAVING tx_count >= ?
           AND MIN(f.block_time) <= t.migration_time + 600
        ORDER BY t.migration_time ASC
    """

    async with db_connect(DB_PATH) as db:
        async with db.execute(query, (min_txs,)) as cur:
            rows = await cur.fetchall()

    if not rows:
        print(f"No clean-coverage alerted tokens found with >= {min_txs} txs")
        return

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "token_address", "symbol", "migration_time_utc",
            "tx_count", "event_count",
            "first_seen_utc", "last_seen_utc", "duration_hours",
            "amm_sol", "lp_sol", "proto_sol", "creator_sol",
            "priority_sol", "jito_sol", "bribes_sol",
            "bribes_pct_of_amm", "creator_share_of_amm",
            "priority_per_tx_lamports", "jito_per_tx_lamports",
            "avg_cu_per_tx",
            "first_tier", "peak_mcap_after", "first_alert_mcap",
            "outcome_x",
            "fg_label", "fg_score", "fg_flags",
            "fg_bribes_pct", "fg_bribes_per_tx_sol", "fg_creator_share",
            "verdict_match",
            "label", "notes",
        ])

        winners = losers = neutral = blank = 0

        for r in rows:
            (token, tx_count, event_count, first_seen, last_seen,
             lp, proto, creator, priority, jito, cu_total,
             symbol, migration_time,
             first_tier, peak_mcap_after, first_alert_mcap,
             fg_label, fg_score, fg_flags, fg_bribes_pct, fg_bribes_per_tx, fg_creator_share) = r

            amm_lamports = lp + proto + creator
            amm_sol = amm_lamports / LAMPORTS_PER_SOL
            priority_sol = priority / LAMPORTS_PER_SOL
            jito_sol = jito / LAMPORTS_PER_SOL
            bribes_sol = priority_sol + jito_sol
            bribes_pct = (bribes_sol / amm_sol * 100) if amm_sol > 0 else 0
            creator_share = (creator / amm_lamports * 100) if amm_lamports > 0 else 0
            duration_hours = (last_seen - first_seen) / 3600 if last_seen > first_seen else 0
            outcome_x = (peak_mcap_after / first_alert_mcap) if (first_alert_mcap and first_alert_mcap > 0) else 0

            if outcome_x == 0:
                blank += 1
            elif outcome_x >= 1.5:
                winners += 1
            elif outcome_x < 1.0:
                losers += 1
            else:
                neutral += 1

            w.writerow([
                token, symbol or "", _fmt_ts(migration_time),
                tx_count, event_count,
                _fmt_ts(first_seen), _fmt_ts(last_seen), f"{duration_hours:.2f}",
                f"{amm_sol:.4f}", f"{lp/LAMPORTS_PER_SOL:.4f}",
                f"{proto/LAMPORTS_PER_SOL:.4f}", f"{creator/LAMPORTS_PER_SOL:.4f}",
                f"{priority_sol:.4f}", f"{jito_sol:.4f}", f"{bribes_sol:.4f}",
                f"{bribes_pct:.2f}", f"{creator_share:.2f}",
                int(priority / tx_count) if tx_count else 0,
                int(jito / tx_count) if tx_count else 0,
                int(cu_total / tx_count) if tx_count else 0,
                first_tier if first_tier is not None else "",
                f"{peak_mcap_after:.0f}" if peak_mcap_after else "",
                f"{first_alert_mcap:.0f}" if first_alert_mcap else "",
                f"{outcome_x:.2f}",
                fg_label or "",
                fg_score if fg_score is not None else "",
                fg_flags or "",
                f"{fg_bribes_pct:.2f}" if fg_bribes_pct is not None else "",
                f"{fg_bribes_per_tx * 1e9:.0f}" if fg_bribes_per_tx is not None else "",
                f"{fg_creator_share * 100:.1f}" if fg_creator_share is not None else "",
                "",  # verdict_match — filled by Excel formula after manual label entry
                "", "",
            ])

    print(f"\nWrote {len(rows)} clean-coverage tokens to {out_path}")
    print(f"\nOutcome distribution:")
    print(f"  winners (outcome_x >= 1.5):  {winners}")
    print(f"  neutral (1.0 <= x < 1.5):    {neutral}")
    print(f"  losers  (outcome_x < 1.0):   {losers}")
    print(f"  blank   (no peak data yet):  {blank}")
    print(f"\nSorted by migration_time ascending — first row is your patient zero.")


def _fmt_ts(unix_ts):
    if not unix_ts:
        return ""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--min-txs", type=int, default=100,
                   help="Minimum tx count to include (default 100)")
    p.add_argument("--out", default="labeling_candidates.csv",
                   help="Output CSV path (default labeling_candidates.csv)")
    args = p.parse_args()
    asyncio.run(dump(args.min_txs, args.out))


if __name__ == "__main__":
    main()