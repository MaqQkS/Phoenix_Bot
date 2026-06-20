"""
export_ante_candidates.py — Export Ante primitive output per TOKEN for manual labeling.

Collapses ante_log from per-alert rows to one row per token_address.
Keeps the FIRST alert's Ante snapshot (earliest signal) plus aggregates:
  - total alert count for that token
  - distinct taxonomy labels seen across all pings
  - worst-case outcome_x across all alerts

Usage:
    python export_ante_candidates.py
    python export_ante_candidates.py --since-days 7 --out ante_candidates.csv
"""

import argparse
import asyncio
import csv
import aiosqlite

from database import db_connect

DB_PATH = "data/bot.db"

HEADER = [
    # Identity
    "token_address",
    "symbol",
    "migration_time_utc",
    # First alert snapshot (earliest Ante signal for this token)
    "first_alert_tier",
    "first_tier_name",
    "first_alert_time_utc",
    "age_at_first_alert_hours",
    # 20sw window (from first alert)
    "median_20sw_sol",
    "p25_20sw_sol",
    "p75_20sw_sol",
    "width_20sw",
    "n_20sw",
    # 5m window (from first alert)
    "median_5m_sol",
    "p25_5m_sol",
    "p75_5m_sol",
    "width_5m",
    "n_5m",
    "base_fee_cov_pct",
    # Taxonomy from first alert
    "label_5m",
    "rule_hit_5m",
    "label_20sw",
    "rule_hit_20sw",
    "windows_agree",
    # Aggregates across all alerts for this token
    "total_alerts",
    "distinct_labels_5m",
    "distinct_labels_20sw",
    # Best outcome across all alerts
    "first_alert_mcap",
    "best_peak_mcap_after",
    "best_outcome_x",
    # Fee Gate cross-reference
    "fee_gate_label",
    "fee_gate_score",
    # Manual labeling
    "label",
    "notes",
]


async def main(since_days: int, out_path: str):
    cutoff_clause = ""
    params = []
    if since_days > 0:
        cutoff_clause = "WHERE al.alert_time >= strftime('%s','now') - ?"
        params.append(since_days * 86400)

    # Pull all ante_log rows (we collapse in Python for clarity)
    query = f"""
        SELECT
            al.token_address,
            al.symbol,
            al.alert_tier,
            al.tier_name,
            al.alert_time,
            t.migration_time,
            al.ante_n20_median_sol,
            al.ante_n20_p25_sol,
            al.ante_n20_p75_sol,
            al.ante_n20_width_ratio,
            al.ante_n20_count,
            al.ante_5m_median_sol,
            al.ante_5m_p25_sol,
            al.ante_5m_p75_sol,
            al.ante_5m_width_ratio,
            al.ante_5m_count,
            al.base_fee_coverage,
            al.label_5m,
            al.rule_hit_5m,
            al.label_20sw,
            al.rule_hit_20sw
        FROM ante_log al
        LEFT JOIN tokens t ON t.address = al.token_address
        {cutoff_clause}
        ORDER BY al.alert_time ASC
    """

    # Collect per-token: first alert snapshot + aggregates
    token_first = {}   # token_address -> first row dict
    token_agg = {}     # token_address -> {count, labels_5m set, labels_20sw set}

    async with db_connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cur:
            async for r in cur:
                addr = r["token_address"]

                if addr not in token_first:
                    # First alert for this token — capture snapshot
                    token_first[addr] = dict(r)
                    token_agg[addr] = {
                        "count": 0,
                        "labels_5m": set(),
                        "labels_20sw": set(),
                    }

                # Aggregate across all alerts
                agg = token_agg[addr]
                agg["count"] += 1
                if r["label_5m"]:
                    agg["labels_5m"].add(r["label_5m"])
                if r["label_20sw"]:
                    agg["labels_20sw"].add(r["label_20sw"])

        # Pull alert outcome data per token (best peak across all tiers)
        alert_outcomes = {}
        async with db.execute("""
            SELECT address,
                   MIN(CASE WHEN tier_index = (
                       SELECT MIN(tier_index) FROM alerts a2 WHERE a2.address = a.address
                   ) THEN alert_mcap END) AS first_alert_mcap,
                   MAX(peak_mcap_after) AS best_peak
            FROM alerts a
            GROUP BY address
        """) as cur:
            async for r in cur:
                alert_outcomes[r[0]] = {
                    "first_alert_mcap": r[1],
                    "best_peak": r[2],
                }

        # Pull fee_gate cross-reference (worst label per token)
        fee_gate_refs = {}
        async with db.execute("""
            SELECT token_address, MAX(score) AS max_score, label
            FROM fee_gate_log
            GROUP BY token_address
            ORDER BY max_score DESC
        """) as cur:
            async for r in cur:
                fee_gate_refs[r[0]] = {
                    "label": r[2],
                    "score": r[1],
                }

    # Build output rows
    rows = []
    for addr, first in token_first.items():
        agg = token_agg[addr]
        outcome = alert_outcomes.get(addr, {})
        fg = fee_gate_refs.get(addr, {})

        alert_ts = first["alert_time"] or 0
        mig_ts = first["migration_time"] or 0
        age_h = ""
        if alert_ts and mig_ts:
            age_h = round((alert_ts - mig_ts) / 3600.0, 2)

        first_mcap = outcome.get("first_alert_mcap")
        best_peak = outcome.get("best_peak")
        best_outcome_x = ""
        if first_mcap and best_peak and first_mcap > 0:
            best_outcome_x = round(best_peak / first_mcap, 3)

        label_5m = first["label_5m"] or ""
        label_20sw = first["label_20sw"] or ""
        windows_agree = ""
        if label_5m and label_20sw:
            windows_agree = "YES" if label_5m == label_20sw else "NO"

        rows.append({
            "token_address": addr,
            "symbol": first["symbol"] or "",
            "migration_time_utc": mig_ts,
            "first_alert_tier": first["alert_tier"] or "",
            "first_tier_name": first["tier_name"] or "",
            "first_alert_time_utc": alert_ts,
            "age_at_first_alert_hours": age_h,
            "median_20sw_sol": first["ante_n20_median_sol"] or "",
            "p25_20sw_sol": first["ante_n20_p25_sol"] or "",
            "p75_20sw_sol": first["ante_n20_p75_sol"] or "",
            "width_20sw": first["ante_n20_width_ratio"] or "",
            "n_20sw": first["ante_n20_count"] or "",
            "median_5m_sol": first["ante_5m_median_sol"] or "",
            "p25_5m_sol": first["ante_5m_p25_sol"] or "",
            "p75_5m_sol": first["ante_5m_p75_sol"] or "",
            "width_5m": first["ante_5m_width_ratio"] or "",
            "n_5m": first["ante_5m_count"] or "",
            "base_fee_cov_pct": first["base_fee_coverage"] or "",
            "label_5m": label_5m,
            "rule_hit_5m": first["rule_hit_5m"] or "",
            "label_20sw": label_20sw,
            "rule_hit_20sw": first["rule_hit_20sw"] or "",
            "windows_agree": windows_agree,
            "total_alerts": agg["count"],
            "distinct_labels_5m": "|".join(sorted(agg["labels_5m"])) if agg["labels_5m"] else "",
            "distinct_labels_20sw": "|".join(sorted(agg["labels_20sw"])) if agg["labels_20sw"] else "",
            "first_alert_mcap": first_mcap or "",
            "best_peak_mcap_after": best_peak or "",
            "best_outcome_x": best_outcome_x,
            "fee_gate_label": fg.get("label", ""),
            "fee_gate_score": fg.get("score", ""),
            "label": "",
            "notes": "",
        })

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in HEADER})

    print(f"\n✓ Wrote {len(rows)} tokens to {out_path}")
    print(f"  (collapsed from {sum(a['count'] for a in token_agg.values())} ante_log rows)")
    if since_days > 0:
        print(f"  Window: last {since_days} days of ante_log")
    else:
        print(f"  Window: all ante_log rows")
    print(f"  Next: open in Excel, label each token organic/wash/bimodal/coordinated/ambiguous")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--since-days", type=int, default=0, help="0 = all rows")
    p.add_argument("--out", default="ante_candidates.csv")
    args = p.parse_args()
    asyncio.run(main(args.since_days, args.out))