"""
scripts/query_ante_alerts.py — Join alerts × ante_log over a recent window.

Satisfies the Phase 1 success criterion:
    "show me all alerts from the last 7 days with their Ante values"

Read-only. Never mutates the DB.

Usage:
    python scripts/query_ante_alerts.py
    python scripts/query_ante_alerts.py --days 7
    python scripts/query_ante_alerts.py --days 14 --csv out.csv
    python scripts/query_ante_alerts.py --tier 1
"""
import argparse
import csv
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = str(_REPO_ROOT / "data" / "bot.db")


def _fmt_ts(unix_ts):
    if not unix_ts:
        return ""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _fmt_sol(v):
    if v is None:
        return "    n/a"
    return f"{v:.6f}"


def run(days: int, tier_filter: int | None, out_csv: str | None):
    since = time.time() - days * 86400
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    # Verify ante_log exists — migration may not have run yet
    has_ante = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='ante_log'"
    ).fetchone() is not None
    if not has_ante:
        print("ante_log table not present yet — run scripts/migrate_add_base_fee.py first.")
        return

    where_tier = "AND a.tier_index = ?" if tier_filter is not None else ""
    params = [since]
    if tier_filter is not None:
        params.append(tier_filter)

    rows = conn.execute(f"""
        SELECT
            a.alert_time, a.symbol, a.address, a.tier_index, a.tier_name,
            a.alert_mcap, a.peak_mcap_after,
            al.ante_n20_count, al.ante_n20_median_sol, al.ante_n20_p25_sol, al.ante_n20_p75_sol,
            al.ante_n20_width_ratio,
            al.ante_5m_count,  al.ante_5m_median_sol,  al.ante_5m_p25_sol,  al.ante_5m_p75_sol,
            al.ante_5m_width_ratio,
            al.base_fee_coverage
        FROM alerts a
        LEFT JOIN ante_log al
            ON al.token_address = a.address
            AND al.alert_time = a.alert_time
        WHERE a.alert_time >= ?
          {where_tier}
        ORDER BY a.alert_time DESC
    """, params).fetchall()
    conn.close()

    if not rows:
        print(f"No alerts in the last {days} days.")
        return

    if out_csv:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([k for k in rows[0].keys()])
            for r in rows:
                w.writerow([r[k] for k in r.keys()])
        print(f"Wrote {len(rows)} rows to {out_csv}")
        return

    print(f"Alerts in last {days}d: {len(rows)}")
    print()
    header = (
        f"{'time (UTC)':16} {'symbol':10} {'tier':6} "
        f"{'n20 med':>10} {'n20 wid':>8} "
        f"{'5m med':>10} {'5m wid':>8} {'n5m':>5} {'cov':>5}  "
        f"{'outcome_x':>9}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        out_x = (
            r["peak_mcap_after"] / r["alert_mcap"]
            if r["alert_mcap"] and r["peak_mcap_after"]
            else 0
        )
        cov = r["base_fee_coverage"]
        n20_w = r["ante_n20_width_ratio"]
        m5_w  = r["ante_5m_width_ratio"]
        print(
            f"{_fmt_ts(r['alert_time']):16} "
            f"{(r['symbol'] or '')[:10]:10} "
            f"T{r['tier_index']}     "
            f"{_fmt_sol(r['ante_n20_median_sol']):>10} "
            f"{(f'{n20_w:>7.1f}x' if n20_w is not None else '    n/a'):>8} "
            f"{_fmt_sol(r['ante_5m_median_sol']):>10} "
            f"{(f'{m5_w:>7.1f}x' if m5_w is not None else '    n/a'):>8} "
            f"{(r['ante_5m_count'] or 0):>5} "
            f"{(int(cov*100) if cov is not None else 0):>4}% "
            f"{out_x:>9.2f}"
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=7, help="Look-back window (default 7)")
    p.add_argument("--tier", type=int, default=None,
                   help="Filter by tier index (0/1/2). Omit for all.")
    p.add_argument("--csv", default=None, help="Write rows to CSV instead of printing")
    args = p.parse_args()
    run(args.days, args.tier, args.csv)


if __name__ == "__main__":
    main()
