"""
scripts/ante_vs_alerts.py — Retroactively score every historical dip alert.

For each row in `alerts`, computes what `_compute_ante_stats` would have
returned **as of that alert's `alert_time`** (no future leakage), using the
same windows the live code uses:

    - last-N (default 20) distinct-signature swaps with block_time <= alert_time
    - last-window_seconds (default 300) seconds ending at alert_time

Because base_fee was not captured before the Phase 1 deploy, this script
computes "partial Ante" = priority_fee + jito_tip per tx — the larger
component of full Ante. base_fee adds at most ~5000 lamports per signature
on top, which is noise compared to typical priority+jito ranges.

Output columns:
    address, symbol, tier, alert_time, alert_price, peak_after,
    ante_n20_median, ante_n20_width, ante_5m_median, ante_5m_width

Width ratio uses the same FLOOR (1e-9 SOL) and CAP (10000) as the live code.

Read-only against data/bot.db.

Usage:
    python scripts/ante_vs_alerts.py
    python scripts/ante_vs_alerts.py --out historical_ante.csv
    python scripts/ante_vs_alerts.py --window-n 20 --window-seconds 300
    python scripts/ante_vs_alerts.py --tier 0           # T1 only
"""
import argparse
import csv
import sqlite3
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = str(_REPO_ROOT / "data" / "bot.db")
LAMPORTS = 1_000_000_000.0

# Match modules.telegram_sender constants exactly
ANTE_WIDTH_FLOOR_SOL = 1e-9
ANTE_WIDTH_CAP = 10000.0


def _p25_median_p75(values):
    if not values:
        return (None, None, None)
    if len(values) == 1:
        v = values[0]
        return (v, v, v)
    srt = sorted(values)
    med = statistics.median(srt)
    try:
        q = statistics.quantiles(srt, n=4, method="inclusive")
        return (q[0], med, q[2])
    except statistics.StatisticsError:
        return (srt[0], med, srt[-1])


def _width_ratio(p25, p75):
    if p25 is None or p75 is None:
        return None
    denom = max(p25, ANTE_WIDTH_FLOOR_SOL)
    ratio = p75 / denom
    if ratio > ANTE_WIDTH_CAP:
        return ANTE_WIDTH_CAP
    return ratio


def _fmt_ts(ts):
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _safe_sym(sym):
    if not sym:
        return ""
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        sym.encode(enc)
        return sym
    except UnicodeEncodeError:
        return sym.encode(enc, errors="replace").decode(enc, errors="replace")


def run(out_path: str, window_n: int, window_s: int, tier_filter: int | None,
        limit: int | None):
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    where_tier = "WHERE tier_index = ?" if tier_filter is not None else ""
    params = (tier_filter,) if tier_filter is not None else ()
    limit_clause = f" LIMIT {limit}" if limit else ""

    alerts = cur.execute(f"""
        SELECT id, address, symbol, tier_index, tier_name,
               alert_price, alert_mcap,
               peak_price_after, peak_mcap_after, alert_time
        FROM alerts
        {where_tier}
        ORDER BY alert_time ASC
        {limit_clause}
    """, params).fetchall()

    if not alerts:
        print("No alerts to score.")
        conn.close()
        return

    print(f"Scoring {len(alerts)} alerts (window_n={window_n}, "
          f"window_s={window_s}s, partial Ante = priority+jito)")
    print(f"Writing CSV to: {out_path}")

    n_with_n20 = n_with_m5 = 0
    t0 = time.time()

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "address", "symbol", "tier", "alert_time_utc", "alert_time_unix",
            "alert_price", "peak_after",
            "ante_n20_count", "ante_n20_median_sol", "ante_n20_p25_sol",
            "ante_n20_p75_sol", "ante_n20_width",
            "ante_5m_count", "ante_5m_median_sol", "ante_5m_p25_sol",
            "ante_5m_p75_sol", "ante_5m_width",
        ])

        for i, a in enumerate(alerts):
            addr = a["address"]
            ts = a["alert_time"]

            # Last-N distinct-signature rows AS OF alert_time
            cur.execute(f"""
                SELECT COALESCE(priority_fee,0) + COALESCE(jito_tip,0)
                FROM pumpswap_fees
                WHERE token_address = ?
                  AND priority_fee IS NOT NULL
                  AND block_time <= ?
                ORDER BY block_time DESC
                LIMIT {window_n}
            """, (addr, ts))
            n_vals = [r[0] / LAMPORTS for r in cur.fetchall()]

            # Last-window_s seconds ending at alert_time
            cur.execute("""
                SELECT COALESCE(priority_fee,0) + COALESCE(jito_tip,0)
                FROM pumpswap_fees
                WHERE token_address = ?
                  AND priority_fee IS NOT NULL
                  AND block_time <= ?
                  AND block_time >= ?
            """, (addr, ts, ts - window_s))
            m_vals = [r[0] / LAMPORTS for r in cur.fetchall()]

            n_p25, n_med, n_p75 = _p25_median_p75(n_vals)
            m_p25, m_med, m_p75 = _p25_median_p75(m_vals)
            n_w = _width_ratio(n_p25, n_p75)
            m_w = _width_ratio(m_p25, m_p75)

            if n_med is not None:
                n_with_n20 += 1
            if m_med is not None:
                n_with_m5 += 1

            w.writerow([
                addr,
                _safe_sym(a["symbol"]),
                a["tier_index"],
                _fmt_ts(ts),
                f"{ts:.0f}",
                f"{a['alert_price']:.10f}" if a["alert_price"] else "",
                f"{a['peak_price_after']:.10f}" if a["peak_price_after"] else "",
                len(n_vals),
                f"{n_med:.9f}" if n_med is not None else "",
                f"{n_p25:.9f}" if n_p25 is not None else "",
                f"{n_p75:.9f}" if n_p75 is not None else "",
                f"{n_w:.3f}" if n_w is not None else "",
                len(m_vals),
                f"{m_med:.9f}" if m_med is not None else "",
                f"{m_p25:.9f}" if m_p25 is not None else "",
                f"{m_p75:.9f}" if m_p75 is not None else "",
                f"{m_w:.3f}" if m_w is not None else "",
            ])

            if (i + 1) % 250 == 0:
                print(f"  scored {i+1}/{len(alerts)} alerts...")

    conn.close()

    dt = time.time() - t0
    print(f"\nDone in {dt:.1f}s — wrote {len(alerts)} rows to {out_path}")
    print(f"Coverage: {n_with_n20}/{len(alerts)} alerts had >=1 sample in last-{window_n} window "
          f"({100*n_with_n20/len(alerts):.0f}%)")
    print(f"          {n_with_m5}/{len(alerts)} had >=1 sample in last-{window_s}s window "
          f"({100*n_with_m5/len(alerts):.0f}%)")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="historical_ante.csv",
                   help="Output CSV path (default historical_ante.csv)")
    p.add_argument("--window-n", type=int, default=20,
                   help="Last-N distinct-signature swaps window (default 20)")
    p.add_argument("--window-seconds", type=int, default=300,
                   help="Trailing-seconds window ending at alert_time (default 300)")
    p.add_argument("--tier", type=int, default=None,
                   help="Filter by tier_index (0/1/2). Omit for all.")
    p.add_argument("--limit", type=int, default=None,
                   help="Max alerts to score (default = all). Useful for smoke tests.")
    args = p.parse_args()
    run(args.out, args.window_n, args.window_seconds, args.tier, args.limit)


if __name__ == "__main__":
    main()
