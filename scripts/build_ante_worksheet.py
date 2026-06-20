"""
Phase 1 — build manual-labeling worksheet for Ante classifier validation.

Pulls up to 15 tokens per bucket from ante_log, deduped by (token, bucket),
preferring n20_count >= 20 and logged within the last 7 days (all current
rows meet both — see diagnostics). Writes CSV + MD to diagnostics_out/.

Read-only. Does not modify bot state. Safe to run while bot runs.
"""
import csv
import os
import sqlite3
from datetime import datetime, timezone

DB_PATH = "data/bot.db"
OUT_CSV = "diagnostics_out/ANTE_VALIDATION_WORKSHEET.csv"
OUT_MD = "diagnostics_out/ANTE_VALIDATION_WORKSHEET.md"
PER_BUCKET = 15
MIN_N = 20
LAST_DAYS = 7

BUCKETS = ["ORGANIC", "WASH_UNIFORM", "BIMODAL", "COORDINATED", "AMBIGUOUS", "INSUFFICIENT"]


def sig2(x):
    """Round to 2 significant figures for display."""
    if x is None:
        return ""
    try:
        x = float(x)
    except (TypeError, ValueError):
        return ""
    if x == 0:
        return "0"
    from math import floor, log10
    d = 2 - int(floor(log10(abs(x)))) - 1
    return f"{round(x, d):.{max(d,0)}f}" if d >= 0 else f"{round(x, d)}"


def fetch_sample(cur, bucket, limit):
    # Latest row per (token, bucket), then random sample up to `limit`.
    cur.execute(
        """
        WITH latest AS (
          SELECT token_address, symbol,
                 MAX(alert_time) AS max_time
          FROM ante_log
          WHERE label_20sw = ?
            AND ante_n20_count >= ?
            AND alert_time >= strftime('%s', 'now', ?)
          GROUP BY token_address
        )
        SELECT a.token_address, a.symbol, a.alert_time, a.label_20sw,
               a.ante_n20_median_sol, a.ante_n20_p25_sol, a.ante_n20_p75_sol,
               a.ante_n20_width_ratio, a.ante_n20_count,
               a.ante_5m_median_sol, a.label_5m, a.rule_hit_20sw
        FROM ante_log a
        JOIN latest l
          ON a.token_address = l.token_address
         AND a.alert_time    = l.max_time
        WHERE a.label_20sw = ?
        ORDER BY RANDOM()
        LIMIT ?
        """,
        (bucket, MIN_N, f"-{LAST_DAYS} days", bucket, limit),
    )
    return cur.fetchall()


def main():
    os.makedirs("diagnostics_out", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    rows_by_bucket = {}
    insufficient_buckets = []
    for b in BUCKETS:
        rows = fetch_sample(cur, b, PER_BUCKET)
        rows_by_bucket[b] = rows

    # Coverage note: flag buckets with total population < 25 (not samples pulled)
    cur.execute(
        "SELECT label_20sw, COUNT(*) FROM ante_log "
        "WHERE label_20sw IS NOT NULL GROUP BY label_20sw"
    )
    pop = dict(cur.fetchall())
    for b in BUCKETS:
        if pop.get(b, 0) < 25:
            insufficient_buckets.append((b, pop.get(b, 0)))

    # Write CSV
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "classifier_bucket", "token_address", "symbol", "logged_at",
            "dexscreener_url",
            "median_20sw_usol", "p25_20sw_usol", "p75_20sw_usol",
            "width_ratio_20sw", "n_samples_20sw", "median_5m_usol",
            "manual_label", "match_yn", "notes",
        ])
        for b in BUCKETS:
            for r in rows_by_bucket[b]:
                (addr, sym, atime, lbl, med20, p25, p75, w20, n20,
                 med5, lbl5, rule) = r
                logged = datetime.fromtimestamp(atime, tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M UTC"
                )
                # Convert SOL → µSOL for readability (1 SOL = 1e6 µSOL)
                def u(x):
                    return "" if x is None else f"{x * 1e6:.2f}"
                w.writerow([
                    lbl, addr, sym or "", logged,
                    f"https://dexscreener.com/solana/{addr}",
                    u(med20), u(p25), u(p75),
                    "" if w20 is None else f"{w20:.2f}",
                    n20,
                    u(med5),
                    "", "", "",
                ])

    # Write MD
    lines = []
    lines.append("# Ante Classifier Validation Worksheet — Phase 1\n")
    lines.append(f"_Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}_\n")
    lines.append("")
    lines.append(
        "This worksheet is for manually validating the Ante taxonomy classifier "
        "(ghost mode) against chart-based reads. Fill in `manual_label`, `match_yn`, "
        "and `notes` columns for each row, then run "
        "`python scripts/ante_precision_analysis.py` to compute precision.\n"
    )
    lines.append("")
    lines.append("## Read order — 4-shape heuristic\n")
    lines.append(
        "For each token, open Dexscreener and look at the first 10–20 min of trading. "
        "Form your own read **before** looking at `classifier_bucket` (cover it with your "
        "hand, or split-screen with the address hidden).\n"
    )
    lines.append(
        "1. **n** — sample count. If under 10, anything downstream is noise.\n"
        "2. **p25** — lower-quartile ante. Is it at the physical floor (5 µSOL) or above real-fee territory (~100 µSOL)?\n"
        "3. **p75** — upper-quartile ante. Does the top tail pay real money or just mirror p25?\n"
        "4. **width** — p75/p25 ratio. Tight (<3) = one fee regime. Wide (>25) = bimodal / two populations.\n"
    )
    lines.append(
        "Then cross-check: do the 5m and 20sw reads agree? Disagreement between "
        "windows is itself signal.\n"
    )
    lines.append("")
    lines.append("## Bucket taxonomy (from modules/ante_taxonomy.py)\n")
    lines.append(
        "These are the six labels the classifier can emit. Use the same names in "
        "`manual_label`, or `AMBIGUOUS` if you genuinely can't tell.\n"
    )
    lines.append(
        "| Bucket | Rule | Condition (width = p75/p25 ratio) |\n"
        "|---|---|---|\n"
        "| `INSUFFICIENT` | 1 | n < 10 |\n"
        "| `BIMODAL` | 2 | p25 at floor AND width ≥ 25 |\n"
        "| `WASH_UNIFORM` | 3 | width < 2 AND p75 < real_fee (100 µSOL) |\n"
        "| `COORDINATED` | 4 | width < 3 AND p25 ≥ floor AND p75 ≥ real_fee |\n"
        "| `ORGANIC` | 5 | p25 ≥ 2·floor AND p75 ≥ real_fee |\n"
        "| `AMBIGUOUS` | 6 | matched nothing cleanly |\n"
    )
    lines.append(
        "_Config thresholds_: floor_sol=5e-6 (5 µSOL), real_fee_sol=1e-4 (100 µSOL), "
        "min_samples=10, wash_width_max=2, bimodal_width_min=25, coordinated_width_max=3.\n"
    )
    lines.append("")
    lines.append("## Reference tokens\n")
    lines.append(
        "_Populate this section with canonical examples (e.g. Dumpling, Namduong, "
        "Nukita) as validation progresses. Not yet collected in this codebase._\n"
    )
    lines.append("")
    lines.append("## How to fill the worksheet\n")
    lines.append(
        "- `manual_label`: same six names as `classifier_bucket`, or `AMBIGUOUS` if unclear.\n"
        "- `match_yn`: `Y` if your label == classifier, `N` otherwise.\n"
        "- `notes`: one-line justification (what you saw on the chart).\n"
    )
    lines.append("")

    # Coverage warnings
    if insufficient_buckets:
        lines.append("## ⚠ Coverage warnings\n")
        for b, n in insufficient_buckets:
            lines.append(
                f"- **{b}**: only {n} rows in `ante_log` — "
                f"insufficient for precision measurement, need more observation time.\n"
            )
        lines.append("")

    # Per-bucket data
    lines.append("## Samples by bucket\n")
    for b in BUCKETS:
        rows = rows_by_bucket[b]
        pop_ct = pop.get(b, 0)
        lines.append(f"### {b} — {len(rows)} sampled, {pop_ct} total in log\n")
        if not rows:
            lines.append("_No rows in this bucket._\n\n")
            continue
        lines.append(
            "| # | symbol | token | logged_at | chart | med20 µSOL | p25 | p75 | width | n | med5 µSOL | manual | Y/N | notes |\n"
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n"
        )
        for i, r in enumerate(rows, 1):
            (addr, sym, atime, lbl, med20, p25, p75, w20, n20,
             med5, lbl5, rule) = r
            logged = datetime.fromtimestamp(atime, tz=timezone.utc).strftime(
                "%m-%d %H:%M"
            )
            def u(x):
                return "—" if x is None else f"{x*1e6:.1f}"
            lines.append(
                f"| {i} | {sym or '—'} | `{addr[:10]}…` | {logged} "
                f"| [chart](https://dexscreener.com/solana/{addr}) "
                f"| {u(med20)} | {u(p25)} | {u(p75)} "
                f"| {'—' if w20 is None else f'{w20:.1f}'} | {n20} | {u(med5)} "
                f"|  |  |  |\n"
            )
        lines.append("")

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("".join(lines))

    # Console summary
    print(f"[ok] wrote {OUT_CSV}")
    print(f"[ok] wrote {OUT_MD}")
    print()
    print("Bucket | sampled | total_in_log")
    for b in BUCKETS:
        print(f"  {b:14s} | {len(rows_by_bucket[b]):2d} | {pop.get(b, 0):4d}")
    if insufficient_buckets:
        print()
        print("Coverage warnings:")
        for b, n in insufficient_buckets:
            print(f"  {b}: only {n} rows — insufficient for precision")


if __name__ == "__main__":
    main()
