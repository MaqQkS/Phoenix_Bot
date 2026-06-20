"""
Phase 2 — compute Ante classifier precision from the filled-in worksheet.

Reads diagnostics_out/ANTE_VALIDATION_WORKSHEET.csv after user has filled in
`manual_label` and `match_yn` columns. Produces a per-bucket precision table
and a mismatch report grouped by (classifier, manual) pair to reveal
systematic drift.

Writes: diagnostics_out/ANTE_PRECISION_REPORT.md
"""
import csv
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

IN_CSV = "diagnostics_out/ANTE_VALIDATION_WORKSHEET.csv"
OUT_MD = "diagnostics_out/ANTE_PRECISION_REPORT.md"

VALID_BUCKETS = {"ORGANIC", "WASH_UNIFORM", "BIMODAL", "COORDINATED",
                 "AMBIGUOUS", "INSUFFICIENT"}


def main():
    if not os.path.exists(IN_CSV):
        print(f"[err] {IN_CSV} not found. Run build_ante_worksheet.py first.")
        sys.exit(1)

    with open(IN_CSV, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # Validate: every row needs manual_label and match_yn filled
    missing = [(i, r) for i, r in enumerate(rows, 2)
               if not (r.get("manual_label", "").strip()
                       and r.get("match_yn", "").strip())]
    if missing:
        print(f"[err] {len(missing)} rows missing manual_label or match_yn:")
        for i, r in missing[:10]:
            print(f"  line {i}: {r.get('symbol','?')} "
                  f"({r.get('token_address','?')[:10]}…) "
                  f"classifier={r.get('classifier_bucket','?')} "
                  f"manual={r.get('manual_label','') or '<empty>'} "
                  f"yn={r.get('match_yn','') or '<empty>'}")
        if len(missing) > 10:
            print(f"  ... and {len(missing) - 10} more")
        sys.exit(1)

    # Validate label values
    bad_manual = [r for r in rows
                  if r["manual_label"].strip().upper() not in VALID_BUCKETS]
    if bad_manual:
        print(f"[warn] {len(bad_manual)} rows have manual_label not in {VALID_BUCKETS}:")
        for r in bad_manual[:5]:
            print(f"  {r['symbol']}: '{r['manual_label']}'")

    bad_yn = [r for r in rows
              if r["match_yn"].strip().upper() not in ("Y", "N")]
    if bad_yn:
        print(f"[warn] {len(bad_yn)} rows have match_yn not Y/N:")
        for r in bad_yn[:5]:
            print(f"  {r['symbol']}: '{r['match_yn']}'")

    # Precision per bucket: Y count / total count where classifier==bucket
    by_classifier = defaultdict(list)
    for r in rows:
        by_classifier[r["classifier_bucket"]].append(r)

    precision = {}
    for bucket, items in by_classifier.items():
        y = sum(1 for r in items if r["match_yn"].strip().upper() == "Y")
        n = len(items)
        precision[bucket] = (y, n, y / n if n else 0.0)

    # Mismatches grouped by (classifier, manual)
    mismatches = defaultdict(list)
    for r in rows:
        cls = r["classifier_bucket"]
        man = r["manual_label"].strip().upper()
        if r["match_yn"].strip().upper() == "N":
            mismatches[(cls, man)].append(r)

    # Confusion matrix
    confusion = Counter()
    for r in rows:
        confusion[(r["classifier_bucket"],
                   r["manual_label"].strip().upper())] += 1

    # Build report
    lines = []
    lines.append("# Ante Classifier Precision Report — Phase 2\n")
    lines.append(
        f"_Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}_\n"
    )
    lines.append(f"_Input: {IN_CSV} ({len(rows)} labeled rows)_\n\n")

    # Overall
    total_y = sum(v[0] for v in precision.values())
    total_n = sum(v[1] for v in precision.values())
    lines.append("## Overall\n")
    lines.append(
        f"- Total labeled rows: **{total_n}**\n"
        f"- Matches (classifier == manual): **{total_y}**\n"
        f"- Overall precision: **{total_y/total_n:.1%}**\n\n"
        if total_n else "- No data.\n\n"
    )

    # Per-bucket precision
    lines.append("## Per-bucket precision\n")
    lines.append(
        "| Classifier bucket | Y | Total | Precision | Note |\n"
        "|---|---|---|---|---|\n"
    )
    for b in sorted(precision.keys(),
                    key=lambda k: -precision[k][1]):
        y, n, p = precision[b]
        note = "⚠ low sample (n<10)" if n < 10 else ""
        lines.append(f"| `{b}` | {y} | {n} | {p:.1%} | {note} |\n")
    lines.append("\n")

    # Confusion matrix
    all_labels = sorted(
        set([c for c, _ in confusion.keys()])
        | set([m for _, m in confusion.keys()])
    )
    lines.append("## Confusion matrix (rows=classifier, cols=manual)\n\n")
    lines.append("| classifier \\ manual | " + " | ".join(f"`{l}`" for l in all_labels) + " |\n")
    lines.append("|" + "---|" * (len(all_labels) + 1) + "\n")
    for c in all_labels:
        row_vals = [str(confusion.get((c, m), 0)) for m in all_labels]
        lines.append(f"| `{c}` | " + " | ".join(row_vals) + " |\n")
    lines.append("\n")

    # Systematic drift: (classifier, manual) mismatch pairs
    lines.append("## Systematic drift — mismatch pairs\n")
    if not mismatches:
        lines.append("_No mismatches — classifier agrees with every manual label._\n\n")
    else:
        pairs_sorted = sorted(mismatches.items(), key=lambda kv: -len(kv[1]))
        lines.append(
            "Pairs sorted by frequency. A recurring (classifier → manual) pair "
            "points at a threshold that needs tuning.\n\n"
        )
        for (cls, man), items in pairs_sorted:
            lines.append(
                f"### `{cls}` → `{man}` ({len(items)} mismatch"
                f"{'es' if len(items) != 1 else ''})\n\n"
            )
            lines.append(
                "| symbol | token | med20 µSOL | p25 | p75 | width | n | notes |\n"
                "|---|---|---|---|---|---|---|---|\n"
            )
            for r in items:
                lines.append(
                    f"| {r['symbol'] or '—'} "
                    f"| [`{r['token_address'][:10]}…`]"
                    f"({r['dexscreener_url']}) "
                    f"| {r['median_20sw_usol']} "
                    f"| {r['p25_20sw_usol']} "
                    f"| {r['p75_20sw_usol']} "
                    f"| {r['width_ratio_20sw']} "
                    f"| {r['n_samples_20sw']} "
                    f"| {r.get('notes', '') or ''} |\n"
                )
            lines.append("\n")

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("".join(lines))

    # Console summary
    print(f"[ok] wrote {OUT_MD}")
    print()
    print("Per-bucket precision:")
    for b in sorted(precision.keys(), key=lambda k: -precision[k][1]):
        y, n, p = precision[b]
        print(f"  {b:14s} {y:3d}/{n:3d}  {p:.1%}")
    if mismatches:
        print()
        print("Top mismatch pairs:")
        for (cls, man), items in sorted(
            mismatches.items(), key=lambda kv: -len(kv[1])
        )[:5]:
            print(f"  {cls} -> {man}: {len(items)}")


if __name__ == "__main__":
    main()
