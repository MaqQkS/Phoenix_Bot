"""
scripts/retro_ante_report.py — Partial-Ante audit over historical pumpswap_fees.

Because base_fee is forward-only, "partial Ante" here = priority_fee + jito_tip
per distinct-signature tx. This is still the larger portion of Ante — base_fee
on a single-sig swap is only 5000 lamports (~0.000005 SOL) vs priority + jito
which regularly range from dust to 0.1+ SOL per swap.

Reports produced:
  1. Top-N tokens by swap count, with per-token partial-Ante quartiles
  2. Overall partial-Ante distribution across all sampled swaps
  3. Breakdown by manual_verdict across all four shadow-log tables
     (fee_gate_log, lp_floor_log, stillborn_log, inspection_gate_log).
     Silently skipped if no manual_verdict rows exist.

Read-only against data/bot.db — cannot mutate.

Usage:
    python scripts/retro_ante_report.py                   # top 20 tokens
    python scripts/retro_ante_report.py --top 50          # top 50
    python scripts/retro_ante_report.py --per-token-limit 50000   # more rows per token
"""
import argparse
import sqlite3
import statistics
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = str(_REPO_ROOT / "data" / "bot.db")
LAMPORTS = 1_000_000_000.0

SHADOW_TABLES = [
    "fee_gate_log",
    "lp_floor_log",
    "stillborn_log",
    "inspection_gate_log",
]


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


def _fmt(v):
    if v is None:
        return "      n/a"
    return f"{v:>9.6f}"


def _safe_sym(sym: str | None) -> str:
    """Strip glyphs the current stdout can't encode. Windows cp1252 is picky."""
    if not sym:
        return "???"
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        sym.encode(enc)
        return sym
    except UnicodeEncodeError:
        return sym.encode(enc, errors="replace").decode(enc, errors="replace")


def _table_exists(conn, name):
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def run(top_n: int, per_token_limit: int):
    t0 = time.time()
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()

    # ── Section 1: top-N tokens by swap count with partial-Ante quartiles ──
    print(f"Finding top {top_n} tokens by distinct-sig swap count...")
    cur.execute("""
        SELECT token_address, COUNT(*) AS cnt
        FROM pumpswap_fees
        WHERE priority_fee IS NOT NULL
          AND token_address IS NOT NULL
        GROUP BY token_address
        ORDER BY cnt DESC
        LIMIT ?
    """, (top_n,))
    top = cur.fetchall()

    print(f"\n== Top {len(top)} tokens by swap count (partial Ante = priority + jito) ==\n")
    header = (
        f"{'token':14} {'symbol':10} {'swaps':>8}  "
        f"{'p25 SOL':>9} {'med SOL':>9} {'p75 SOL':>9}  "
        f"{'mean SOL':>10} {'max SOL':>10}"
    )
    print(header)
    print("-" * len(header))

    all_samples = []

    for token_addr, cnt in top:
        sym_row = cur.execute(
            "SELECT symbol FROM tokens WHERE address = ?", (token_addr,)
        ).fetchone()
        sym = _safe_sym((sym_row[0] if sym_row and sym_row[0] else "???"))[:10]

        cur.execute("""
            SELECT COALESCE(priority_fee,0) + COALESCE(jito_tip,0)
            FROM pumpswap_fees
            WHERE token_address = ?
              AND priority_fee IS NOT NULL
            ORDER BY block_time DESC
            LIMIT ?
        """, (token_addr, per_token_limit))
        vals_lamp = [r[0] for r in cur.fetchall()]
        if not vals_lamp:
            continue

        vals_sol = [v / LAMPORTS for v in vals_lamp]
        all_samples.extend(vals_sol)
        p25, med, p75 = _p25_median_p75(vals_sol)
        mean_sol = sum(vals_sol) / len(vals_sol)
        max_sol = max(vals_sol)

        print(
            f"{token_addr[:14]:14} {sym:10} {cnt:>8}  "
            f"{_fmt(p25)} {_fmt(med)} {_fmt(p75)}  "
            f"{mean_sol:>10.6f} {max_sol:>10.6f}"
        )

    # ── Section 2: overall distribution across sampled tokens ─────────────
    if all_samples:
        p25, med, p75 = _p25_median_p75(all_samples)
        print(
            f"\n== Overall across {len(all_samples):,} sampled swaps "
            f"({len(top)} tokens × up to {per_token_limit:,}/token) =="
        )
        print(f"  partial Ante: p25 {p25:.6f}  med {med:.6f}  p75 {p75:.6f} SOL")
        print(f"  mean: {sum(all_samples)/len(all_samples):.6f} SOL  "
              f"max: {max(all_samples):.6f} SOL")

    # ── Section 3: breakdown by manual_verdict ─────────────────────────────
    print("\n== Manual verdict breakdown ==")
    verdict_found_any = False

    for tbl in SHADOW_TABLES:
        if not _table_exists(conn, tbl):
            continue
        try:
            rows = cur.execute(
                f"""SELECT token_address, manual_verdict
                    FROM {tbl}
                    WHERE manual_verdict IS NOT NULL AND manual_verdict != ''"""
            ).fetchall()
        except sqlite3.OperationalError:
            # Table exists but has no manual_verdict column
            continue
        if not rows:
            continue
        verdict_found_any = True
        print(f"\n  {tbl} — {len(rows)} labeled rows")

        # Group by verdict, then fetch per-token Ante samples and aggregate
        by_verdict: dict[str, list[float]] = {}
        for token_addr, verdict in rows:
            cur.execute("""
                SELECT COALESCE(priority_fee,0) + COALESCE(jito_tip,0)
                FROM pumpswap_fees
                WHERE token_address = ? AND priority_fee IS NOT NULL
                ORDER BY block_time DESC LIMIT ?
            """, (token_addr, per_token_limit))
            for (v,) in cur.fetchall():
                by_verdict.setdefault(verdict, []).append(v / LAMPORTS)

        sub_header = f"    {'verdict':24} {'tokens':>7} {'swaps':>9} {'p25':>9} {'med':>9} {'p75':>9}"
        print(sub_header)
        print("    " + "-" * (len(sub_header) - 4))
        by_verdict_tokens = {
            v: len({a for a, vv in rows if vv == v}) for v in by_verdict
        }
        for verdict in sorted(by_verdict.keys()):
            vals = by_verdict[verdict]
            if not vals:
                continue
            p25, med, p75 = _p25_median_p75(vals)
            print(
                f"    {verdict[:24]:24} "
                f"{by_verdict_tokens.get(verdict, 0):>7} "
                f"{len(vals):>9} "
                f"{p25:>9.6f} {med:>9.6f} {p75:>9.6f}"
            )

    if not verdict_found_any:
        print("  (no manual_verdict rows in any shadow log — section skipped)")

    conn.close()
    print(f"\nDone in {time.time() - t0:.1f}s")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--top", type=int, default=20,
                   help="Top N tokens by swap count (default 20)")
    p.add_argument("--per-token-limit", type=int, default=10000,
                   help="Max most-recent rows to sample per token (default 10000)")
    args = p.parse_args()
    run(args.top, args.per_token_limit)


if __name__ == "__main__":
    main()
