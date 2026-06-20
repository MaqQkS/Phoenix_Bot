"""
PART C - gRPC ATH primitive sensitivity.

For all T1 alerts, compute fee peak mcap using each primitive, in first 60 minutes
after MIN(block_time):

A: Raw running-max (no dust)
B: Dust-filtered at T = 1M, 5M, 25M lamports
C: K-tick smoothed peak — max V s.t. >=3 ticks within +-5% of V exist within a
   60s sliding window
"""
import sqlite3, time, sys, io
from statistics import median

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

SOL_USD = 86.0
DB = "data/bot.db"

conn = sqlite3.connect(DB, timeout=60); conn.row_factory = sqlite3.Row
cur = conn.cursor()

seven_days_ago = time.time() - 7 * 86400

cur.execute("""
    SELECT a.address, a.symbol, t.migration_price, t.migration_mcap, t.ath_price,
           t.pool_address, t.token_decimals
    FROM alerts a JOIN tokens t ON t.address=a.address
    WHERE a.tier_index=0 AND a.alert_time >= ?""", (seven_days_ago,))
alerts = [dict(r) for r in cur.fetchall()]
print(f"T1 alerts last 7d: {len(alerts)}")


def fee_peak_dust(pool, t0, dust):
    cur.execute(
        """SELECT MAX(CAST(quote_amount AS REAL)/CAST(base_amount AS REAL))
           FROM pumpswap_fees WHERE pool_address=? AND block_time >= ? AND block_time <= ?
             AND quote_amount >= ? AND base_amount > 0""",
        (pool, t0, t0 + 3600, dust))
    return cur.fetchone()[0]


def fee_peak_ksmooth(pool, t0, dust=1_000_000, k=3, win=60.0, tol=0.05):
    """Walk ticks in time order. For each candidate price V, check whether >=k
    ticks within tol of V exist inside a 60s window centered on the candidate.
    Implementation: for each tick (i, t_i, p_i), look at all ticks (j, t_j, p_j)
    with |t_j - t_i| <= win/2 and |p_j - p_i| <= tol*p_i; if count >= k, p_i is
    a confirmed peak candidate; return max over all confirmed candidates.
    """
    cur.execute(
        """SELECT block_time, CAST(quote_amount AS REAL)/CAST(base_amount AS REAL) AS px
           FROM pumpswap_fees WHERE pool_address=? AND block_time >= ? AND block_time <= ?
             AND quote_amount >= ? AND base_amount > 0
           ORDER BY block_time""",
        (pool, t0, t0 + 3600, dust))
    ticks = cur.fetchall()
    if not ticks:
        return None
    n = len(ticks)
    best = None
    half = win / 2.0
    # Two-pointer over the time-ordered ticks.
    j_lo = 0
    j_hi = 0
    for i in range(n):
        t_i = ticks[i]["block_time"]
        p_i = ticks[i]["px"]
        # Advance window
        while j_lo < n and ticks[j_lo]["block_time"] < t_i - half:
            j_lo += 1
        while j_hi < n and ticks[j_hi]["block_time"] <= t_i + half:
            j_hi += 1
        # Count ticks in window within tol of p_i
        lo = p_i * (1 - tol)
        hi = p_i * (1 + tol)
        cnt = 0
        for jj in range(j_lo, j_hi):
            if lo <= ticks[jj]["px"] <= hi:
                cnt += 1
                if cnt >= k:
                    break
        if cnt >= k:
            if best is None or p_i > best:
                best = p_i
    return best


PRIMITIVES = [
    ("A: raw running-max",  "raw"),
    ("B: dust>=1M",         "dust1M"),
    ("B: dust>=5M",         "dust5M"),
    ("B: dust>=25M",        "dust25M"),
    ("C: k=3 +-5% 60s",     "ksmooth"),
]

per_primitive = {p[0]: [] for p in PRIMITIVES}
per_token_peaks = []  # [{address, symbol, peaks: dict, A_peak_mcap, n_ticks_at_A}]

for a in alerts:
    if not a["migration_price"] or not a["migration_mcap"] or not a["pool_address"]:
        continue
    supply = a["migration_mcap"] / a["migration_price"]
    td = a["token_decimals"] or 6
    decimals_factor = 10 ** (td - 9)
    pool = a["pool_address"]

    cur.execute("SELECT MIN(block_time) FROM pumpswap_fees WHERE pool_address=?", (pool,))
    t0 = cur.fetchone()[0]
    if t0 is None:
        continue

    stored = (a["ath_price"] or 0) * supply

    peaks = {}
    raw = fee_peak_dust(pool, t0, 0)
    peaks["A: raw running-max"] = raw
    peaks["B: dust>=1M"]  = fee_peak_dust(pool, t0, 1_000_000)
    peaks["B: dust>=5M"]  = fee_peak_dust(pool, t0, 5_000_000)
    peaks["B: dust>=25M"] = fee_peak_dust(pool, t0, 25_000_000)
    peaks["C: k=3 +-5% 60s"] = fee_peak_ksmooth(pool, t0)

    for label, _ in PRIMITIVES:
        px = peaks[label]
        if px is None or px <= 0:
            continue
        mcap = px * decimals_factor * SOL_USD * supply
        if mcap <= 0:
            continue
        per_primitive[label].append(1.0 - stored / mcap)

    if raw and raw > 0:
        a_mcap = raw * decimals_factor * SOL_USD * supply
        # Count ticks within 5% of A's peak
        cur.execute(
            """SELECT COUNT(*) FROM pumpswap_fees WHERE pool_address=?
               AND block_time >= ? AND block_time <= ?
               AND quote_amount > 0 AND base_amount > 0
               AND ABS(CAST(quote_amount AS REAL)/CAST(base_amount AS REAL) / ? - 1.0) <= 0.05""",
            (pool, t0, t0 + 3600, raw))
        n_within_5pct = cur.fetchone()[0]
        c_px = peaks["C: k=3 +-5% 60s"]
        c_mcap = c_px * decimals_factor * SOL_USD * supply if c_px else None
        per_token_peaks.append({
            "address": a["address"], "symbol": a["symbol"],
            "A_mcap": a_mcap, "C_mcap": c_mcap,
            "n_within_5pct": n_within_5pct, "stored": stored,
        })

# Print primitive table
print()
print(f"{'primitive':<28} {'>50%':>5} {'30-50%':>7} {'10-30%':>7} {'parity':>7} {'over':>5} {'med':>7}")
print("-" * 80)
for label, _ in PRIMITIVES:
    rows = per_primitive[label]
    b = {">50":0, "30-50":0, "10-30":0, "parity":0, "over":0}
    for u in rows:
        if u > 0.50: b[">50"] += 1
        elif u > 0.30: b["30-50"] += 1
        elif u > 0.10: b["10-30"] += 1
        elif u >= -0.10: b["parity"] += 1
        else: b["over"] += 1
    med = median(rows) if rows else 0
    print(f"{label:<28} {b['>50']:>5} {b['30-50']:>7} {b['10-30']:>7} {b['parity']:>7} {b['over']:>5} {med:>7.3f}")

# Top-10 by A peak mcap
print("\n--- Top-10 tokens with largest A (raw) peaks: A vs C, plus tick concentration ---")
top10 = sorted(per_token_peaks, key=lambda x: -x["A_mcap"])[:10]
print(f"{'symbol':<12} {'A_mcap':>14} {'C_mcap':>14} {'A/C ratio':>9} {'#ticks_within_5%':>17}")
for r in top10:
    a_disp = f"${r['A_mcap']:>12,.0f}"
    c_disp = f"${r['C_mcap']:>12,.0f}" if r["C_mcap"] else "N/A".rjust(14)
    ratio = (r["A_mcap"] / r["C_mcap"]) if r["C_mcap"] else float("inf")
    ratio_disp = f"{ratio:>9.2f}" if r["C_mcap"] else "inf".rjust(9)
    sym = (r["symbol"] or "?")[:12]
    print(f"{sym:<12} {a_disp} {c_disp} {ratio_disp} {r['n_within_5pct']:>17}")

print("\nInterpretation: ")
print("  A/C ratio ~1.0 + many ticks within 5% = sustained pump (real peak)")
print("  A/C ratio >>1 + few ticks within 5% = single-tick wick (artifact)")
conn.close()
