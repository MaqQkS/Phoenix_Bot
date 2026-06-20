"""
PART A - Resanity check the prior systemic finding at $86 SOL.

For all T1 alerts (tier_index=0) in the last 7 days:
1. stored_ATH_mcap = tokens.ath_price * (migration_mcap / migration_price)
2. fee_peak_mcap = MAX(quote_amount/base_amount * 86) * supply,
   restricted to first 60 minutes after MIN(block_time) per pool,
   filtered to that pool only, dust filter quote_amount >= 1_000_000 lamports
3. undershoot_pct = 1 - (stored_ATH_mcap / fee_peak_mcap)
"""

import sqlite3
import time
from statistics import median

SOL_USD = 86.0
DUST_LAMPORTS = 1_000_000  # ~$0.18 at $86 SOL
DB = "data/bot.db"

conn = sqlite3.connect(DB, timeout=60)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

now = time.time()
seven_days_ago = now - 7 * 86400

cur.execute(
    """
    SELECT a.address, a.symbol, a.alert_time, a.alert_mcap, a.tier_index,
           t.migration_price, t.migration_mcap, t.ath_price, t.ath_mcap,
           t.ath_source, t.pool_address, t.migration_time, t.token_decimals
    FROM alerts a
    JOIN tokens t ON t.address = a.address
    WHERE a.tier_index = 0
      AND a.alert_time >= ?
    ORDER BY a.alert_time
    """,
    (seven_days_ago,),
)
alerts = [dict(r) for r in cur.fetchall()]
print(f"T1 alerts last 7d: {len(alerts)}")

results = []
skipped = {"no_supply": 0, "no_pool": 0, "no_fees": 0, "no_peak": 0}

for a in alerts:
    addr = a["address"]
    pool = a["pool_address"]
    mig_p = a["migration_price"]
    mig_m = a["migration_mcap"]
    ath_p = a["ath_price"]

    if not mig_p or mig_p <= 0 or not mig_m or mig_m <= 0:
        skipped["no_supply"] += 1
        continue
    supply = mig_m / mig_p
    if not pool:
        skipped["no_pool"] += 1
        continue

    cur.execute(
        "SELECT MIN(block_time) FROM pumpswap_fees WHERE pool_address = ?", (pool,)
    )
    row = cur.fetchone()
    fee_t0 = row[0] if row else None
    if fee_t0 is None:
        skipped["no_fees"] += 1
        continue

    window_end = fee_t0 + 3600

    cur.execute(
        """
        SELECT MAX(CAST(quote_amount AS REAL)/CAST(base_amount AS REAL)) AS px,
               COUNT(*) AS n_ticks
        FROM pumpswap_fees
        WHERE pool_address = ?
          AND block_time >= ?
          AND block_time <= ?
          AND quote_amount >= ?
          AND base_amount > 0
        """,
        (pool, fee_t0, window_end, DUST_LAMPORTS),
    )
    row = cur.fetchone()
    px_native = row[0]
    n_ticks = row[1]
    if px_native is None or n_ticks == 0:
        skipped["no_peak"] += 1
        continue

    token_decimals = a["token_decimals"] or 6

    # quote_amount is SOL lamports (9 decimals); base_amount in token base units (token_decimals).
    # price (SOL per token) = (quote/1e9) / (base/10^td) = (quote/base) * 10^(td-9)
    decimals_factor = 10 ** (token_decimals - 9)
    price_per_token_sol = px_native * decimals_factor
    price_per_token_usd = price_per_token_sol * SOL_USD
    fee_peak_mcap = price_per_token_usd * supply

    stored_ath_mcap = (ath_p or 0) * supply

    if fee_peak_mcap <= 0:
        skipped["no_peak"] += 1
        continue

    undershoot = 1.0 - (stored_ath_mcap / fee_peak_mcap)

    results.append({
        "address": addr,
        "symbol": a["symbol"],
        "ath_source": a["ath_source"],
        "stored_ath_mcap": stored_ath_mcap,
        "fee_peak_mcap": fee_peak_mcap,
        "undershoot": undershoot,
        "n_ticks": n_ticks,
        "alert_time": a["alert_time"],
    })

print(f"Skipped: {skipped}")
print(f"Computed: {len(results)}")

buckets = {
    ">50% undershoot": 0,
    "30-50% undershoot": 0,
    "10-30% undershoot": 0,
    "-10 to 10% (parity)": 0,
    "overshoot >10%": 0,
}
under_vals = []
for r in results:
    u = r["undershoot"]
    under_vals.append(u)
    if u > 0.50:
        buckets[">50% undershoot"] += 1
    elif u > 0.30:
        buckets["30-50% undershoot"] += 1
    elif u > 0.10:
        buckets["10-30% undershoot"] += 1
    elif u >= -0.10:
        buckets["-10 to 10% (parity)"] += 1
    else:
        buckets["overshoot >10%"] += 1

print("\n--- Distribution ($86 SOL, 1M lamport dust) ---")
total = len(results)
for k, v in buckets.items():
    pct_v = (v / total * 100) if total else 0
    print(f"  {k}: {v} ({pct_v:.1f}%)")


def quantile(vals, p):
    s = sorted(vals)
    if not s:
        return None
    k = (len(s) - 1) * p
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


print(f"\nMedian undershoot: {median(under_vals):.3f}")
print(f"p25 undershoot:    {quantile(under_vals, 0.25):.3f}")
print(f"p75 undershoot:    {quantile(under_vals, 0.75):.3f}")

print("\n--- ath_source for >30% undershoot rows ---")
src_counts = {}
src_total = {}
for r in results:
    src = r["ath_source"] or "unknown"
    src_total[src] = src_total.get(src, 0) + 1
    if r["undershoot"] > 0.30:
        src_counts[src] = src_counts.get(src, 0) + 1
for src in sorted(src_total.keys(), key=lambda s: -src_counts.get(s, 0)):
    bad = src_counts.get(src, 0)
    tot = src_total[src]
    print(f"  {src}: {bad}/{tot} ({bad/tot*100:.1f}% of source)")

print("\n--- Comparison to prior $180 SOL / 10k lamport finding ---")
print("Prior dust filter 10k lamports = ~$0.0009 (very permissive); allowed sub-$0.01 wicks.")
print("Prior SOL=180 inflated fee_peak_mcap by 180/86 = 2.09x.")
print("Net: prior fee_peak_mcap was ~2.1x too high, exaggerating undershoots.")
print("With SOL=86 and dust=1M lamports, fee_peak_mcap is ~2.1x lower, narrowing the gap.")

# Save results for reuse in other parts (in-memory equivalents are fine, but persist to a tmp csv).
import csv
with open("scripts/_investigation/_part_a_rows.csv", "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["address", "symbol", "ath_source", "stored_ath_mcap", "fee_peak_mcap", "undershoot", "n_ticks", "alert_time"])
    for r in results:
        w.writerow([r["address"], r["symbol"], r["ath_source"], f"{r['stored_ath_mcap']:.2f}",
                    f"{r['fee_peak_mcap']:.2f}", f"{r['undershoot']:.4f}", r["n_ticks"], r["alert_time"]])

conn.close()
