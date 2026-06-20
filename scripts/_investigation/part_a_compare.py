"""Side-by-side: $86 vs $180 SOL, dust=1M vs dust=10k lamports."""
import sqlite3
import time
from statistics import median

DB = "data/bot.db"
conn = sqlite3.connect(DB, timeout=60)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

now = time.time()
seven_days_ago = now - 7 * 86400

cur.execute(
    """
    SELECT a.address, a.symbol, t.migration_price, t.migration_mcap, t.ath_price,
           t.ath_source, t.pool_address, t.token_decimals
    FROM alerts a JOIN tokens t ON t.address = a.address
    WHERE a.tier_index = 0 AND a.alert_time >= ?
    """, (seven_days_ago,))
alerts = [dict(r) for r in cur.fetchall()]

VARIANTS = [
    ("$86 SOL, dust=1M",   86.0, 1_000_000),
    ("$86 SOL, dust=10k",  86.0, 10_000),
    ("$180 SOL, dust=10k (prior)", 180.0, 10_000),
    ("$180 SOL, dust=1M",   180.0, 1_000_000),
]

print(f"{'variant':<32} {'>50%':>5} {'30-50%':>7} {'10-30%':>7} {'parity':>7} {'over':>5} {'med':>7}")
print("-" * 80)

for name, sol_usd, dust in VARIANTS:
    rows = []
    for a in alerts:
        if not a["migration_price"] or not a["migration_mcap"] or not a["pool_address"]:
            continue
        supply = a["migration_mcap"] / a["migration_price"]
        td = a["token_decimals"] or 6
        cur.execute("SELECT MIN(block_time) FROM pumpswap_fees WHERE pool_address = ?", (a["pool_address"],))
        t0 = cur.fetchone()[0]
        if t0 is None:
            continue
        cur.execute(
            """SELECT MAX(CAST(quote_amount AS REAL)/CAST(base_amount AS REAL))
               FROM pumpswap_fees WHERE pool_address = ?
               AND block_time >= ? AND block_time <= ?
               AND quote_amount >= ? AND base_amount > 0""",
            (a["pool_address"], t0, t0 + 3600, dust))
        px = cur.fetchone()[0]
        if px is None:
            continue
        fee_peak = px * (10 ** (td - 9)) * sol_usd * supply
        stored_ath = (a["ath_price"] or 0) * supply
        if fee_peak <= 0:
            continue
        rows.append(1.0 - stored_ath / fee_peak)

    b = {">50":0, "30-50":0, "10-30":0, "parity":0, "over":0}
    for u in rows:
        if u > 0.50: b[">50"] += 1
        elif u > 0.30: b["30-50"] += 1
        elif u > 0.10: b["10-30"] += 1
        elif u >= -0.10: b["parity"] += 1
        else: b["over"] += 1
    med = median(rows) if rows else 0
    print(f"{name:<32} {b['>50']:>5} {b['30-50']:>7} {b['10-30']:>7} {b['parity']:>7} {b['over']:>5} {med:>7.3f}")

conn.close()
