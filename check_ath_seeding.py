"""
check_ath_seeding.py — Diagnose late/missing ATH seeding.
Run from project root:  python check_ath_seeding.py
"""

import sqlite3
import time

DB_PATH = "data/bot.db"

conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

now = time.time()
day_ago = now - 86400

# Pull all tokens from last 24h missing ATH
rows = cur.execute("""
    SELECT symbol, address, migration_time, current_mcap, ath_mcap
    FROM tokens
    WHERE migration_time >= ?
      AND (ath_mcap IS NULL OR ath_mcap = 0)
    ORDER BY migration_time DESC
""", (day_ago,)).fetchall()

print(f"\n{'='*80}")
print(f"Tokens missing ATH in last 24h: {len(rows)}")
print(f"{'='*80}\n")

if not rows:
    print("✅ None missing — no issue.")
    conn.close()
    exit()

# Bucket by age
buckets = {
    "0-30 min (benign, just migrated)": [],
    "30-120 min (concerning)": [],
    "2-6 hours (real bug)": [],
    "6+ hours (permanently skipped)": [],
}

for r in rows:
    age_min = (now - r["migration_time"]) / 60
    record = {
        "symbol": r["symbol"],
        "age_min": age_min,
        "current_mcap": r["current_mcap"] or 0,
    }
    if age_min <= 30:
        buckets["0-30 min (benign, just migrated)"].append(record)
    elif age_min <= 120:
        buckets["30-120 min (concerning)"].append(record)
    elif age_min <= 360:
        buckets["2-6 hours (real bug)"].append(record)
    else:
        buckets["6+ hours (permanently skipped)"].append(record)

for bucket_name, items in buckets.items():
    print(f"── {bucket_name}: {len(items)} ──")
    for item in items[:10]:
        print(f"   {item['symbol']:12s}  age={item['age_min']:6.1f} min  "
              f"mc=${item['current_mcap']:,.0f}")
    if len(items) > 10:
        print(f"   ... and {len(items) - 10} more")
    print()

# Check for clustering — were they migrated in bursts?
print("── Migration clustering (5-min windows) ──")
cur.execute("""
    SELECT
        CAST(migration_time / 300 AS INTEGER) * 300 as window,
        COUNT(*) as total,
        SUM(CASE WHEN ath_mcap IS NULL OR ath_mcap = 0 THEN 1 ELSE 0 END) as missing
    FROM tokens
    WHERE migration_time >= ?
    GROUP BY window
    HAVING total >= 3
    ORDER BY missing DESC
    LIMIT 10
""", (day_ago,))

cluster_rows = cur.fetchall()
if cluster_rows:
    print(f"{'Time':20s}  {'Total':>6s}  {'Missing':>8s}  {'% Missing':>10s}")
    for r in cluster_rows:
        ts = time.strftime("%m-%d %H:%M", time.localtime(r["window"]))
        pct = (r["missing"] / r["total"]) * 100
        print(f"{ts:20s}  {r['total']:>6d}  {r['missing']:>8d}  {pct:>9.0f}%")

conn.close()
print()