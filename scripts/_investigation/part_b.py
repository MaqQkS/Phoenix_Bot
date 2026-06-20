"""
PART B - Fee t0 gap root cause.

For all T1 alerts in last 7 days:
1. migration_time
2. fee_t0 = MIN(block_time) for that pool from pumpswap_fees
3. gap_seconds = fee_t0 - migration_time

Histogram + top-5 with log forensics.
"""
import sqlite3
import time
from datetime import datetime, timezone

DB = "data/bot.db"
conn = sqlite3.connect(DB, timeout=60)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

now = time.time()
seven_days_ago = now - 7 * 86400

cur.execute(
    """
    SELECT a.address, a.symbol, a.alert_time,
           t.migration_time, t.pool_address, t.ath_source
    FROM alerts a JOIN tokens t ON t.address = a.address
    WHERE a.tier_index = 0 AND a.alert_time >= ?
    """,
    (seven_days_ago,),
)
alerts = [dict(r) for r in cur.fetchall()]
print(f"T1 alerts last 7d: {len(alerts)}")

rows = []
for a in alerts:
    if not a["pool_address"] or not a["migration_time"]:
        continue
    cur.execute(
        "SELECT MIN(block_time) FROM pumpswap_fees WHERE pool_address = ?",
        (a["pool_address"],),
    )
    r = cur.fetchone()
    fee_t0 = r[0] if r else None
    if fee_t0 is None:
        continue
    gap = fee_t0 - a["migration_time"]
    rows.append({
        "address": a["address"],
        "symbol": a["symbol"],
        "migration_time": a["migration_time"],
        "fee_t0": fee_t0,
        "gap": gap,
        "ath_source": a["ath_source"],
        "alert_time": a["alert_time"],
    })

print(f"With pool+migration_time+fees: {len(rows)}")

# Histogram
buckets = [
    ("<0 (fees BEFORE mig)", lambda g: g < 0),
    ("0-60s",                lambda g: 0 <= g < 60),
    ("60-300s",              lambda g: 60 <= g < 300),
    ("300-1200s",            lambda g: 300 <= g < 1200),
    ("1200-3600s",           lambda g: 1200 <= g < 3600),
    (">3600s",               lambda g: g >= 3600),
]
counts = {name: 0 for name, _ in buckets}
for r in rows:
    for name, fn in buckets:
        if fn(r["gap"]):
            counts[name] += 1
            break

print("\n--- Gap histogram (fee_t0 - migration_time) ---")
total = len(rows)
for name, _ in buckets:
    c = counts[name]
    pct = c / total * 100 if total else 0
    print(f"  {name:<22}: {c:>4} ({pct:>5.1f}%)")

# Stats
gaps = sorted(r["gap"] for r in rows)
def q(p):
    k = (len(gaps) - 1) * p
    f = int(k); c = min(f + 1, len(gaps) - 1)
    return gaps[f] + (gaps[c] - gaps[f]) * (k - f)
print(f"\nGap min/p25/median/p75/p95/max: "
      f"{gaps[0]:.0f} / {q(0.25):.0f} / {q(0.5):.0f} / {q(0.75):.0f} / {q(0.95):.0f} / {gaps[-1]:.0f} sec")

# >300s list
big = sorted([r for r in rows if r["gap"] > 300], key=lambda x: -x["gap"])
print(f"\n--- {len(big)} alerts with gap > 300s ---")
print(f"{'address':<46} {'symbol':<14} {'gap_s':>8} {'ath_source':<22} {'alert_UTC':<20}")
for r in big:
    ts = datetime.fromtimestamp(r["alert_time"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{r['address']:<46} {(r['symbol'] or '?')[:14]:<14} {r['gap']:>8.0f} {(r['ath_source'] or '?'):<22} {ts}")

# Top-5 largest positive gaps + slot info
print("\n--- Top-5 largest gaps: indexer-late vs quiet-pool diagnosis ---")
top5 = sorted(rows, key=lambda x: -x["gap"])[:5]
for r in top5:
    pool = None
    cur.execute("SELECT pool_address FROM tokens WHERE address=?", (r["address"],))
    pool = cur.fetchone()[0]
    cur.execute(
        """SELECT COUNT(DISTINCT slot), MIN(slot), MAX(slot)
           FROM pumpswap_fees WHERE pool_address=?
             AND block_time BETWEEN ? AND ?""",
        (pool, r["migration_time"], r["fee_t0"]))
    sc = cur.fetchone()
    n_slots_in_gap, min_slot_gap, max_slot_gap = sc[0], sc[1], sc[2]

    cur.execute(
        """SELECT COUNT(*), MIN(slot), MAX(slot)
           FROM pumpswap_fees WHERE pool_address=? AND block_time <= ?""",
        (pool, r["fee_t0"] + 1))
    early = cur.fetchone()
    n_at_or_before_t0, min_slot, max_slot = early[0], early[1], early[2]

    cur.execute(
        """SELECT slot, block_time, received_at
           FROM pumpswap_fees WHERE pool_address=?
           ORDER BY block_time ASC LIMIT 1""", (pool,))
    first = cur.fetchone()
    first_slot = first["slot"] if first else None
    first_recv = first["received_at"] if first else None

    cur.execute(
        """SELECT COUNT(*) FROM pumpswap_fees
           WHERE pool_address=? AND block_time > ? AND block_time < ?
             AND slot < ?""",
        (pool, r["migration_time"], r["fee_t0"], first_slot if first_slot else 0))
    n_in_gap_pre_first_slot = cur.fetchone()[0]

    mig_utc = datetime.fromtimestamp(r["migration_time"], tz=timezone.utc).strftime("%H:%M:%S")
    t0_utc = datetime.fromtimestamp(r["fee_t0"], tz=timezone.utc).strftime("%H:%M:%S")
    recv_lag = (first_recv - r["fee_t0"]) if first_recv else None

    print(f"\n  {r['symbol']:<12} {r['address']}")
    print(f"    gap: {r['gap']:.0f}s  mig_UTC: {mig_utc}  fee_t0_UTC: {t0_utc}")
    print(f"    distinct slots in (mig, fee_t0): {n_slots_in_gap}")
    print(f"    rows in gap with slot < first_recorded_slot ({first_slot}): {n_in_gap_pre_first_slot}")
    print(f"    first row received_at - block_time: {recv_lag:.1f}s" if recv_lag is not None else "    received_at: n/a")
    print(f"    ath_source: {r['ath_source']}")

conn.close()
