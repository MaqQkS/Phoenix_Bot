"""Top-5 largest gaps: indexer-alive vs pool-quiet, plus bot.log forensics."""
import sqlite3, time, os, re, sys, io
from datetime import datetime, timezone

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DB = "data/bot.db"
conn = sqlite3.connect(DB, timeout=60); conn.row_factory = sqlite3.Row
cur = conn.cursor()

seven_days_ago = time.time() - 7 * 86400

# Re-derive top-5
cur.execute("""
    SELECT a.address, a.symbol, a.alert_time, t.migration_time, t.pool_address, t.ath_source
    FROM alerts a JOIN tokens t ON t.address=a.address
    WHERE a.tier_index=0 AND a.alert_time >= ?""", (seven_days_ago,))
alerts = [dict(r) for r in cur.fetchall()]
rows = []
for a in alerts:
    if not a["pool_address"] or not a["migration_time"]: continue
    cur.execute("SELECT MIN(block_time) FROM pumpswap_fees WHERE pool_address=?", (a["pool_address"],))
    t0 = cur.fetchone()[0]
    if t0 is None: continue
    rows.append({**a, "fee_t0": t0, "gap": t0 - a["migration_time"]})

top5 = sorted(rows, key=lambda x: -x["gap"])[:5]

# Open bot.log once
bot_log_path = "bot.log"
print(f"bot.log size: {os.path.getsize(bot_log_path):,} bytes\n")

print("=" * 80)
for r in top5:
    pool = r["pool_address"]
    addr = r["address"]
    sym = r["symbol"]
    mig = r["migration_time"]
    t0 = r["fee_t0"]

    mig_iso = datetime.fromtimestamp(mig, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    t0_iso = datetime.fromtimestamp(t0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    alert_iso = datetime.fromtimestamp(r["alert_time"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{sym:<10} {addr}")
    print(f"  migration_time: {mig_iso}  fee_t0: {t0_iso}  gap: {r['gap']/3600:.2f}h")
    print(f"  alert_time:     {alert_iso}  ath_source: {r['ath_source']}")

    # 1) Was the indexer alive during the gap?
    cur.execute(
        """SELECT COUNT(*), COUNT(DISTINCT pool_address)
           FROM pumpswap_fees WHERE block_time > ? AND block_time < ?""",
        (mig, t0))
    n_rows_global, n_pools_global = cur.fetchone()
    print(f"  GLOBAL pumpswap_fees rows in gap (any pool): {n_rows_global:,} across {n_pools_global} pools")

    # 2) Slot range for this pool around fee_t0
    cur.execute(
        """SELECT slot, block_time, received_at, event_type, quote_amount, base_amount
           FROM pumpswap_fees WHERE pool_address=?
           ORDER BY block_time ASC LIMIT 3""", (pool,))
    earliest = cur.fetchall()
    print("  Earliest 3 rows for this pool:")
    for er in earliest:
        bt = datetime.fromtimestamp(er["block_time"], tz=timezone.utc).strftime("%H:%M:%S")
        rcv_lag = er["received_at"] - er["block_time"]
        print(f"    slot={er['slot']} block={bt} ev={er['event_type']} q={er['quote_amount']} b={er['base_amount']} recv_lag={rcv_lag:.1f}s")

    # 3) Any rows with received_at near migration_time but block_time later? (subscription happened late)
    cur.execute(
        """SELECT MIN(received_at), MAX(received_at) FROM pumpswap_fees WHERE pool_address=?""",
        (pool,))
    min_recv, max_recv = cur.fetchone()
    if min_recv:
        recv_iso = datetime.fromtimestamp(min_recv, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print(f"  earliest received_at ANY row this pool: {recv_iso}")

    # 4) bot.log forensics — match by address ONLY (symbols collide)
    found = {"migration_ws": None, "birdeye_seed": None, "first_ath": None, "raw_lines": []}
    with open(bot_log_path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            if addr not in line:
                continue
            line = line.strip()
            if found["migration_ws"] is None and ("migration" in line.lower() or "Mint found" in line):
                found["migration_ws"] = line
            if found["birdeye_seed"] is None and "birdeye" in line.lower() and ("seed" in line.lower() or "ATH" in line.lower()):
                found["birdeye_seed"] = line
            if found["first_ath"] is None and ("new ATH" in line or "ATH update" in line):
                found["first_ath"] = line
            if len(found["raw_lines"]) < 30:
                found["raw_lines"].append(line)
    print(f"  bot.log lines mentioning addr: {len(found['raw_lines'])}")
    for rl in found["raw_lines"][:8]:
        print(f"    | {rl[:220]}")
    if found["migration_ws"]:
        print(f"  >> Migration WS: {found['migration_ws'][:220]}")
    if found["birdeye_seed"]:
        print(f"  >> Birdeye seed: {found['birdeye_seed'][:220]}")
    if found["first_ath"]:
        print(f"  >> First ATH:    {found['first_ath'][:220]}")

conn.close()
