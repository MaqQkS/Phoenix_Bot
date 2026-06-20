"""Back out the implied SOL_USD from migration_mcap vs the earliest pumpswap_fees tick."""
import sqlite3, time
DB = "data/bot.db"
conn = sqlite3.connect(DB, timeout=60); conn.row_factory = sqlite3.Row
cur = conn.cursor()

seven_days_ago = time.time() - 7 * 86400
cur.execute("""
    SELECT a.address, a.symbol, t.migration_price, t.migration_mcap,
           t.pool_address, t.token_decimals, t.migration_time
    FROM alerts a JOIN tokens t ON t.address=a.address
    WHERE a.tier_index=0 AND a.alert_time >= ?
    LIMIT 30""", (seven_days_ago,))
alerts = [dict(r) for r in cur.fetchall()]

print(f"{'symbol':<10} {'mig_USD/tok':>14} {'fee_SOL/tok':>14} {'implied_SOL_USD':>18}")
print("-" * 60)
ratios = []
for a in alerts:
    if not a["migration_price"] or not a["pool_address"]: continue
    td = a["token_decimals"] or 6
    cur.execute("""SELECT CAST(quote_amount AS REAL)/CAST(base_amount AS REAL) AS px
                   FROM pumpswap_fees WHERE pool_address=? AND quote_amount > 1000000
                   AND base_amount > 0 ORDER BY block_time ASC LIMIT 1""",
                (a["pool_address"],))
    r = cur.fetchone()
    if not r or r["px"] is None: continue
    fee_sol_per_token = r["px"] * (10 ** (td - 9))
    implied = a["migration_price"] / fee_sol_per_token if fee_sol_per_token > 0 else None
    if implied is None: continue
    ratios.append(implied)
    print(f"{a['symbol'] or '?':<10} {a['migration_price']:>14.2e} {fee_sol_per_token:>14.2e} {implied:>18.2f}")

if ratios:
    ratios.sort()
    print(f"\nmedian implied SOL_USD: ${ratios[len(ratios)//2]:.2f}")
    print(f"min: ${ratios[0]:.2f}  max: ${ratios[-1]:.2f}")
conn.close()
