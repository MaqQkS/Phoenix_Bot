# check_trumbull.py
import sqlite3
c = sqlite3.connect('data/bot.db')
# get token address
for r in c.execute("SELECT address FROM tokens WHERE symbol='TRUMBULL'"):
    print("address:", r[0])
    addr = r[0]
# get all its pumpswap_fees
for r in c.execute("SELECT slot, event_type, quote_amount/1e9 FROM pumpswap_fees WHERE token_address=? ORDER BY slot LIMIT 30", (addr,)):
    print(r)