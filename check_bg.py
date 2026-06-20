import sqlite3
c = sqlite3.connect('data/bot.db')

print("=== Total bundle_gate rows ===")
print(c.execute("SELECT COUNT(*) FROM bundle_gate_log").fetchone())

print("\n=== Row for this alert's token ===")
rows = list(c.execute(
    "SELECT label, buy_usd, check_started_at FROM bundle_gate_log WHERE token_address = ?",
    ('3DATMNU4c9ucp1eDx5CukDZXdv8qyUBPJEeehVLBpump',)
))
print(rows if rows else "NO ROW — token not in bundle_gate_log")

print("\n=== Latest 5 rows (any token) ===")
for r in c.execute("SELECT symbol, label, buy_usd, check_started_at FROM bundle_gate_log ORDER BY id DESC LIMIT 5"):
    print(r)

c.close()