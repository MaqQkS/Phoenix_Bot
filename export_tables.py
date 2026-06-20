import sqlite3
import csv
import os

DB_PATH = "data/bot.db"
OUT_DIR = "db_exports"

TABLES = [
    "pumpswap_fees",
    "fee_gate_log",
    "alerts",
    "tokens",
]

os.makedirs(OUT_DIR, exist_ok=True)

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

for table in TABLES:
    rows = cur.execute(f"SELECT * FROM {table}").fetchall()
    out_path = os.path.join(OUT_DIR, f"{table}.csv")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows([dict(r) for r in rows])
        else:
            f.write("")

    print(f"Exported {table} -> {out_path} ({len(rows)} rows)")

conn.close()