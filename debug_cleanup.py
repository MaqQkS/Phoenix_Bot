"""debug_cleanup.py — find out why DELETE matched 0 rows"""
import sqlite3, time

conn = sqlite3.connect("data/bot.db", timeout=30.0)
cur = conn.cursor()

now = time.time()
cutoff = now - (48 * 3600)
print(f"Cutoff: {cutoff} ({time.strftime('%Y-%m-%d %H:%M', time.localtime(cutoff))})")

# Get alerted addresses
alerted = [r[0] for r in cur.execute("SELECT DISTINCT address FROM alerts").fetchall()]
print(f"Alerted: {len(alerted)}")
print(f"First alerted addr: {alerted[0]!r}  (type: {type(alerted[0]).__name__})")

# Sample a row from pumpswap_fees
sample = cur.execute("SELECT token_address, block_time FROM pumpswap_fees LIMIT 1").fetchone()
print(f"Sample fee row: token_address={sample[0]!r}  block_time={sample[1]!r}  (type: {type(sample[1]).__name__})")

# Test 1: Simple WHERE block_time < cutoff (no IN clause)
c1 = cur.execute("SELECT COUNT(*) FROM pumpswap_fees WHERE block_time < ?", (cutoff,)).fetchone()[0]
print(f"\nTest 1 (block_time < cutoff only): {c1:,}")

# Test 2: Full WHERE with NOT IN
placeholders = ",".join("?" for _ in alerted)
q2 = f"SELECT COUNT(*) FROM pumpswap_fees WHERE block_time < ? AND token_address NOT IN ({placeholders})"
c2 = cur.execute(q2, [cutoff] + alerted).fetchone()[0]
print(f"Test 2 (block_time < cutoff AND NOT IN alerted): {c2:,}")

# Test 3: Same but with explicit LIMIT subquery
q3 = f"""SELECT id FROM pumpswap_fees 
WHERE block_time < ? AND token_address NOT IN ({placeholders}) LIMIT 5"""
rows = cur.execute(q3, [cutoff] + alerted).fetchall()
print(f"Test 3 (subquery LIMIT 5): returned {len(rows)} rows")
if rows:
    print(f"  Sample ids: {[r[0] for r in rows]}")

# Test 4: The exact DELETE-style query
q4 = f"""SELECT COUNT(*) FROM pumpswap_fees WHERE id IN (
SELECT id FROM pumpswap_fees WHERE block_time < ? AND token_address NOT IN ({placeholders}) LIMIT 100000
)"""
c4 = cur.execute(q4, [cutoff] + alerted).fetchone()[0]
print(f"Test 4 (id IN subquery): {c4:,}")

conn.close()
