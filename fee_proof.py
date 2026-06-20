import sqlite3
c = sqlite3.connect('data/bot.db')

# ── Q1: Row distribution by fee equation match ──
print("=== Q1: Fee equation categories ===")
cats = c.execute("""
    SELECT
        SUM(CASE WHEN total_fee = lp_fee + protocol_fee THEN 1 ELSE 0 END) AS matches_2way,
        SUM(CASE WHEN total_fee = lp_fee + protocol_fee + creator_fee THEN 1 ELSE 0 END) AS matches_3way,
        SUM(CASE WHEN total_fee != lp_fee + protocol_fee 
                  AND total_fee != lp_fee + protocol_fee + creator_fee THEN 1 ELSE 0 END) AS neither,
        COUNT(*) AS total
    FROM pumpswap_fees
""").fetchone()
print(f"  matches 2-way (lp+proto):           {cats[0]:>12,}")
print(f"  matches 3-way (lp+proto+creator):   {cats[1]:>12,}")
print(f"  neither:                            {cats[2]:>12,}")
print(f"  total:                              {cats[3]:>12,}")

# ── Q2: Cross-tab by creator_fee zero/nonzero ──
print("\n=== Q2: Breakdown by creator_fee = 0 vs > 0 ===")
rows = c.execute("""
    SELECT
        CASE WHEN creator_fee = 0 THEN 'creator=0' ELSE 'creator>0' END AS bucket,
        SUM(CASE WHEN total_fee = lp_fee + protocol_fee THEN 1 ELSE 0 END) AS eq_2way,
        SUM(CASE WHEN total_fee = lp_fee + protocol_fee + creator_fee THEN 1 ELSE 0 END) AS eq_3way,
        SUM(CASE WHEN total_fee != lp_fee + protocol_fee 
                  AND total_fee != lp_fee + protocol_fee + creator_fee THEN 1 ELSE 0 END) AS neither,
        COUNT(*) AS total
    FROM pumpswap_fees
    GROUP BY bucket
""").fetchall()
for r in rows:
    print(f"  {r[0]:12s}  2way={r[1]:>10,}  3way={r[2]:>10,}  neither={r[3]:>8,}  total={r[4]:>10,}")

# ── Q3a: 10 samples where total = lp + proto (2-way match) ──
print("\n=== Q3a: 10 samples where total_fee = lp+proto (2-way) ===")
for r in c.execute("""
    SELECT id, lp_fee, protocol_fee, creator_fee, total_fee,
           lp_fee+protocol_fee AS sum2, lp_fee+protocol_fee+creator_fee AS sum3
    FROM pumpswap_fees
    WHERE total_fee = lp_fee + protocol_fee
    ORDER BY id DESC LIMIT 10
"""):
    print(f"  id={r[0]} lp={r[1]} proto={r[2]} creator={r[3]} total={r[4]} sum2={r[5]} sum3={r[6]}")

# ── Q3b: 10 samples where total = lp + proto + creator (3-way match) ──
print("\n=== Q3b: 10 samples where total_fee = lp+proto+creator (3-way) ===")
for r in c.execute("""
    SELECT id, lp_fee, protocol_fee, creator_fee, total_fee,
           lp_fee+protocol_fee AS sum2, lp_fee+protocol_fee+creator_fee AS sum3
    FROM pumpswap_fees
    WHERE total_fee = lp_fee + protocol_fee + creator_fee
      AND creator_fee > 0
    ORDER BY id DESC LIMIT 10
"""):
    print(f"  id={r[0]} lp={r[1]} proto={r[2]} creator={r[3]} total={r[4]} sum2={r[5]} sum3={r[6]}")

# ── Q3c: 10 samples that match NEITHER equation ──
print("\n=== Q3c: 10 samples matching NEITHER equation ===")
for r in c.execute("""
    SELECT id, lp_fee, protocol_fee, creator_fee, total_fee,
           lp_fee+protocol_fee AS sum2, lp_fee+protocol_fee+creator_fee AS sum3
    FROM pumpswap_fees
    WHERE total_fee != lp_fee + protocol_fee
      AND total_fee != lp_fee + protocol_fee + creator_fee
    ORDER BY id DESC LIMIT 10
"""):
    print(f"  id={r[0]} lp={r[1]} proto={r[2]} creator={r[3]} total={r[4]} sum2={r[5]} sum3={r[6]}")

# ── Q4: Timeline — when did 3-way rows start appearing? ──
print("\n=== Q4: Earliest and latest row IDs per category ===")
for label, where in [
    ("2-way only", "total_fee = lp_fee + protocol_fee AND creator_fee = 0"),
    ("2-way but creator>0 (BROKEN)", "total_fee = lp_fee + protocol_fee AND creator_fee > 0"),
    ("3-way correct", "total_fee = lp_fee + protocol_fee + creator_fee AND creator_fee > 0"),
]:
    row = c.execute(f"SELECT MIN(id), MAX(id), COUNT(*) FROM pumpswap_fees WHERE {where}").fetchone()
    print(f"  {label:35s}  min_id={row[0]}  max_id={row[1]}  count={row[2]:>10,}")

c.close()
