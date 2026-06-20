import sqlite3
c = sqlite3.connect('data/bot.db')

# Exact counts using the current schema
r = c.execute("""
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN total_fee = lp_fee + protocol_fee + creator_fee THEN 1 ELSE 0 END) AS ok_3way,
        SUM(CASE WHEN total_fee = lp_fee + protocol_fee AND creator_fee = 0 THEN 1 ELSE 0 END) AS ok_2way_zero_creator,
        SUM(CASE WHEN total_fee != lp_fee + protocol_fee + creator_fee THEN 1 ELSE 0 END) AS broken
    FROM pumpswap_fees
""").fetchone()
print(f"total:                          {r[0]:>12,}")
print(f"ok 3-way:                       {r[1]:>12,}")
print(f"ok 2-way (creator=0 subset):    {r[2]:>12,}")
print(f"BROKEN (total != 3-way):        {r[3]:>12,}")

# If broken > 0, show 10 samples
if r[3] > 0:
    print("\n=== 10 broken samples ===")
    for row in c.execute("""
        SELECT id, lp_fee, protocol_fee, creator_fee, total_fee,
               (lp_fee + protocol_fee + creator_fee) AS expected
        FROM pumpswap_fees
        WHERE total_fee != lp_fee + protocol_fee + creator_fee
        ORDER BY id DESC LIMIT 10
    """):
        print(f"  id={row[0]} lp={row[1]} proto={row[2]} creator={row[3]} total={row[4]} expected={row[5]} diff={row[5]-row[4]}")

c.close()