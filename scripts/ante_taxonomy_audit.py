"""
Audit Ante taxonomy distribution over last 7 days.
"""
import asyncio
import aiosqlite

async def main():
    async with aiosqlite.connect("file:data/bot.db?mode=ro", uri=True) as db:
        print("\n=== 5m window distribution ===")
        async with db.execute("""
            SELECT label_5m, COUNT(*) AS n
            FROM ante_log
            WHERE alert_time > unixepoch('now','-7 days')
            GROUP BY label_5m
            ORDER BY n DESC
        """) as cur:
            async for row in cur:
                print(f"  {row[0]:<15} {row[1]}")

        print("\n=== 20sw window distribution ===")
        async with db.execute("""
            SELECT label_20sw, COUNT(*) AS n
            FROM ante_log
            WHERE alert_time > unixepoch('now','-7 days')
            GROUP BY label_20sw
            ORDER BY n DESC
        """) as cur:
            async for row in cur:
                print(f"  {row[0]:<15} {row[1]}")

        print("\n=== Window disagreement rate ===")
        async with db.execute("""
            SELECT
              SUM(CASE WHEN label_5m != label_20sw THEN 1 ELSE 0 END) AS disagree,
              COUNT(*) AS total
            FROM ante_log
            WHERE alert_time > unixepoch('now','-7 days')
              AND label_5m IS NOT NULL AND label_20sw IS NOT NULL
        """) as cur:
            row = await cur.fetchone()
            if row and row[1]:
                print(f"  {row[0]}/{row[1]} ({100*row[0]/row[1]:.1f}%)")

        print("\n=== Rule 6 (AMBIGUOUS) rate — calibration signal ===")
        async with db.execute("""
            SELECT
              SUM(CASE WHEN rule_hit_5m = 6 THEN 1 ELSE 0 END) AS amb_5m,
              SUM(CASE WHEN rule_hit_20sw = 6 THEN 1 ELSE 0 END) AS amb_20sw,
              COUNT(*) AS total
            FROM ante_log
            WHERE alert_time > unixepoch('now','-7 days')
        """) as cur:
            row = await cur.fetchone()
            if row:
                print(f"  5m:   {row[0]}/{row[2]}")
                print(f"  20sw: {row[1]}/{row[2]}")

if __name__ == "__main__":
    asyncio.run(main())
