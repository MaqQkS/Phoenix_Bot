import asyncio
import aiosqlite

async def show():
    async with aiosqlite.connect('data/bot.db') as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT address, symbol, migration_mcap, ath_price, current_mcap, status FROM tokens') as cursor:
            rows = await cursor.fetchall()
            if not rows:
                print('No tokens in DB yet')
            for row in rows:
                print(f"${row['symbol']} | {row['address']} | MigMcap: ${row['migration_mcap']:,.0f} | ATH: {row['ath_price']} | CurrentMcap: ${row['current_mcap']:,.0f} | Status: {row['status']}")

asyncio.run(show())