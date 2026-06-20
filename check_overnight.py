import sqlite3
c = sqlite3.connect('data/bot.db')
print('rows w/ priority data:', c.execute('SELECT COUNT(*) FROM pumpswap_fees WHERE priority_fee IS NOT NULL').fetchone()[0])
print('rows w/ priority_fee > 0:', c.execute('SELECT COUNT(*) FROM pumpswap_fees WHERE priority_fee > 0').fetchone()[0])
print('rows w/ jito_tip > 0:', c.execute('SELECT COUNT(*) FROM pumpswap_fees WHERE jito_tip > 0').fetchone()[0])
print('total rows:', c.execute('SELECT COUNT(*) FROM pumpswap_fees').fetchone()[0])
print('latest row:', c.execute('SELECT datetime(block_time, "unixepoch"), signature FROM pumpswap_fees ORDER BY id DESC LIMIT 1').fetchone())
c.close()