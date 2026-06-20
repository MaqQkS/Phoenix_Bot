import sqlite3
c = sqlite3.connect('data/bot.db')
for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    print(r[0])
c.close()