import sqlite3
conn = sqlite3.connect('data/bot.db')
cur = conn.cursor()
cur.execute("SELECT sql FROM sqlite_master WHERE type='table'")
for row in cur.fetchall():
    print(row[0])
    print('---')
conn.close()
