import sqlite3

# Connect to db
conn = sqlite3.connect('/vol_b/data/scrapy_cluster_data/data.db', timeout=30)
c = conn.cursor()

# Print number of rows in db
print(c.execute("""SELECT COUNT(*) FROM dump""").fetchall()[0][0])

# print number of unique crawlid's
print(c.execute("""SELECT count(distinct(crawlid)) FROM dump""").fetchall()[0][0])
c.close()
