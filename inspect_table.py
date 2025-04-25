import sqlite3

db_path = "/Users/ashishmathew/Documents/Development/AlgoTrader/database/algo_data.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(nifty_50_historic_20240419)")
columns = cursor.fetchall()
print("Columns in nifty_50_historic_20240419:")
for col in columns:
    print(f"Name: {col[1]}, Type: {col[2]}")
conn.close()