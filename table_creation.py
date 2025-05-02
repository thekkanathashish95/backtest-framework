# import sqlite3
# import yaml

# # Load config
# with open('config/config.yaml', 'r') as f:
#     config = yaml.safe_load(f)
# db_path = config['database']['db_path']

# # Create trade_logs table
# conn = sqlite3.connect(db_path)
# cursor = conn.cursor()
# cursor.execute("""
# CREATE TABLE IF NOT EXISTS trade_logs (
#     log_id INTEGER PRIMARY KEY AUTOINCREMENT,
#     run_id TEXT NOT NULL,
#     timestamp TEXT NOT NULL,
#     log_type TEXT NOT NULL,
#     message TEXT NOT NULL,
#     details JSON
# )
# """)
# conn.commit()
# conn.close()
# print("trade_logs table created successfully")



#DELETE TABLE

import sqlite3
import yaml

# Load config
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)
db_path = config['database']['db_path']

# Create trade_logs table
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("""
DELETE FROM trade_logs
""")
conn.commit()
conn.close()
print("trade_logs deleted successfully")