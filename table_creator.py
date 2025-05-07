import sqlite3
import os

class TableCreator:
    def __init__(self, db_path: str):
        self.db_path = db_path
        if not os.path.exists(os.path.dirname(db_path)):
            os.makedirs(os.path.dirname(db_path))
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        # Create trades table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                run_id TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                position_id INTEGER,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price REAL NOT NULL,
                value REAL NOT NULL,
                fees REAL NOT NULL,
                net_profit REAL,
                reason TEXT,
                PRIMARY KEY (run_id, trade_id)
            )
        ''')

        # Create trade_logs table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trade_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                log_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                message TEXT NOT NULL,
                details TEXT
            )
        ''')

        # Create portfolio_logs table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS portfolio_logs (
                run_id TEXT NOT NULL,
                date TEXT NOT NULL,
                cash REAL NOT NULL,
                holdings REAL NOT NULL,
                total REAL NOT NULL,
                quantity INTEGER NOT NULL,
                PRIMARY KEY (run_id, date)
            )
        ''')

        # Create runs table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                initiated_time TEXT NOT NULL
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS signal_logs (
                run_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                price REAL NOT NULL,
                rsi REAL,
                signal INTEGER,
                PRIMARY KEY (run_id, timestamp)
            )
        ''')

        # Create metrics table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                run_id TEXT PRIMARY KEY,
                annualized_return REAL,
                max_drawdown REAL,
                max_drawdown_start TEXT,
                max_drawdown_end TEXT,
                win_rate REAL,
                sharpe_ratio REAL,
                sortino_ratio REAL,
                calmar_ratio REAL,
                profit_factor REAL,
                total_trades INTEGER,
                avg_trade_duration REAL,
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
        ''')

        self.conn.commit()

    def drop_tables(self):
        self.cursor.execute('DROP TABLE IF EXISTS trades')
        self.cursor.execute('DROP TABLE IF EXISTS trade_logs')
        self.cursor.execute('DROP TABLE IF EXISTS portfolio_logs')
        self.cursor.execute('DROP TABLE IF EXISTS runs')
        self.cursor.execute('DROP TABLE IF EXISTS signal_logs')
        self.cursor.execute('DROP TABLE IF EXISTS metrics')
        self.conn.commit()

    def query_tables(self):
        try:
            # Execute query
            self.cursor.execute('SELECT * FROM trades LIMIT 10')
            # Fetch column names
            columns = [description[0] for description in self.cursor.description]
            # Fetch all rows
            rows = self.cursor.fetchall()
            # Print results
            if rows:
                print("Trades Table (Top 10 Rows):")
                print(columns)
                for row in rows:
                    print(row)
            else:
                print("No rows found in trades table.")
        except sqlite3.Error as e:
            print(f"Database error: {e}")

    def clear_trades(self, run_id: str):
        self.cursor.execute('DELETE FROM trades WHERE run_id = ?', (run_id,))
        self.conn.commit()

    def clear_logs(self, run_id: str):
        self.cursor.execute('DELETE FROM trade_logs WHERE run_id = ?', (run_id,))
        self.cursor.execute('DELETE FROM portfolio_logs WHERE run_id = ?', (run_id,))
        self.conn.commit()

    def __del__(self):
        self.conn.close()

# Usage
def main():
    table_creator = TableCreator("/Users/ashishmathew/Documents/Development/AlgoTrader/database/algo_data.db")
    # table_creator.drop_tables()
    table_creator.create_tables()
    table_creator.query_tables()

if __name__ == "__main__":
    main()