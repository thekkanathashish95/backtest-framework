import pandas as pd
import sqlite3
import yaml
from pathlib import Path
from pandas import Timestamp

class DataHandler:
    def __init__(self, tradingsymbol: str, config_path: str, table_name: str, start_date: str, end_date: str):
        self.tradingsymbol = tradingsymbol
        self.table_name = table_name
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        # Load db_path from config.yaml
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.db_path = config['database']['db_path']
        self.data = self.load_data()

    def load_data(self) -> pd.DataFrame:
        try:
            # Connect to SQLite database
            conn = sqlite3.connect(self.db_path)
            try:
                # Query with tradingsymbol filter
                query = f"""
                    SELECT date, open, high, low, close, volume, tradingsymbol
                    FROM {self.table_name}
                    WHERE tradingsymbol = ? AND date BETWEEN ? AND ?
                    ORDER BY date
                """
                # Convert timestamps to SQLite-compatible string format
                start_date_str = self.start_date.strftime('%Y-%m-%d %H:%M:%S')
                end_date_str = self.end_date.strftime('%Y-%m-%d %H:%M:%S')
                df = pd.read_sql_query(
                    query,
                    conn,
                    params=(self.tradingsymbol, start_date_str, end_date_str),
                    index_col='date',
                    parse_dates=['date']
                )
                # Check if data is empty
                if df.empty:
                    raise ValueError(f"No data found for tradingsymbol '{self.tradingsymbol}' in table '{self.table_name}' "
                                     f"between {self.start_date} and {self.end_date}")
                # Validate required columns
                expected_columns = ['open', 'high', 'low', 'close', 'volume', 'tradingsymbol']
                if not all(col in df.columns for col in expected_columns):
                    raise ValueError(f"Data must contain columns: {expected_columns}")
                # Rename columns to standard OHLCV format
                df = df.rename(columns={
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume',
                    'tradingsymbol': 'TradingSymbol'
                })
                # Ensure the index is timezone-aware (Asia/Kolkata)
                df.index = df.index.tz_convert('Asia/Kolkata') if df.index.tz else df.index.tz_localize('Asia/Kolkata')
                return df
            except sqlite3.OperationalError as e:
                if "no such table" in str(e).lower():
                    raise ValueError(f"Table '{self.table_name}' does not exist in database '{self.db_path}'") from e
                raise
            finally:
                conn.close()
        except sqlite3.DatabaseError as e:
            raise ValueError(f"Failed to connect to database '{self.db_path}': {str(e)}") from e

    def get_historical_data(self, end_date: pd.Timestamp) -> pd.DataFrame:
        # Ensure end_date is timezone-aware (Asia/Kolkata)
        if end_date.tzinfo is None:
            end_date = end_date.tz_localize('Asia/Kolkata')
        else:
            end_date = end_date.tz_convert('Asia/Kolkata')
        # Return data up to end_date (exclusive) for 1-minute data
        return self.data.loc[:end_date - pd.Timedelta(minutes=1)]