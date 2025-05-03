import pandas as pd
import sqlite3
from pandas import Timestamp

class DataHandler:
    def __init__(self, tradingsymbol: str, db_path: str, table_name: str, start_date: str, end_date: str):
        self.tradingsymbol = tradingsymbol
        self.db_path = db_path
        self.table_name = table_name
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.data = self.load_data()

    def load_data(self) -> pd.DataFrame:
        try:
            conn = sqlite3.connect(self.db_path)
            try:
                query = """
                    SELECT date, open, high, low, close, volume, tradingsymbol
                    FROM {} 
                    WHERE tradingsymbol = ? AND date BETWEEN ? AND ?
                    ORDER BY date
                """.format(self.table_name)
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
                # Validate prices
                if (df['Close'] <= 0).any():
                    raise ValueError(f"Invalid data: {sum(df['Close'] <= 0)} bars with non-positive Close prices")
                if (df['Volume'] < 0).any():
                    raise ValueError(f"Invalid data: {sum(df['Volume'] < 0)} bars with negative Volume")
                df.index = df.index.tz_convert('Asia/Kolkata') if df.index.tz else df.index.tz_localize('Asia/Kolkata')

                # Filter for trading hours (09:15 to 15:30 IST, Monday to Friday)
                df = df[
                    (df.index.time >= pd.Timestamp('09:15').time()) &
                    (df.index.time <= pd.Timestamp('15:30').time()) &
                    (df.index.weekday < 5)
                ]

                # Generate expected index using unique trading days
                unique_dates = df.index.normalize().unique().date
                trading_days = pd.DatetimeIndex(unique_dates).tz_localize('Asia/Kolkata')
                expected_index = pd.DatetimeIndex([])
                for day in trading_days:
                    day_start = pd.Timestamp(day.date()) + pd.Timedelta(hours=9, minutes=15)
                    day_end = pd.Timestamp(day.date()) + pd.Timedelta(hours=15, minutes=30)
                    day_index = pd.date_range(start=day_start, end=day_end, freq='1min', tz='Asia/Kolkata')
                    expected_index = expected_index.union(day_index)

                # Reindex to fill missing bars within trading hours
                missing = expected_index.difference(df.index)
                if len(missing) > 0:
                    print(f"Warning: {len(missing)} missing bars within trading hours detected. Filling with forward-fill.")
                    df = df.reindex(expected_index, method='ffill')
                    # Set Volume to 0 for filled bars
                    df.loc[missing, 'Volume'] = 0
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
        if end_date.tzinfo is None:
            end_date = end_date.tz_localize('Asia/Kolkata')
        else:
            end_date = end_date.tz_convert('Asia/Kolkata')
        return self.data.loc[:end_date - pd.Timedelta(minutes=1)]