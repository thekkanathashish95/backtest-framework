from src.core.data_handler import DataHandler
import pandas as pd

# Example usage
dh = DataHandler(
    tradingsymbol='ADANIENT',
    config_path='config/config.yaml',
    table_name='nifty_50_historic_20240419',
    start_date='2025-02-20 09:15:00',
    end_date='2025-04-17 15:29:00'
)
print("Full data")
print("Head results:")
print(dh.data.head(1))
print("Tail results:")
print(dh.data.tail(1))
print("Fetching historical data")
historical_data = dh.get_historical_data(pd.to_datetime('2025-03-03 15:30:00'))
print("Head results:")
print(historical_data.head(1))
print("Tail results:")
print(historical_data.tail(1))