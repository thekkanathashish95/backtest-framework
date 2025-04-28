from src.core.data_handler import DataHandler
from src.strategies.rsi_strategy import RSIStrategy
from src.portfolio.portfolio import Portfolio
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

# Initialize RSI Strategy
rsi_strategy = RSIStrategy(data_handler=dh, rsi_period=14, overbought=70, oversold=30)
print("\nFetching RSI signals")
signals = rsi_strategy.generate_signals()
print("Head results (RSI and Signals):")
print(signals[['Close', 'RSI', 'Signal']].head(5))
print("Tail results (RSI and Signals):")
print(signals[['Close', 'RSI', 'Signal']].tail(5))
print("Shape results (RSI and Signals):")
print(signals[['Close', 'RSI', 'Signal']].shape)
print("Value count results (RSI and Signals):")
print(signals[['Signal']].value_counts())



# Initialize Portfolio
portfolio = Portfolio(initial_cash=100000, data_handler=dh, strategy=rsi_strategy)
portfolio.execute_trades()
summary = portfolio.get_portfolio_summary()
print("\nPortfolio Summary")
print("Final Cash:", summary['final_cash'])
print("Final Holdings:", summary['final_holdings'])
print("Trades:")
print(summary['trades'])
print("Portfolio Value (last 5):")
print(summary['portfolio_value'].tail(5))
