import pandas as pd
import numpy as np
from src.core.data_handler import DataHandler
from src.strategies.rsi_strategy import RSIStrategy
from src.portfolio.portfolio import Portfolio
from src.logging.trade_logger import TradeLogger
from src.backtest.stress_test import StressTester
from src.backtest.visualizer import Visualizer
import yaml

def run_backtest(stress_test: bool = False):
    """
    Run backtest with optional stress testing.
    Args:
        stress_test: If True, apply price shocks and liquidity constraints.
    """
    # Load config
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    db_path = config['database']['db_path']

    # Initialize logger
    logger = TradeLogger(
        log_file='logs/trade.log',
        db_path=db_path,
    )

    # Initialize data handler
    dh = DataHandler(
        tradingsymbol='ADANIENT',
        config_path='config/config.yaml',
        table_name='nifty_50_historic_20240419',
        start_date='2025-02-20 09:15:00',
        end_date='2025-04-17 15:29:00'
    )

    print("Data summary:")
    print(dh.data.describe())

    # Apply stress tests if enabled
    if stress_test:
        stress_tester = StressTester(dh.data, seed=42)
        dh.data = stress_tester.apply_price_shock(shock_factor=0.1, probability=0.01)
        dh.data = stress_tester.apply_liquidity_constraint(max_volume_pct=0.1)
        print("Applied stress tests: ±10% price shocks (1% probability), 10% volume limit")

    # Initialize RSI Strategy (Situation 3: base_overbought=60, base_oversold=30)
    rsi_strategy = RSIStrategy(data_handler=dh, rsi_period=14, overbought=60, oversold=30, wait_period=5, logger=logger)

    # Initialize Portfolio
    portfolio = Portfolio(initial_cash=100000, data_handler=dh, strategy=rsi_strategy, logger=logger)

    print("\nProcessing bars...")
    last_printed_date = None
    for i, date in enumerate(dh.data.index):
        portfolio.process_bar(date)
        if logger:
            logger._log("DEBUG", f"Processed bar at {date}", date, {})

        # Check if this is the last bar of the day (15:30 IST) or the date has changed
        current_date = date.date()
        is_last_bar_of_day = (date.time().hour == 15 and date.time().minute == 30) or (i == len(dh.data.index) - 1)
        if is_last_bar_of_day and last_printed_date != current_date:
            portfolio_value = portfolio.portfolio_value.loc[date, 'Total']
            print(f"End of day {current_date}: Portfolio Value = ${portfolio_value:.2f}")
            last_printed_date = current_date

    summary = portfolio.get_portfolio_summary()
    print("\nPortfolio Summary")
    print("Final Cash:", summary['final_cash'])
    print("Final Holdings:", summary['final_holdings'])
    print("Trades:")
    print(summary['trades'].shape[0])
    print("Trade Counts by Action:")
    print(summary['trades']['Action'].value_counts())
    print("Portfolio Value (last 5):")
    print(summary['portfolio_value'].tail(5))

    # Visualize results
    visualizer = Visualizer(portfolio.portfolio_value)
    visualizer.plot_equity_curve()
    visualizer.plot_drawdowns()
    print("Plots saved to reports/")

    # Close logger
    logger.close()

if __name__ == "__main__":
    run_backtest(stress_test=False)