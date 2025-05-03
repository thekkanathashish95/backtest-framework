import pandas as pd
import yaml
from src.core.data_handler import DataHandler
from src.strategies.rsi_strategy import RSIStrategy
from src.portfolio.portfolio import Portfolio
from src.logging.trade_logger import TradeLogger
from src.backtest.stress_test import StressTester
from src.backtest.visualizer import Visualizer

def run_backtest(stress_test: bool = False):
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    db_path = config['database']['db_path']
    strategy_params = config['strategy']
    portfolio_params = config['portfolio']
    transaction_costs = config['transaction_costs']
    slippage_params = config['slippage']
    backtest_params = config['backtest']
    stress_test_params = config['stress_test'] if stress_test else None

    logger = TradeLogger(log_file=backtest_params['log_file_path'], db_path=db_path)

    dh = DataHandler(
        tradingsymbol=backtest_params['symbol'],
        db_path=db_path,
        table_name='nifty_50_historic_20240419',
        start_date=backtest_params['start_date'],
        end_date=backtest_params['end_date']
    )

    print("Data summary:")
    print(dh.data.describe())

    # Apply stress tests if enabled
    if stress_test:
        stress_tester = StressTester(dh.data, seed=42)
        dh.data = stress_tester.apply_price_shock(
            shock_factor=stress_test_params['shock_factor'],
            probability=stress_test_params['probability']
        )
        dh.data = stress_tester.apply_liquidity_constraint(
            max_volume_pct=stress_test_params['max_volume_pct']
        )
        print("Applied stress tests: ±10% price shocks (1% probability), 10% volume limit")

    rsi_strategy = RSIStrategy(
        data_handler=dh,
        rsi_period=strategy_params['rsi_period'],
        overbought=strategy_params['overbought'],
        oversold=strategy_params['oversold'],
        wait_period=strategy_params['wait_period'],
        logger=logger
    )

    portfolio = Portfolio(
        initial_cash=portfolio_params['initial_cash'],
        data_handler=dh,
        strategy=rsi_strategy,
        logger=logger,
        transaction_costs=transaction_costs,
        slippage_pct=slippage_params['slippage_pct'],
        buy_cash_pct=portfolio_params['buy_cash_pct'],
        short_cash_pct=portfolio_params['short_cash_pct'],
        stop_loss_pct=portfolio_params['stop_loss_pct'],
        take_profit_pct=portfolio_params['take_profit_pct']
    )

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
    print(summary['trades'])
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