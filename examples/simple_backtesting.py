import pandas as pd
import yaml
from src.core.data_handler import DataHandler
from src.portfolio.portfolio import Portfolio
from src.logging.trade_logger import TradeLogger
from src.backtest.stress_test import StressTester
from src.backtest.visualizer import Visualizer
from src.backtest.optimizer import ParameterOptimizer
from src.strategies.registry import STRATEGY_REGISTRY
import argparse

def run_backtest_with_params(stress_test: bool = False, strategy_params: dict = None):
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    db_path = config['database']['db_path']
    strategy_config = config['strategy']
    strategy_type = config['strategy']['type']
    portfolio_params = config['portfolio']
    transaction_costs = config['transaction_costs']
    slippage_params = config['slippage']
    backtest_params = config['backtest']
    stress_test_params = config['stress_test'] if stress_test else None
    
    strategy_params = strategy_params or strategy_config.get('params', {})
    if strategy_type not in STRATEGY_REGISTRY:
        raise ValueError(f"Unknown strategy type: {strategy_type}")

    logger = TradeLogger(
        log_file=backtest_params['log_file_path'],
        db_path=db_path,
        strategy_type=strategy_type,
        strategy_config=strategy_params
    )

    dh = DataHandler(
        tradingsymbol=backtest_params['symbol'],
        db_path=db_path,
        table_name=backtest_params['table_name'],
        start_date=backtest_params['start_date'],
        end_date=backtest_params['end_date']
    )

    print("Data summary:")
    print(dh.data.describe())

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

    strategy_class = STRATEGY_REGISTRY[strategy_type]
    strategy = strategy_class(
        data_handler=dh,
        logger=logger,
        **strategy_params
    )

    portfolio = Portfolio(
        initial_cash=portfolio_params['initial_cash'],
        data_handler=dh,
        strategy=strategy,
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
    
    return summary

def main():
    parser = argparse.ArgumentParser(description="Run backtest or optimize parameters")
    parser.add_argument('--optimize', action='store_true', help="Run parameter optimization")
    parser.add_argument('--stress-test', action='store_true', help="Apply stress testing")
    args = parser.parse_args()
    
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    if args.optimize:
        strategy_type = config['strategy']['type']
        if strategy_type not in STRATEGY_REGISTRY:
            raise ValueError(f"Unknown strategy type: {strategy_type}")
        optimizer = ParameterOptimizer(
            backtest_func=run_backtest_with_params,
            strategy_class=STRATEGY_REGISTRY[strategy_type],
            param_grid=config['optimization']['param_grid'],
            metric=config['optimization']['metric'],
            output_dir='reports'
        )
        results = optimizer.optimize()
    else:
        run_backtest_with_params(stress_test=args.stress_test)

if __name__ == "__main__":
    main()