import pandas as pd
import numpy as np
from typing import Dict, List, Callable
from itertools import product
from src.backtest.metrics import Metrics
from src.strategies.registry import STRATEGY_REGISTRY
import os
from datetime import datetime
import logging
import yaml

class ParameterOptimizer:
    def __init__(self, backtest_func: Callable, param_grid: Dict[str, List], 
                 metric: str = 'Sharpe Ratio', output_dir: str = 'reports'):
        self.backtest_func = backtest_func
        self.param_grid = param_grid
        self.metric = metric
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self.logger = logging.getLogger('ParameterOptimizer')
        if not self.logger.handlers:
            handler = logging.FileHandler(os.path.join(output_dir, 'optimizer.log'))
            handler.setFormatter(logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s'))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
    def generate_param_combinations(self) -> List[Dict[str, any]]:
        param_names = list(self.param_grid.keys())
        param_values = [self.param_grid[name] for name in param_names]
        combinations = [dict(zip(param_names, values)) for values in product(*param_values)]
        return combinations
    
    def run_single_backtest(self, params: Dict[str, any]) -> Dict:
        """Run a single backtest with given parameters."""
        try:
            # Run backtest with the provided parameters
            summary = self.backtest_func(strategy_params=params)
            metrics = summary['metrics']
            result = {'params': params, **metrics}
            self.logger.info(f"Completed backtest for params {params}: {self.metric} = {metrics.get(self.metric, np.nan):.4f}")
            return result
        except Exception as e:
            self.logger.error(f"Backtest failed for params {params}: {e}")
            return {'params': params, self.metric: np.nan}
    
    def optimize(self) -> pd.DataFrame:
        """Run sequential grid search optimization and return results."""
        self.logger.info("Starting sequential optimization...")
        print("Starting sequential optimization...")
        start_time = datetime.now()
        
        # Generate all parameter combinations
        param_combinations = self.generate_param_combinations()
        print(f"Testing {len(param_combinations)} parameter combinations")
        self.logger.info(f"Testing {len(param_combinations)} parameter combinations")
        
        # Run backtests sequentially
        results = []
        for i, params in enumerate(param_combinations, 1):
            print(f"Running backtest {i}/{len(param_combinations)}: {params}")
            result = self.run_single_backtest(params)
            results.append(result)
        
        # Process results
        results_df = pd.DataFrame([{
            **r['params'],
            self.metric: r.get(self.metric, np.nan),
            **{k: v for k, v in r.items() if k not in ['params', self.metric, 'Max Drawdown Start', 'Max Drawdown End']}
        } for r in results])
        
        # Sort by metric (descending for most metrics, ascending for Max Drawdown)
        ascending = 'Max Drawdown' in self.metric
        results_df = results_df.sort_values(by=self.metric, ascending=ascending)
        
        # Save results
        output_path = os.path.join(self.output_dir, 'optimization_results.csv')
        results_df.to_csv(output_path, index=False)
        print(f"Results saved to {output_path}")
        self.logger.info(f"Results saved to {output_path}")
        
        # Print best parameters
        best_params = results_df.iloc[0][list(self.param_grid.keys())].to_dict()
        best_metric = results_df.iloc[0][self.metric]
        print(f"\nBest Parameters: {best_params}")
        print(f"Best {self.metric}: {best_metric:.4f}")
        self.logger.info(f"Best Parameters: {best_params}")
        self.logger.info(f"Best {self.metric}: {best_metric:.4f}")
        
        end_time = datetime.now()
        print(f"Optimization completed in {end_time - start_time}")
        self.logger.info(f"Optimization completed in {end_time - start_time}")
        
        return results_df