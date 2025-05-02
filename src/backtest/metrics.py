import pandas as pd
import numpy as np
from typing import Dict, Tuple

class Metrics:
    def __init__(self, portfolio_value: pd.DataFrame, trades: pd.DataFrame, initial_cash: float):
        """
        Compute performance metrics for a backtest.
        Args:
            portfolio_value: DataFrame with 'Cash', 'Holdings', 'Total' columns.
            trades: DataFrame with 'Date', 'Action', 'Quantity', 'Price', 'Value' columns.
            initial_cash: Initial portfolio cash.
        """
        self.portfolio_value = portfolio_value
        self.trades = trades
        self.initial_cash = initial_cash
        self.returns = self._calculate_returns()

    def _calculate_returns(self) -> pd.Series:
        """
        Calculate 1-minute returns from portfolio value.
        Returns:
            Series of returns.
        """
        total_value = self.portfolio_value['Total']
        returns = total_value.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0)
        return returns

    def annualized_return(self) -> float:
        """
        Calculate annualized return.
        Returns:
            Annualized return as a percentage.
        """
        total_value = self.portfolio_value['Total']
        if len(total_value) < 2:
            return 0.0
        days = (total_value.index[-1] - total_value.index[0]).days
        if days == 0:
            return 0.0
        total_return = (total_value.iloc[-1] / self.initial_cash) - 1
        annualized = ((1 + total_return) ** (252 / days)) - 1
        return annualized * 100

    def max_drawdown(self) -> Tuple[float, pd.Timestamp, pd.Timestamp]:
        """
        Calculate maximum drawdown.
        Returns:
            Tuple of (max drawdown %, start date, end date).
        """
        total_value = self.portfolio_value['Total']
        if total_value.min() <= 0:
            return -100.0, total_value.index[0], total_value.index[-1]
        rolling_max = total_value.cummax()
        drawdowns = (total_value - rolling_max) / rolling_max
        max_dd = drawdowns.min()
        end_date = drawdowns.idxmin()
        start_date = total_value[:end_date].idxmax()
        return max_dd * 100, start_date, end_date

    def win_rate(self) -> float:
        """
        Calculate win rate (percentage of profitable trades).
        Returns:
            Win rate as a percentage.
        """
        if self.trades.empty:
            return 0.0
        profits = []
        for i, trade in self.trades.iterrows():
            if trade['Action'] in ['Sell', 'Cover']:
                entry = self.trades[(self.trades['Date'] < trade['Date']) & 
                                  (self.trades['Action'].isin(['Buy', 'Short']))].iloc[-1]
                if trade['Action'] == 'Sell':
                    profit = (trade['Price'] - entry['Price']) * trade['Quantity']
                else:  # Cover
                    profit = (entry['Price'] - trade['Price']) * trade['Quantity']
                profits.append(profit)
        wins = sum(1 for p in profits if p > 0)
        return (wins / len(profits)) * 100 if profits else 0.0

    def sharpe_ratio(self, risk_free_rate: float = 0.0) -> float:
        """
        Calculate Sharpe ratio.
        Args:
            risk_free_rate: Annual risk-free rate (default: 0.0).
        Returns:
            Sharpe ratio.
        """
        if len(self.returns) < 2:
            return 0.0
        mean_return = self.returns.mean() * 252 * 390  # 390 minutes per trading day
        std_return = self.returns.std() * np.sqrt(252 * 390)
        if std_return == 0 or np.isnan(std_return):
            return 0.0
        return (mean_return - risk_free_rate) / std_return

    def profit_factor(self) -> float:
        """
        Calculate profit factor (gross profits / gross losses).
        Returns:
            Profit factor.
        """
        profits = []
        for i, trade in self.trades.iterrows():
            if trade['Action'] in ['Sell', 'Cover']:
                entry = self.trades[(self.trades['Date'] < trade['Date']) & 
                                  (self.trades['Action'].isin(['Buy', 'Short']))].iloc[-1]
                if trade['Action'] == 'Sell':
                    profit = (trade['Price'] - entry['Price']) * trade['Quantity']
                else:  # Cover
                    profit = (entry['Price'] - trade['Price']) * trade['Quantity']
                profits.append(profit)
        gross_profits = sum(p for p in profits if p > 0)
        gross_losses = -sum(p for p in profits if p < 0)
        return gross_profits / gross_losses if gross_losses > 0 else float('inf')

    def get_metrics(self) -> Dict[str, float]:
        """
        Compute all metrics.
        Returns:
            Dictionary of metric names and values.
        """
        max_dd, dd_start, dd_end = self.max_drawdown()
        return {
            'Annualized Return (%)': self.annualized_return(),
            'Max Drawdown (%)': max_dd,
            'Max Drawdown Start': dd_start,
            'Max Drawdown End': dd_end,
            'Win Rate (%)': self.win_rate(),
            'Sharpe Ratio': self.sharpe_ratio(),
            'Profit Factor': self.profit_factor(),
            'Total Trades': len(self.trades[self.trades['Action'].isin(['Sell', 'Cover'])]),
        }