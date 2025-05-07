import pandas as pd
import numpy as np
from typing import Dict, Tuple

class Metrics:
    def __init__(self, portfolio_value: pd.DataFrame, trades: pd.DataFrame, initial_cash: float):
        """
        Compute performance metrics for a backtest.
        Args:
            portfolio_value: DataFrame with 'Cash', 'Holdings', 'Total' columns.
            trades: DataFrame with 'Date', 'Action', 'Quantity', 'Price', 'Value', 'Fees', 'NetProfit' columns.
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
        # Consider only exit trades (Sell or Cover) with non-null NetProfit
        exit_trades = self.trades[self.trades['Action'].isin(['Sell', 'Cover']) & self.trades['NetProfit'].notnull()]
        if exit_trades.empty:
            return 0.0
        wins = (exit_trades['NetProfit'] > 0).sum()
        return (wins / len(exit_trades)) * 100

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
        mean_return = self.returns.mean() * 252 * 390
        std_return = self.returns.std() * np.sqrt(252 * 390)
        if std_return == 0 or np.isnan(std_return):
            return 0.0
        return (mean_return - risk_free_rate) / std_return

    def sortino_ratio(self, risk_free_rate: float = 0.0) -> float:
        """
        Calculate Sortino ratio (downside risk only).
        Args:
            risk_free_rate: Annual risk-free rate (default: 0.0).
        Returns:
            Sortino ratio.
        """
        if len(self.returns) < 2:
            return 0.0
        mean_return = self.returns.mean() * 252 * 390
        downside_returns = self.returns[self.returns < 0]
        downside_std = downside_returns.std() * np.sqrt(252 * 390) if not downside_returns.empty else 0.0
        if downside_std == 0 or np.isnan(downside_std):
            return 0.0
        return (mean_return - risk_free_rate) / downside_std

    def calmar_ratio(self) -> float:
        """
        Calculate Calmar ratio (annualized return / max drawdown).
        Returns:
            Calmar ratio.
        """
        annualized_ret = self.annualized_return() / 100
        max_dd, _, _ = self.max_drawdown()
        max_dd = abs(max_dd / 100)  # Convert to positive fraction
        if max_dd == 0 or np.isnan(max_dd):
            return 0.0
        return annualized_ret / max_dd

    def profit_factor(self) -> float:
        """
        Calculate profit factor (gross profits / gross losses).
        Returns:
            Profit factor.
        """
        if self.trades.empty:
            return 0.0
        # Consider only exit trades (Sell or Cover) with non-null NetProfit
        exit_trades = self.trades[self.trades['Action'].isin(['Sell', 'Cover']) & self.trades['NetProfit'].notnull()]
        if exit_trades.empty:
            return 0.0
        gross_profits = exit_trades[exit_trades['NetProfit'] > 0]['NetProfit'].sum()
        gross_losses = -exit_trades[exit_trades['NetProfit'] < 0]['NetProfit'].sum()
        return gross_profits / gross_losses if gross_losses > 0 else float('inf')

    def avg_trade_duration(self) -> float:
        """
        Calculate average trade duration in minutes.
        Returns:
            Average trade duration in minutes.
        """
        if self.trades.empty:
            return 0.0
        # Pair entry and exit trades by position_id
        entry_actions = ['Buy', 'Short']
        exit_actions = ['Sell', 'Cover']
        durations = []
        for position_id in self.trades['position_id'].unique():
            position_trades = self.trades[self.trades['position_id'] == position_id].sort_values('Date')
            entry_trades = position_trades[position_trades['Action'].isin(entry_actions)]
            exit_trades = position_trades[position_trades['Action'].isin(exit_actions)]
            if not entry_trades.empty and not exit_trades.empty:
                entry_time = entry_trades['Date'].iloc[0]
                exit_time = exit_trades['Date'].iloc[-1]
                duration = (exit_time - entry_time).total_seconds() / 60.0
                durations.append(duration)
        return np.mean(durations) if durations else 0.0

    def get_metrics(self) -> Dict[str, any]:
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
            'Sortino Ratio': self.sortino_ratio(),
            'Calmar Ratio': self.calmar_ratio(),
            'Profit Factor': self.profit_factor(),
            'Total Trades': len(self.trades[self.trades['Action'].isin(['Sell', 'Cover'])]),
            'Avg Trade Duration (min)': self.avg_trade_duration(),
        }