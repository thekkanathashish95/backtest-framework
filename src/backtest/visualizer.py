import pandas as pd
import matplotlib.pyplot as plt
import os

class Visualizer:
    def __init__(self, portfolio_value: pd.DataFrame, output_dir: str = "reports"):
        """
        Visualize backtest results.
        Args:
            portfolio_value: DataFrame with 'Cash', 'Holdings', 'Total' columns.
            output_dir: Directory to save plots.
        """
        self.portfolio_value = portfolio_value
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def plot_equity_curve(self):
        """
        Plot the equity curve (portfolio total value).
        """
        plt.figure(figsize=(12, 6))
        plt.plot(self.portfolio_value.index, self.portfolio_value['Total'], label='Portfolio Value')
        plt.title('Equity Curve')
        plt.xlabel('Date')
        plt.ylabel('Portfolio Value ($)')
        plt.grid(True)
        plt.legend()
        plt.savefig(os.path.join(self.output_dir, 'equity_curve.png'))
        plt.close()

    def plot_drawdowns(self):
        """
        Plot drawdowns over time.
        """
        total_value = self.portfolio_value['Total']
        rolling_max = total_value.cummax()
        drawdowns = (total_value - rolling_max) / rolling_max * 100
        plt.figure(figsize=(12, 6))
        plt.plot(drawdowns.index, drawdowns, label='Drawdown (%)', color='red')
        plt.title('Drawdowns')
        plt.xlabel('Date')
        plt.ylabel('Drawdown (%)')
        plt.grid(True)
        plt.legend()
        plt.savefig(os.path.join(self.output_dir, 'drawdowns.png'))
        plt.close()