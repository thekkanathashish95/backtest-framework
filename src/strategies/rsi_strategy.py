import pandas as pd
import numpy as np
from src.strategies.base_strategy import BaseStrategy
from src.logging.trade_logger import TradeLogger

class RSIStrategy(BaseStrategy):
    def __init__(self, data_handler, rsi_period: int = 14, overbought: float = 70, oversold: float = 30, logger: TradeLogger = None):
        super().__init__(data_handler)
        self.rsi_period = rsi_period
        self.overbought = overbought
        self.oversold = oversold
        self.logger = logger

    def calculate_rsi(self, prices: pd.Series) -> pd.Series:
        """
        Calculate RSI for the given price series.
        """
        delta = prices.diff()
        gain = delta.where(delta > 0, 0).rolling(window=self.rsi_period).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=self.rsi_period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def generate_signals(self) -> pd.DataFrame:
        """
        Generate trading signals based on RSI.
        Buy (1) when RSI < oversold, Sell (-1) when RSI > overbought, Hold (0) otherwise.
        """
        signals = pd.DataFrame(index=self.data.index)
        signals['Close'] = self.data['Close']
        signals['RSI'] = self.calculate_rsi(self.data['Close'])
        signals['Signal'] = 0

        # Generate signals
        signals.loc[signals['RSI'] < self.oversold, 'Signal'] = 1   # Buy
        signals.loc[signals['RSI'] > self.overbought, 'Signal'] = -1  # Sell

        # Log signals
        if self.logger:
            for date, row in signals.iterrows():
                if not pd.isna(row['RSI']):  # Skip NaN RSI values
                    self.logger.log_signal(date, row['Signal'], row['RSI'], row['Close'])

        return signals