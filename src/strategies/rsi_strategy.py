import pandas as pd
import numpy as np
from src.strategies.base_strategy import BaseStrategy
from src.logging.trade_logger import TradeLogger
from typing import Optional

class RSIStrategy(BaseStrategy):
    def __init__(self, data_handler, rsi_period: int, overbought: float, oversold: float, wait_period: int, logger: TradeLogger = None):
        super().__init__(data_handler)
        self.rsi_period = rsi_period
        self.overbought = overbought
        self.oversold = oversold
        self.wait_period = wait_period
        self.logger = logger
        self._rsi_cache = pd.Series(index=self.data.index, dtype=float)
        self._last_calculated_date = None

    def calculate_rsi(self, prices: pd.Series, end_date: pd.Timestamp) -> float:
        data_slice = prices.loc[:end_date]
        if len(data_slice) < self.rsi_period + 1:
            return np.nan
        delta = data_slice.diff()
        gain = delta.where(delta > 0, 0).rolling(window=self.rsi_period).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=self.rsi_period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]

    def generate_signal(self, date: pd.Timestamp, portfolio: 'Portfolio') -> Optional[int]:
        if date not in self.data.index:
            return None

        current_data = self.data.loc[date]
        price = current_data['Close']

        if pd.isna(self._rsi_cache.loc[date]) or self._last_calculated_date != date:
            rsi = self.calculate_rsi(self.data['Close'], date)
            self._rsi_cache.loc[date] = rsi
            self._last_calculated_date = date
        else:
            rsi = self._rsi_cache.loc[date]

        if pd.isna(rsi):
            if self.logger:
                self.log_signal_data(date, price, rsi, None)
            return None

        current_quantity = portfolio.get_current_quantity(date)

        signal = 0
        if current_quantity == 0:
            if rsi < self.oversold:
                signal = 1  # Buy
            elif rsi > self.overbought:
                signal = -1  # Short
        elif current_quantity > 0:  # Long position
            if rsi > self.overbought:
                signal = -1  # Sell
        elif current_quantity < 0:  # Short position
            if rsi < self.oversold:
                signal = 1  # Cover

        # Log signal data
        if self.logger:
            self.log_signal_data(date, price, rsi, signal)

            return signal
        
    
    def log_signal_data(self, timestamp: pd.Timestamp, price: float, rsi: float, signal: Optional[int]):
        """Delegate signal data logging to TradeLogger."""
        self.logger.log_signal_data(timestamp, price, rsi, signal)
        
    

    def generate_signals(self) -> pd.DataFrame:
        raise NotImplementedError("Batch signal generation is deprecated. Use generate_signal for sequential processing.")