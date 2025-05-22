import pandas as pd
import numpy as np
from src.strategies.base_strategy import BaseStrategy
from src.logging.trade_logger import TradeLogger
from typing import Optional, Dict

class RSIStrategy(BaseStrategy):
    strategy_type = "RSI"

    def __init__(self, data_handler, rsi_period: int, overbought: float, oversold: float, cooldown_minutes: int, logger: TradeLogger = None):
        super().__init__(data_handler)
        self.rsi_period = rsi_period
        self.overbought = overbought
        self.oversold = oversold
        self.cooldown_minutes = cooldown_minutes
        self.logger = logger
        self._rsi_cache = pd.Series(index=self.data.index, dtype=float)
        self._last_calculated_date = None
        self.last_trade_time = None

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

        # Check cooldown
        if self.last_trade_time and (date - self.last_trade_time).total_seconds() / 60 < self.cooldown_minutes:
            if self.logger:
                self.log_signal_data(date, price, {"rsi": np.nan}, None)
            return None

        if pd.isna(self._rsi_cache.loc[date]) or self._last_calculated_date != date:
            rsi = self.calculate_rsi(self.data['Close'], date)
            self._rsi_cache.loc[date] = rsi
            self._last_calculated_date = date
        else:
            rsi = self._rsi_cache.loc[date]

        if pd.isna(rsi):
            if self.logger:
                self.log_signal_data(date, price, {"rsi": rsi}, None)
            return None

        current_quantity = portfolio.get_current_quantity(date)
        signal = 0
        if current_quantity == 0:
            prev_rsi = self._rsi_cache.loc[:date].iloc[-2] if len(self._rsi_cache.loc[:date]) > 1 else np.nan
            if not pd.isna(prev_rsi):
                if rsi < self.oversold and prev_rsi >= self.oversold:
                    signal = 1  # Buy
                elif rsi > self.overbought and prev_rsi <= self.overbought:
                    signal = -1  # Short
        elif current_quantity > 0:  # Long position
            if rsi > self.overbought:
                signal = -1  # Sell
                self.last_trade_time = date
        elif current_quantity < 0:  # Short position
            if rsi < self.oversold:
                signal = 1  # Cover
                self.last_trade_time = date

        # Log signal data
        if self.logger:
            self.log_signal_data(date, price, {"rsi": rsi}, signal)

        return signal

    def log_signal_data(self, timestamp: pd.Timestamp, price: float, indicators: Dict, signal: Optional[int]):
        """Delegate signal data logging to TradeLogger."""
        if self.logger:
            self.logger.log_signal_data(timestamp, price, indicators, signal)

    def generate_signals(self) -> pd.DataFrame:
        raise NotImplementedError("Batch signal generation is deprecated. Use generate_signal for sequential processing.")