import pandas as pd
import numpy as np
from src.strategies.base_strategy import BaseStrategy
from src.logging.trade_logger import TradeLogger
from typing import Optional, Dict

class MACDStrategy(BaseStrategy):
    strategy_type = "MACD"

    def __init__(
        self,
        data_handler,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        cooldown_minutes: int = 30,
        logger: TradeLogger = None
    ):
        super().__init__(data_handler)
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.cooldown_minutes = cooldown_minutes
        self.logger = logger
        self._macd_cache = pd.Series(index=self.data.index, dtype=float)
        self._signal_line_cache = pd.Series(index=self.data.index, dtype=float)
        self._histogram_cache = pd.Series(index=self.data.index, dtype=float)
        self._last_calculated_date = None
        self.last_trade_time = None
        self.position = None

    def calculate_macd(self, prices: pd.Series, end_date: pd.Timestamp) -> tuple[float, float, float]:
        data_slice = prices.loc[:end_date]
        if len(data_slice) < max(self.slow_period, self.signal_period + self.fast_period):
            return np.nan, np.nan, np.nan

        ema_fast = data_slice.ewm(span=self.fast_period, adjust=False).mean()
        ema_slow = data_slice.ewm(span=self.slow_period, adjust=False).mean()
        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=self.signal_period, adjust=False).mean()
        histogram = macd - signal_line

        return macd.iloc[-1], signal_line.iloc[-1], histogram.iloc[-1]

    def generate_signal(self, date: pd.Timestamp, portfolio: 'Portfolio') -> Optional[int]:
        if date not in self.data.index:
            return None

        current_data = self.data.loc[date]
        price = current_data['Close']

        # Check cooldown
        if self.last_trade_time and (date - self.last_trade_time).total_seconds() / 60 < self.cooldown_minutes:
            if self.logger:
                self.log_signal_data(date, price, {"macd": np.nan, "signal_line": np.nan, "histogram": np.nan}, None)
            return None

        if (pd.isna(self._macd_cache.loc[date]) or 
            pd.isna(self._signal_line_cache.loc[date]) or 
            pd.isna(self._histogram_cache.loc[date]) or 
            self._last_calculated_date != date):
            macd, signal_line, histogram = self.calculate_macd(self.data['Close'], date)
            self._macd_cache.loc[date] = macd
            self._signal_line_cache.loc[date] = signal_line
            self._histogram_cache.loc[date] = histogram
            self._last_calculated_date = date
        else:
            macd = self._macd_cache.loc[date]
            signal_line = self._signal_line_cache.loc[date]
            histogram = self._histogram_cache.loc[date]

        if pd.isna(macd) or pd.isna(signal_line) or pd.isna(histogram):
            if self.logger:
                self.log_signal_data(date, price, {"macd": macd, "signal_line": signal_line, "histogram": histogram}, None)
            return None

        prev_macd = self._macd_cache.loc[:date].iloc[-2] if len(self._macd_cache.loc[:date]) > 1 else np.nan
        prev_signal_line = self._signal_line_cache.loc[:date].iloc[-2] if len(self._signal_line_cache.loc[:date]) > 1 else np.nan

        if pd.isna(prev_macd) or pd.isna(prev_signal_line):
            if self.logger:
                self.log_signal_data(date, price, {"macd": macd, "signal_line": signal_line, "histogram": histogram}, None)
            return None

        current_quantity = portfolio.get_current_quantity(date)
        signal = 0

        if current_quantity == 0 and self.position is None:
            if macd > signal_line and prev_macd <= prev_signal_line:
                signal = 1  # Buy
                quantity = portfolio.calculate_position_size(price)
                self.position = {'type': 'long', 'entry_price': price, 'quantity': quantity}
            elif macd < signal_line and prev_macd >= prev_signal_line:
                signal = -1  # Short
                quantity = portfolio.calculate_position_size(price)
                self.position = {'type': 'short', 'entry_price': price, 'quantity': quantity}
        elif self.position:
            if self.position['type'] == 'long' and macd < signal_line and prev_macd >= prev_signal_line:
                signal = -1  # Sell
                self.position = None
                self.last_trade_time = date
            elif self.position['type'] == 'short' and macd > signal_line and prev_macd <= prev_signal_line:
                signal = 1  # Cover
                self.position = None
                self.last_trade_time = date

        if self.logger:
            self.log_signal_data(date, price, {"macd": macd, "signal_line": signal_line, "histogram": histogram}, signal)

        return signal

    def log_signal_data(self, timestamp: pd.Timestamp, price: float, indicators: Dict, signal: Optional[int]):
        if self.logger:
            self.logger.log_signal_data(timestamp, price, indicators, signal)

    def generate_signals(self) -> pd.DataFrame:
        raise NotImplementedError("Batch signal generation is deprecated. Use generate_signal for sequential processing.")