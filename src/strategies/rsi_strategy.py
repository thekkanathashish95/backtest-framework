import pandas as pd
import numpy as np
from src.strategies.base_strategy import BaseStrategy
from src.logging.trade_logger import TradeLogger
from typing import Optional

class RSIStrategy(BaseStrategy):
    def __init__(self, data_handler, rsi_period: int = 14, overbought: float = 60, oversold: float = 30, wait_period: int = 5, logger: TradeLogger = None):
        super().__init__(data_handler)
        self.rsi_period = rsi_period
        self.base_overbought = overbought
        self.base_oversold = oversold
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

    def calculate_atr(self, data: pd.DataFrame, end_date: pd.Timestamp, period: int = 14) -> float:
        data_slice = data.loc[:end_date]
        if len(data_slice) < period + 1:
            return np.nan
        high_low = data_slice['High'] - data_slice['Low']
        high_close = abs(data_slice['High'] - data_slice['Close'].shift(1))
        low_close = abs(data_slice['Low'] - data_slice['Close'].shift(1))
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(window=period).mean().iloc[-1]
        return atr

    def generate_signal(self, date: pd.Timestamp, portfolio: 'Portfolio') -> Optional[int]:
        if date not in self.data.index:
            return None

        if portfolio.last_trade_date is not None:
            time_since_last_trade = (date - portfolio.last_trade_date).total_seconds() / 60
            if time_since_last_trade < self.wait_period:
                self.logger._log("DEBUG", f"Skipped signal: within {self.wait_period}-minute wait period (time since last trade: {time_since_last_trade:.2f} minutes)", date, {})
                return None

        current_data = self.data.loc[date]
        price = current_data['Close']

        # Calculate RSI
        if pd.isna(self._rsi_cache.loc[date]) or self._last_calculated_date != date:
            rsi = self.calculate_rsi(self.data['Close'], date)
            self._rsi_cache.loc[date] = rsi
            self._last_calculated_date = date
        else:
            rsi = self._rsi_cache.loc[date]

        if pd.isna(rsi):
            return None

        # Calculate ATR for dynamic thresholds
        atr = self.calculate_atr(self.data, date)
        if pd.isna(atr):
            overbought = self.base_overbought
            oversold = self.base_oversold
        else:
            atr_factor = atr / price * 100  # Normalize ATR to price
            overbought = min(75, self.base_overbought + 5 * atr_factor)  # Cap at 75
            oversold = max(25, self.base_oversold - 5 * atr_factor)      # Floor at 25

        # Log RSI and thresholds
        if self.logger:
            self.logger._log("DEBUG", f"RSI: {rsi:.2f}, Overbought: {overbought:.2f}, Oversold: {oversold:.2f}, ATR: {atr:.2f}", date, {})

        # Calculate 5-period SMA for trend
        hist_data = self.data_handler.get_historical_data(date)
        if len(hist_data) >= 5:
            price_ma5 = hist_data['Close'].rolling(5).mean().iloc[-1]
            price_ma5_prev = hist_data['Close'].rolling(5).mean().iloc[-2]
            uptrend = price_ma5 > price_ma5_prev
        else:
            uptrend = True  # Default to allow trading with limited data

        current_quantity = portfolio.get_current_quantity(date)

        signal = 0

        # Signal logic
        if current_quantity == 0:
            if rsi < oversold and uptrend:
                signal = 1  # Buy on oversold in uptrend
            elif rsi > overbought and not uptrend:
                signal = -1  # Short on overbought in downtrend
        elif current_quantity > 0:  # Long position
            if rsi > overbought:
                signal = -1  # Sell when overbought
        elif current_quantity < 0:  # Short position
            if rsi < oversold:
                signal = 1  # Cover when oversold

        if self.logger and signal != 0:
            self.logger.log_signal(date, signal, rsi, price)

        return signal

    def generate_signals(self) -> pd.DataFrame:
        raise NotImplementedError("Batch signal generation is deprecated. Use generate_signal for sequential processing.")