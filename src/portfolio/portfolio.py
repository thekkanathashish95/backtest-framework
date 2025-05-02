import pandas as pd
import numpy as np
from src.logging.trade_logger import TradeLogger
from src.backtest.metrics import Metrics
from typing import Optional

class Portfolio:
    def __init__(self, initial_cash: float, data_handler, strategy, logger: TradeLogger):
        self.initial_cash = float(initial_cash)
        self.cash = self.initial_cash
        self.data_handler = data_handler
        self.strategy = strategy
        self.logger = logger
        self.positions = pd.DataFrame(
            index=self.data_handler.data.index,
            columns=['Quantity', 'Price'],
            dtype=float
        ).fillna(0)
        self.trades = pd.DataFrame({
            'Date': pd.Series(dtype='datetime64[ns, Asia/Kolkata]'),
            'Symbol': pd.Series(dtype=str),
            'Action': pd.Series(dtype=str),
            'Quantity': pd.Series(dtype=int),
            'Price': pd.Series(dtype=float),
            'Value': pd.Series(dtype=float),
            'Fees': pd.Series(dtype=float),
            'NetProfit': pd.Series(dtype=float)
        })
        self.portfolio_value = pd.DataFrame(
            index=self.data_handler.data.index,
            columns=['Cash', 'Holdings', 'Total'],
            dtype=float
        )
        self.portfolio_value['Cash'] = self.initial_cash
        self.portfolio_value['Holdings'] = 0.0
        self.portfolio_value['Total'] = self.initial_cash
        self._current_quantity = 0
        self.last_trade_date: Optional[pd.Timestamp] = None
        self.skipped_trades = 0
        self.entry_price = None
        # Transaction cost parameters (in USD, 1 USD = 83 INR)
        self.brokerage_rate = 0.0003  # 0.03%
        self.brokerage_min = 0.24     # Rs. 20
        self.stt_ctt_rate = 0.00025   # 0.025% on sell side
        self.transaction_rate = 0.0000297  # NSE: 0.00297%
        self.gst_rate = 0.18          # 18%
        self.sebi_rate = 0.12 / 10000000  # Rs. 10/crore
        self.stamp_rate = 0.00003     # 0.003% on buy side

    def _calculate_fees(self, trade_value: float, action: str) -> float:
        """
        Calculate transaction fees for a trade.
        """
        brokerage = min(self.brokerage_rate * trade_value, self.brokerage_min)
        transaction = self.transaction_rate * trade_value
        sebi = self.sebi_rate * trade_value
        gst = self.gst_rate * (brokerage + transaction + sebi)
        stt_ctt = self.stt_ctt_rate * trade_value if action in ['Sell', 'Cover'] else 0
        stamp = self.stamp_rate * trade_value if action in ['Buy', 'Short'] else 0
        total_fees = brokerage + transaction + sebi + gst + stt_ctt + stamp
        return total_fees

    def process_bar(self, date: pd.Timestamp) -> None:
        if date not in self.data_handler.data.index:
            self.logger._log("DEBUG", f"Skipping date {date}: not in data index", date, {})
            return

        price = self.data_handler.data.loc[date, 'Close']
        symbol = self.data_handler.tradingsymbol
        max_tradeable_volume = self.data_handler.data.loc[date, 'MaxTradeableVolume'] if 'MaxTradeableVolume' in self.data_handler.data.columns else float('inf')

        if self.cash < 0.1 * self.initial_cash:
            self.logger._log("WARNING", f"Low cash: {self.cash:.2f} (<10% of initial)", date, {})

        # Apply stop-loss and take-profit
        if self._current_quantity != 0 and self.entry_price is not None:
            price_change = (price - self.entry_price) / self.entry_price
            if self._current_quantity > 0:  # Long position
                if price_change <= -0.03:  # Stop-loss: -3%
                    self._exit_position(date, price, symbol, max_tradeable_volume, 'Sell', "Stop-loss triggered")
                elif price_change >= 0.05:  # Take-profit: +5%
                    self._exit_position(date, price, symbol, max_tradeable_volume, 'Sell', "Take-profit triggered")
            elif self._current_quantity < 0:  # Short position
                if price_change >= 0.03:  # Stop-loss: +3%
                    self._exit_position(date, price, symbol, max_tradeable_volume, 'Cover', "Stop-loss triggered")
                elif price_change <= -0.05:  # Take-profit: -5%
                    self._exit_position(date, price, symbol, max_tradeable_volume, 'Cover', "Take-profit triggered")

        # Close positions on final bar
        if date == self.data_handler.data.index[-1] and self._current_quantity != 0:
            if self._current_quantity > 0:
                self._exit_position(date, price, symbol, max_tradeable_volume, 'Sell', "Closing position at end")
            elif self._current_quantity < 0:
                self._exit_position(date, price, symbol, max_tradeable_volume, 'Cover', "Closing position at end")

        # Generate signal
        signal = self.strategy.generate_signal(date, self)
        if signal is None:
            self._update_portfolio_value(date, price)
            return

        # Execute trade
        if signal == 1:
            if self._current_quantity < 0:
                self._exit_position(date, price, symbol, max_tradeable_volume, 'Cover', "Cover short position")
            elif self._current_quantity == 0:
                quantity = min(int((0.5 * self.cash) // price), max_tradeable_volume)
                if quantity > 0:
                    cost = quantity * price
                    fees = self._calculate_fees(cost, 'Buy')
                    self.cash -= cost + fees
                    self._current_quantity += quantity
                    self.positions.loc[date, 'Quantity'] = self._current_quantity
                    self.positions.loc[date, 'Price'] = price
                    self.entry_price = price
                    self.trades = pd.concat([self.trades, pd.DataFrame([{
                        'Date': date,
                        'Symbol': symbol,
                        'Action': 'Buy',
                        'Quantity': quantity,
                        'Price': price,
                        'Value': cost,
                        'Fees': fees,
                        'NetProfit': np.nan
                    }])], ignore_index=True)
                    self.logger.log_trade(date, 'Buy', quantity, price, cost + fees, symbol=symbol, fees=fees, net_profit=None)
                    self.logger._log("DEBUG", f"Bought: {quantity} shares at {price}, fees: {fees:.2f}", date, {})
                    self.last_trade_date = date
                else:
                    self.logger._log("DEBUG", f"Skipped Buy: insufficient cash {self.cash:.2f} or volume {max_tradeable_volume}", date, {})
                    self.skipped_trades += 1
            else:
                self.logger._log("DEBUG", f"Skipped Buy: already holding {self._current_quantity}", date, {})

        elif signal == -1:
            if self._current_quantity > 0:
                self._exit_position(date, price, symbol, max_tradeable_volume, 'Sell', "Sell long position")
            elif self._current_quantity == 0:
                short_quantity = min(int((0.3 * self.cash) // price), max_tradeable_volume)
                if short_quantity > 0:
                    short_value = short_quantity * price
                    fees = self._calculate_fees(short_value, 'Short')
                    self.cash += short_value - fees
                    self._current_quantity = -short_quantity
                    self.positions.loc[date, 'Quantity'] = self._current_quantity
                    self.positions.loc[date, 'Price'] = price
                    self.entry_price = price
                    self.trades = pd.concat([self.trades, pd.DataFrame([{
                        'Date': date,
                        'Symbol': symbol,
                        'Action': 'Short',
                        'Quantity': short_quantity,
                        'Price': price,
                        'Value': short_value,
                        'Fees': fees,
                        'NetProfit': np.nan
                    }])], ignore_index=True)
                    self.logger.log_trade(date, 'Short', short_quantity, price, short_value - fees, symbol=symbol, fees=fees, net_profit=None)
                    self.logger._log("DEBUG", f"Shorted: {short_quantity} shares at {price}, fees: {fees:.2f}", date, {})
                    self.last_trade_date = date
                else:
                    self.logger._log("DEBUG", f"Skipped Short: insufficient cash {self.cash:.2f} or volume {max_tradeable_volume}", date, {})
                    self.skipped_trades += 1
            else:
                self.logger._log("DEBUG", f"Skipped Sell: already short {self._current_quantity}", date, {})

        self._update_portfolio_value(date, price)

    def _exit_position(self, date: pd.Timestamp, price: float, symbol: str, max_tradeable_volume: float, action: str, reason: str):
        """
        Exit a position (sell long or cover short) and log the trade.
        """
        if action == 'Sell':
            quantity = min(self._current_quantity, max_tradeable_volume)
            if quantity > 0:
                revenue = quantity * price
                fees = self._calculate_fees(revenue, 'Sell')
                profit = (price - self.entry_price) * quantity
                net_profit = profit - (fees + self.trades.iloc[-1]['Fees'])
                self.cash += revenue - fees
                self._current_quantity -= quantity
                self.trades = pd.concat([self.trades, pd.DataFrame([{
                    'Date': date,
                    'Symbol': symbol,
                    'Action': 'Sell',
                    'Quantity': quantity,
                    'Price': price,
                    'Value': revenue,
                    'Fees': fees,
                    'NetProfit': net_profit
                }])], ignore_index=True)
                self.logger.log_trade(date, 'Sell', quantity, price, revenue - fees, symbol=symbol, fees=fees, net_profit=net_profit)
                self.logger._log("DEBUG", f"{reason}: sold {quantity} shares at {price}, fees: {fees:.2f}, net profit: {net_profit:.2f}", date, {})
                if net_profit < 0:
                    self.logger._log("INFO", f"Losing trade: {action} at {price}, net profit: {net_profit:.2f}", date, {})
                self.positions.loc[date, 'Quantity'] = self._current_quantity
                self.positions.loc[date, 'Price'] = price
                self.entry_price = None
            else:
                self.logger._log("DEBUG", f"Skipped {action}: insufficient volume {max_tradeable_volume}", date, {})
                self.skipped_trades += 1
        elif action == 'Cover':
            cover_quantity = min(-self._current_quantity, max_tradeable_volume)
            cost = cover_quantity * price
            fees = self._calculate_fees(cost, 'Cover')
            if cost + fees <= self.cash and cover_quantity > 0:
                profit = (self.entry_price - price) * cover_quantity
                net_profit = profit - (fees + self.trades.iloc[-1]['Fees'])
                self.cash -= cost + fees
                self._current_quantity += cover_quantity
                self.positions.loc[date, 'Quantity'] = self._current_quantity
                self.positions.loc[date, 'Price'] = price
                self.trades = pd.concat([self.trades, pd.DataFrame([{
                    'Date': date,
                    'Symbol': symbol,
                    'Action': 'Cover',
                    'Quantity': cover_quantity,
                    'Price': price,
                    'Value': cost,
                    'Fees': fees,
                    'NetProfit': net_profit
                }])], ignore_index=True)
                self.logger.log_trade(date, 'Cover', cover_quantity, price, cost + fees, symbol=symbol, fees=fees, net_profit=net_profit)
                self.logger._log("DEBUG", f"{reason}: covered {cover_quantity} shares at {price}, fees: {fees:.2f}, net profit: {net_profit:.2f}", date, {})
                if net_profit < 0:
                    self.logger._log("INFO", f"Losing trade: {action} at {price}, net profit: {net_profit:.2f}", date, {})
                self.entry_price = None
            else:
                self.logger._log("DEBUG", f"Skipped {action}: insufficient cash {self.cash:.2f} or volume {max_tradeable_volume}", date, {})
                self.skipped_trades += 1

    def _update_portfolio_value(self, date: pd.Timestamp, price: float):
        holdings_value = self._current_quantity * price
        self.portfolio_value.loc[date] = {
            'Cash': self.cash,
            'Holdings': holdings_value,
            'Total': self.cash + holdings_value
        }
        if self.portfolio_value.loc[date, 'Total'] <= 0:
            self.logger._log("ERROR", f"Portfolio value non-positive: {self.portfolio_value.loc[date, 'Total']:.2f}", date, {})
        self.logger.log_portfolio(date, self.cash, holdings_value, self.cash + holdings_value, self._current_quantity)

    def get_current_quantity(self, date: pd.Timestamp) -> int:
        return self._current_quantity

    def get_portfolio_summary(self) -> dict:
        print("\nNon-zero Positions:")
        print(self.positions[self.positions['Quantity'] != 0])
        metrics = Metrics(self.portfolio_value, self.trades, self.initial_cash)
        metrics_dict = metrics.get_metrics()
        print("\nPerformance Metrics:")
        for key, value in metrics_dict.items():
            if isinstance(value, pd.Timestamp):
                print(f"{key}: {value}")
            else:
                print(f"{key}: {value:.2f}")
        print(f"Skipped Trades: {self.skipped_trades}")
        return {
            'portfolio_value': self.portfolio_value,
            'trades': self.trades,
            'final_cash': self.cash,
            'final_holdings': self._current_quantity,
            'metrics': metrics_dict,
            'skipped_trades': self.skipped_trades
        }

    def execute_trades(self):
        raise NotImplementedError("Batch trade execution is deprecated. Use process_bar for sequential processing.")