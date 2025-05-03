import pandas as pd
import numpy as np
from src.logging.trade_logger import TradeLogger
from src.backtest.metrics import Metrics
from typing import Optional
import yaml

class Portfolio:
    def __init__(self, initial_cash: float, data_handler, strategy, logger: TradeLogger, config_path: str = 'config/config.yaml'):
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
            'trade_id': pd.Series(dtype=int),
            'entry_trade_id': pd.Series(dtype=int),
            'Date': pd.Series(dtype='datetime64[ns, Asia/Kolkata]'),
            'Symbol': pd.Series(dtype=str),
            'Action': pd.Series(dtype=str),
            'Quantity': pd.Series(dtype=int),
            'Price': pd.Series(dtype=float),
            'Value': pd.Series(dtype=float),
            'Fees': pd.Series(dtype=float),
            'NetProfit': pd.Series(dtype=float),
            'Reason': pd.Series(dtype=str)
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
        self.avg_entry_price = None
        self.total_entry_value = 0.0
        self.trade_counter = 0
        # Load transaction costs from config
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.transaction_costs = config.get('transaction_costs', {
            'brokerage_rate': 0.0003,
            'brokerage_min': 0.24,
            'stt_ctt_rate': 0.00025,
            'transaction_rate': 0.0000297,
            'gst_rate': 0.18,
            'sebi_rate': 0.12 / 10000000,
            'stamp_rate': 0.00003
        })

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

    def calculate_position_size(self, price: float, date: pd.Timestamp, action: str) -> int:
        hist_data = self.data_handler.get_historical_data(date)
        portfolio_value = self.portfolio_value.loc[date, 'Total']
        if portfolio_value <= 0:
            self.logger._log("WARNING", f"Cannot trade: portfolio value non-positive ({portfolio_value:.2f})", date, {})
            return 0
        if len(hist_data) >= 14:
            atr = self.calculate_atr(hist_data, date)
            volatility = atr / price if not pd.isna(atr) else 0.005
            risk_capital = 0.01 * portfolio_value  # Risk 1% of portfolio
            quantity = int(risk_capital / (price * volatility))
            # Cap position size to 20% of portfolio value or 2x leverage
            max_quantity = int((0.2 * portfolio_value) / price)
            max_leverage_quantity = int((2 * self.initial_cash) / price)
            max_volume = hist_data.loc[date, 'MaxTradeableVolume'] if 'MaxTradeableVolume' in hist_data.columns else float('inf')
            quantity = max(5, min(quantity, max_quantity, max_leverage_quantity, max_volume))
            self.logger._log("DEBUG", f"Position size: {quantity} shares, volatility: {volatility:.4f}, risk_capital: {risk_capital:.2f}, max_quantity: {max_quantity}", date, {})
            return quantity
        # Fallback to fixed percentage
        max_tradeable_volume = hist_data.loc[date, 'MaxTradeableVolume'] if 'MaxTradeableVolume' in hist_data.columns else float('inf')
        max_quantity = int((0.2 * portfolio_value) / price)
        quantity = max(5, min(int((0.3 * self.cash) // price), max_quantity, max_tradeable_volume)) if action in ['Buy', 'Cover'] else max(5, min(int((0.2 * self.cash) // price), max_quantity, max_tradeable_volume))
        self.logger._log("DEBUG", f"Fallback position size: {quantity} shares, cash: {self.cash:.2f}, max_quantity: {max_quantity}", date, {})
        return quantity

    def apply_slippage(self, price: float, quantity: int, action: str, date: pd.Timestamp) -> float:
        base_slippage_pct = 0.0005  # 0.05%
        volume = self.data_handler.data.loc[date, 'Volume'] if 'Volume' in self.data_handler.data.columns else float('inf')
        volume_factor = min(1.0, quantity / volume * 10)
        direction_factor = 1.1 if action in ['Buy', 'Cover'] else 0.9
        slippage_pct = base_slippage_pct * volume_factor * direction_factor
        return price * (1 + slippage_pct) if action in ['Buy', 'Cover'] else price * (1 - slippage_pct)

    def _calculate_fees(self, trade_value: float, action: str) -> float:
        brokerage = min(self.transaction_costs['brokerage_rate'] * trade_value, self.transaction_costs['brokerage_min'])
        transaction = self.transaction_costs['transaction_rate'] * trade_value
        sebi = self.transaction_costs['sebi_rate'] * trade_value
        gst = self.transaction_costs['gst_rate'] * (brokerage + transaction + sebi)
        stt_ctt = self.transaction_costs['stt_ctt_rate'] * trade_value if action in ['Sell', 'Cover'] else 0
        stamp = self.transaction_costs['stamp_rate'] * trade_value if action in ['Buy', 'Short'] else 0
        return brokerage + transaction + sebi + gst + stt_ctt + stamp

    def process_bar(self, date: pd.Timestamp) -> None:
        if date not in self.data_handler.data.index:
            self.logger._log("DEBUG", f"Skipping date {date}: not in data index", date, {})
            return

        price = self.data_handler.data.loc[date, 'Close']
        symbol = self.data_handler.tradingsymbol
        max_tradeable_volume = self.data_handler.data.loc[date, 'MaxTradeableVolume'] if 'MaxTradeableVolume' in self.data_handler.data.columns else float('inf')

        if self.cash < 0.1 * self.initial_cash:
            self.logger._log("WARNING", f"Low cash: {self.cash:.2f} (<10% of initial)", date, {})
            return

        if self.portfolio_value.loc[date, 'Total'] <= 0:
            self.logger._log("WARNING", f"Cannot trade: portfolio value non-positive ({self.portfolio_value.loc[date, 'Total']:.2f})", date, {})
            return

        # Apply stop-loss and take-profit
        if self._current_quantity != 0 and self.entry_price is not None:
            atr = self.calculate_atr(self.data_handler.data, date)
            stop_loss_pct = 0.03 if pd.isna(atr) else min(0.08, 3 * atr / price)  # 3x ATR, capped at 8%
            take_profit_pct = 0.05 if pd.isna(atr) else min(0.12, 4.5 * atr / price)  # 4.5x ATR, capped at 12%
            if self.logger:
                self.logger._log("DEBUG", f"Stop-Loss Pct: {stop_loss_pct:.4f}, Take-Profit Pct: {take_profit_pct:.4f}, ATR: {atr:.2f}", date, {})
            if self._current_quantity > 0:  # Long position
                stop_loss_price = self.avg_entry_price * (1 - stop_loss_pct)
                take_profit_price = self.avg_entry_price * (1 + take_profit_pct)
                if price <= stop_loss_price:
                    self._exit_position(date, price, symbol, max_tradeable_volume, 'Sell', "Stop-loss triggered")
                elif price >= take_profit_price:
                    self._exit_position(date, price, symbol, max_tradeable_volume, 'Sell', "Take-profit triggered")
            elif self._current_quantity < 0:  # Short position
                stop_loss_price = self.avg_entry_price * (1 + stop_loss_pct)
                take_profit_price = self.avg_entry_price * (1 - take_profit_pct)
                if price >= stop_loss_price:
                    self._exit_position(date, price, symbol, max_tradeable_volume, 'Cover', "Stop-loss triggered")
                elif price <= take_profit_price:
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
                quantity = self.calculate_position_size(price, date, 'Buy')
                if quantity > 0:
                    adjusted_price = self.apply_slippage(price, quantity, 'Buy', date)
                    cost = quantity * adjusted_price
                    fees = self._calculate_fees(cost, 'Buy')
                    if cost + fees > self.cash:
                        self.logger._log("DEBUG", f"Skipped Buy: insufficient cash {self.cash:.2f} for cost {cost + fees:.2f}", date, {})
                        self.skipped_trades += 1
                        return
                    self.cash -= cost + fees
                    self._current_quantity += quantity
                    self.positions.loc[date, 'Quantity'] = self._current_quantity
                    self.positions.loc[date, 'Price'] = adjusted_price
                    self.entry_price = adjusted_price
                    self.avg_entry_price = adjusted_price
                    self.total_entry_value = cost
                    trade_data = {
                        'trade_id': self.trade_counter,
                        'entry_trade_id': None,
                        'Date': date,
                        'Symbol': symbol,
                        'Action': 'Buy',
                        'Quantity': quantity,
                        'Price': adjusted_price,
                        'Value': cost,
                        'Fees': fees,
                        'NetProfit': np.nan,
                        'Reason': 'Entry'
                    }
                    self.trades = pd.concat([self.trades, pd.DataFrame([trade_data])], ignore_index=True)
                    self.trade_counter += 1
                    self.logger.log_trade(date, 'Buy', quantity, adjusted_price, cost + fees, symbol=symbol, fees=fees, net_profit=None)
                    self.logger._log("DEBUG", f"Bought: {quantity} shares at {adjusted_price}, fees: {fees:.2f}", date, {})
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
                short_quantity = self.calculate_position_size(price, date, 'Short')
                if short_quantity > 0:
                    adjusted_price = self.apply_slippage(price, short_quantity, 'Short', date)
                    short_value = short_quantity * adjusted_price
                    fees = self._calculate_fees(short_value, 'Short')
                    self.cash += short_value - fees
                    self._current_quantity = -short_quantity
                    self.positions.loc[date, 'Quantity'] = self._current_quantity
                    self.positions.loc[date, 'Price'] = adjusted_price
                    self.entry_price = adjusted_price
                    self.avg_entry_price = adjusted_price
                    self.total_entry_value = short_value
                    trade_data = {
                        'trade_id': self.trade_counter,
                        'entry_trade_id': None,
                        'Date': date,
                        'Symbol': symbol,
                        'Action': 'Short',
                        'Quantity': short_quantity,
                        'Price': adjusted_price,
                        'Value': short_value,
                        'Fees': fees,
                        'NetProfit': np.nan,
                        'Reason': 'Entry'
                    }
                    self.trades = pd.concat([self.trades, pd.DataFrame([trade_data])], ignore_index=True)
                    self.trade_counter += 1
                    self.logger.log_trade(date, 'Short', short_quantity, adjusted_price, short_value - fees, symbol=symbol, fees=fees, net_profit=None)
                    self.logger._log("DEBUG", f"Shorted: {short_quantity} shares at {adjusted_price}, fees: {fees:.2f}", date, {})
                    self.last_trade_date = date
                else:
                    self.logger._log("DEBUG", f"Skipped Short: insufficient cash {self.cash:.2f} or volume {max_tradeable_volume}", date, {})
                    self.skipped_trades += 1
            else:
                self.logger._log("DEBUG", f"Skipped Sell: already short {self._current_quantity}", date, {})

        self._update_portfolio_value(date, price)

    def _exit_position(self, date: pd.Timestamp, price: float, symbol: str, max_tradeable_volume: float, action: str, reason: str):
        adjusted_price = self.apply_slippage(price, abs(self._current_quantity), action, date)
        if action == 'Sell':
            quantity = min(self._current_quantity, max_tradeable_volume)
            if quantity > 0:
                revenue = quantity * adjusted_price
                fees = self._calculate_fees(revenue, 'Sell')
                entry_trade = self.trades[(self.trades['Action'] == 'Buy') & (self.trades['Date'] < date)].iloc[-1]
                profit = (adjusted_price - entry_trade['Price']) * quantity
                net_profit = profit - (fees + entry_trade['Fees'])
                self.cash += revenue - fees
                self._current_quantity -= quantity
                trade_data = {
                    'trade_id': self.trade_counter,
                    'entry_trade_id': entry_trade['trade_id'],
                    'Date': date,
                    'Symbol': symbol,
                    'Action': 'Sell',
                    'Quantity': quantity,
                    'Price': adjusted_price,
                    'Value': revenue,
                    'Fees': fees,
                    'NetProfit': net_profit,
                    'Reason': reason
                }
                self.trades = pd.concat([self.trades, pd.DataFrame([trade_data])], ignore_index=True)
                self.trade_counter += 1
                self.logger.log_trade(date, 'Sell', quantity, adjusted_price, revenue - fees, symbol=symbol, fees=fees, net_profit=net_profit)
                self.logger._log("DEBUG", f"{reason}: sold {quantity} shares at {adjusted_price}, fees: {fees:.2f}, net profit: {net_profit:.2f}", date, {})
                if net_profit < 0:
                    self.logger._log("INFO", f"Losing trade: {action} at {adjusted_price}, net profit: {net_profit:.2f}", date, {})
                self.positions.loc[date, 'Quantity'] = self._current_quantity
                self.positions.loc[date, 'Price'] = adjusted_price
                self.entry_price = None
                self.avg_entry_price = None
                self.total_entry_value = 0.0
                self.last_trade_date = date
            else:
                self.logger._log("DEBUG", f"Skipped {action}: insufficient volume {max_tradeable_volume}", date, {})
                self.skipped_trades += 1
        elif action == 'Cover':
            cover_quantity = min(-self._current_quantity, max_tradeable_volume)
            cost = cover_quantity * adjusted_price
            fees = self._calculate_fees(cost, 'Cover')
            if cost + fees <= self.cash and cover_quantity > 0:
                entry_trade = self.trades[(self.trades['Action'] == 'Short') & (self.trades['Date'] < date)].iloc[-1]
                profit = (entry_trade['Price'] - adjusted_price) * cover_quantity
                net_profit = profit - (fees + entry_trade['Fees'])
                self.cash -= cost + fees
                self._current_quantity += cover_quantity
                trade_data = {
                    'trade_id': self.trade_counter,
                    'entry_trade_id': entry_trade['trade_id'],
                    'Date': date,
                    'Symbol': symbol,
                    'Action': 'Cover',
                    'Quantity': cover_quantity,
                    'Price': adjusted_price,
                    'Value': cost,
                    'Fees': fees,
                    'NetProfit': net_profit,
                    'Reason': reason
                }
                self.trades = pd.concat([self.trades, pd.DataFrame([trade_data])], ignore_index=True)
                self.trade_counter += 1
                self.logger.log_trade(date, 'Cover', cover_quantity, adjusted_price, cost + fees, symbol=symbol, fees=fees, net_profit=net_profit)
                self.logger._log("DEBUG", f"{reason}: covered {cover_quantity} shares at {adjusted_price}, fees: {fees:.2f}, net profit: {net_profit:.2f}", date, {})
                if net_profit < 0:
                    self.logger._log("INFO", f"Losing trade: {action} at {adjusted_price}, net profit: {net_profit:.2f}", date, {})
                self.positions.loc[date, 'Quantity'] = self._current_quantity
                self.positions.loc[date, 'Price'] = adjusted_price
                self.entry_price = None
                self.avg_entry_price = None
                self.total_entry_value = 0.0
                self.last_trade_date = date
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
        stop_loss_trades = self.trades[self.trades['NetProfit'].notna() & self.trades['Reason'].str.contains("Stop-loss", na=False)]
        take_profit_trades = self.trades[self.trades['NetProfit'].notna() & self.trades['Reason'].str.contains("Take-profit", na=False)]
        print(f"Stop-Loss Triggers: {len(stop_loss_trades)}")
        print(f"Take-Profit Triggers: {len(take_profit_trades)}")
        avg_position_size = self.trades['Quantity'].mean() if not self.trades.empty else 0
        avg_fees = self.trades['Fees'].mean() if not self.trades.empty else 0
        print(f"Average Position Size: {avg_position_size:.2f}")
        print(f"Average Fees per Trade: {avg_fees:.2f}")
        return {
            'portfolio_value': self.portfolio_value,
            'trades': self.trades,
            'final_cash': self.cash,
            'final_holdings': self._current_quantity,
            'metrics': metrics_dict,
            'skipped_trades': self.skipped_trades,
            'stop_loss_triggers': len(stop_loss_trades),
            'take_profit_triggers': len(take_profit_trades),
            'avg_position_size': avg_position_size,
            'avg_fees': avg_fees
        }

    def execute_trades(self):
        raise NotImplementedError("Batch trade execution is deprecated. Use process_bar for sequential processing.")