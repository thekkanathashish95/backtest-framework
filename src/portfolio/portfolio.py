import pandas as pd
import numpy as np
import uuid
from src.logging.trade_logger import TradeLogger
from src.backtest.metrics import Metrics
from typing import Optional

class Portfolio:
    def __init__(self, initial_cash: float, data_handler, strategy, logger: TradeLogger,
                 transaction_costs: dict, slippage_pct: float, buy_cash_pct: float, 
                 short_cash_pct: float, stop_loss_pct: float, take_profit_pct: float):
        self.initial_cash = float(initial_cash)
        self.cash = self.initial_cash
        self.debt = 0.0
        self.data_handler = data_handler
        self.strategy = strategy
        self.logger = logger
        self.transaction_costs = transaction_costs
        self.slippage_pct = slippage_pct
        self.buy_cash_pct = buy_cash_pct
        self.short_cash_pct = short_cash_pct
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.positions = pd.DataFrame(
            index=self.data_handler.data.index,
            columns=['Quantity', 'Price'],
            dtype=float
        ).fillna(0)
        self._trade_list = []
        self.trades = pd.DataFrame(columns=[
            'trade_id', 'position_id', 'Date', 'Symbol', 'Action', 'Quantity', 'Price', 
            'Value', 'Fees', 'NetProfit', 'Reason'
        ])
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
        self.position_queue = []
        self.current_position_id = 0

    def _calculate_fees(self, trade_value: float, action: str) -> float:
        brokerage = min(self.transaction_costs['brokerage_rate'] * trade_value, self.transaction_costs['brokerage_min'])
        transaction = self.transaction_costs['transaction_rate'] * trade_value
        sebi = self.transaction_costs['sebi_rate'] * trade_value
        gst = self.transaction_costs['gst_rate'] * (brokerage + transaction + sebi)
        stt_ctt = self.transaction_costs['stt_ctt_rate'] * trade_value if action in ['Sell', 'Cover'] else 0
        stamp = self.transaction_costs['stamp_rate'] * trade_value if action in ['Buy', 'Short'] else 0
        return brokerage + transaction + sebi + gst + stt_ctt + stamp

    def _update_entry_price(self, new_quantity: int, new_price: float):
        if new_quantity == 0:
            return
        fees = self._calculate_fees(abs(new_quantity) * new_price, 'Buy' if new_quantity > 0 else 'Short')
        self.position_queue.append({
            'quantity': new_quantity,
            'entry_price': new_price,
            'fees': fees
        })

    def _calculate_exit_profit(self, date: pd.Timestamp, action: str, abs_quantity: int, exit_price: float) -> tuple[float, float]:
        remaining = abs_quantity
        total_profit = 0.0
        total_entry_fees = 0.0
        exit_fees = self._calculate_fees(abs_quantity * exit_price, action)
        
        # Process FIFO queue to calculate profit/loss from closed positions.
        # `remaining` is the quantity of the current exit trade still to be matched with entries.
        # `position_queue` stores entries as dicts: {'quantity': signed_int, 'entry_price': float, 'fees': float}
        while remaining > 0 and self.position_queue:
            entry = self.position_queue[0]  # Get the oldest entry (FIFO)
            
            # `entry_qty` is the absolute quantity of the current entry in the queue.
            # `qty_to_use` is the amount of this entry that will be consumed by the current exit trade.
            # It's the minimum of what's left to close (`remaining`) and what this entry offers (`entry_qty`).
            # Both `remaining` and `entry_qty` are positive at this point.
            entry_qty = abs(entry['quantity'])
            if entry_qty == 0: # Should not happen if entries are added correctly
                self.logger._log("ERROR", f"Skipping zero-quantity entry in position_queue: {entry}", date, {})
                self.position_queue.pop(0) # Remove problematic entry
                continue

            qty_to_use = min(remaining, entry_qty)
            
            # Calculate profit based on the action (Sell closes long, Cover closes short)
            # entry['quantity'] is signed, so entry['entry_price'] is for that direction.
            if action == 'Sell': # Selling a long position
                profit = (exit_price - entry['entry_price']) * qty_to_use
            else: # Covering a short position (action == 'Cover')
                profit = (entry['entry_price'] - exit_price) * qty_to_use
            
            total_profit += profit
            # Pro-rate the entry fees for the portion of the entry being closed.
            total_entry_fees += (qty_to_use / entry_qty) * entry['fees']
            remaining -= qty_to_use # Reduce the quantity yet to be closed for this exit trade.
            
            # Update or remove the entry from the queue
            if qty_to_use == entry_qty:
                # Entire entry was consumed
                self.position_queue.pop(0)
            else:
                # Partial entry was consumed, update its quantity and fees.
                # entry['quantity'] is signed. qty_to_use is positive.
                if entry['quantity'] > 0: # Long entry
                    entry['quantity'] -= qty_to_use
                else: # Short entry
                    entry['quantity'] += qty_to_use
                # Adjust fees proportionally to the remaining quantity.
                entry['fees'] *= (abs(entry['quantity']) / entry_qty) # (new_abs_qty / old_abs_qty)
        
        # If remaining > 0, it means the position_queue was exhausted before the full
        # quantity of the exit trade (`abs_quantity`) could be matched.
        # This indicates an inconsistency: tried to close more than was open according to the queue.
        # This should ideally be prevented by upstream logic ensuring `_current_quantity` matches the queue sum.
        if remaining > 0:
            # This error is critical as it implies a flaw in position state management.
            # The quantity to close (`abs_quantity`) was derived from `_current_quantity` (or user input),
            # which was apparently larger than the sum of quantities in `position_queue`.
            error_msg = (
                f"Failed to close {remaining} units of {abs_quantity} total for action {action}. "
                f"Position queue exhausted. Current queue: {self.position_queue}. "
                f"_current_quantity at trade execution start: {self._current_quantity + remaining if action in ['Sell', 'Cover'] else self._current_quantity - remaining} (estimated)" # Attempt to reconstruct
            )
            self.logger._log("CRITICAL", error_msg, pd.Timestamp.now(), {}) # Using current time for log if date is problematic
            raise ValueError(error_msg)
        
        return total_profit - (total_entry_fees + exit_fees), exit_fees

    def _validate_position(self, date: pd.Timestamp):
        # Calculates the sum of signed quantities from all entries in the position_queue.
        calc_quantity = sum(entry['quantity'] for entry in self.position_queue)
        
        # This sum should always match self._current_quantity, which is the official net position.
        if calc_quantity != self._current_quantity:
            # Log detailed information if a mismatch is found.
            # This is a critical state inconsistency.
            log_msg = (
                f"Position mismatch detected at {date}: "
                f"Sum of quantities in position_queue ({calc_quantity}) "
                f"does not match _current_quantity ({self._current_quantity}). "
                f"Position queue details: {self.position_queue}"
            )
            self.logger._log("ERROR", log_msg, date, {})
            self.skipped_trades += 1
            # Depending on desired robustness, could raise an error to halt backtest:
            # raise RuntimeError(log_msg)

    def _execute_trade(self, date: pd.Timestamp, price: float, symbol: str, quantity: int, 
                      action: str, reason: str, max_tradeable_volume: float, force_close: bool = False) -> bool:
        if quantity == 0:
            self.logger._log("DEBUG", f"Skipped {action}: zero quantity", date, {})
            return False
        if not isinstance(quantity, int):
            self.logger._log("ERROR", f"Non-integer quantity for {action}: {quantity}", date, {})
            self.skipped_trades += 1
            return False
        if not isinstance(max_tradeable_volume, (int, float)) or max_tradeable_volume < 0:
            self.logger._log("ERROR", f"Invalid max_tradeable_volume: {max_tradeable_volume}", date, {})
            self.skipped_trades += 1
            return False
        # If max_tradeable_volume is a float (and not infinity), it's rounded down (truncated)
        # to the nearest whole number. This is a conservative approach to ensure that the
        # trade quantity derived from it does not exceed actual tradable limits,
        # as fractional shares are typically not supported by exchanges for many instruments.
        if isinstance(max_tradeable_volume, float) and not np.isinf(max_tradeable_volume):
            self.logger._log("WARNING", f"Non-integer max_tradeable_volume {max_tradeable_volume}, rounding down to {int(max_tradeable_volume)}", date, {})
            max_tradeable_volume = int(max_tradeable_volume)
        if not price > 0 or np.isnan(price):
            self.logger._log("ERROR", f"Invalid price for {action}: {price}", date, {})
            self.skipped_trades += 1
            return False

        # Adjust price for slippage
        adjusted_price = price
        if action in ['Buy', 'Cover']:
            adjusted_price *= (1 + self.slippage_pct)
        elif action in ['Sell', 'Short']:
            adjusted_price *= (1 - self.slippage_pct)

        # Adjust quantity for max tradeable volume
        abs_quantity = abs(quantity)
        if not np.isinf(max_tradeable_volume) and abs_quantity > max_tradeable_volume:
            abs_quantity = int(max_tradeable_volume)
            if abs_quantity <= 0:
                self.logger._log("DEBUG", f"Skipped {action}: insufficient volume {max_tradeable_volume}", date, {})
                self.skipped_trades += 1
                return False

        # Calculate trade value and fees
        trade_value = abs_quantity * adjusted_price
        fees = self._calculate_fees(trade_value, action)

        # Handle trade types
        net_profit = None
        final_quantity = 0
        if action == 'Buy':
            if not force_close and trade_value + fees > self.cash:
                self.logger._log("DEBUG", f"Skipped Buy: insufficient cash {self.cash:.2f} for {trade_value + fees:.2f}", date, {})
                self.skipped_trades += 1
                return False
            self.cash -= trade_value + fees
            final_quantity = abs_quantity
        elif action == 'Cover':
            if not force_close and trade_value + fees > self.cash:
                self.logger._log("DEBUG", f"Skipped Cover: insufficient cash {self.cash:.2f} for {trade_value + fees:.2f}", date, {})
                self.skipped_trades += 1
                return False
            self.cash -= trade_value + fees
            final_quantity = abs_quantity
        elif action == 'Sell':
            self.cash += trade_value - fees
            final_quantity = -abs_quantity
        elif action == 'Short':
            if not force_close and self.cash - fees < 0:
                self.logger._log("DEBUG", f"Skipped Short: insufficient cash {self.cash:.2f} after fees {fees:.2f}", date, {})
                self.skipped_trades += 1
                return False
            self.cash += trade_value - fees
            final_quantity = -abs_quantity

        if self.cash < 0 and not force_close:
            self.logger._log("CRITICAL", f"Negative cash after {action}: {self.cash:.2f}. Stopping backtest.", date, {})
            self.skipped_trades += 1
            return False

        # Update position
        old_quantity = self._current_quantity
        self._current_quantity += final_quantity

        trade_id = str(uuid.uuid4())
        if old_quantity == 0:
            self.current_position_id += 1
        position_id = self.current_position_id

        if (old_quantity == 0) or (old_quantity * final_quantity > 0):
            self._update_entry_price(final_quantity, adjusted_price)
        else:
            net_profit, fees = self._calculate_exit_profit(action, abs_quantity, adjusted_price)
            if self._current_quantity == 0:
                self.current_position_id += 1

        # Validate position consistency
        self._validate_position(date)

        # Record the trade
        self.positions.loc[date, 'Quantity'] = self._current_quantity
        self.positions.loc[date, 'Price'] = adjusted_price
        trade_record = {
            'trade_id': trade_id,
            'position_id': position_id,
            'Date': date.tz_convert('Asia/Kolkata') if date.tzinfo else date.tz_localize('Asia/Kolkata'),
            'Symbol': symbol,
            'Action': action,
            'Quantity': abs_quantity,
            'Price': adjusted_price,
            'Value': trade_value,
            'Fees': fees,
            'NetProfit': net_profit,
            'Reason': reason
        }
        self._trade_list.append(trade_record)

        # Log the trade
        value = trade_value + fees if action in ['Buy', 'Cover'] else trade_value - fees
        self.logger.log_trade(date, action, abs_quantity, adjusted_price, value, symbol=symbol, fees=fees, net_profit=net_profit, reason=reason, trade_id=trade_id, position_id=position_id)
        profit_str = f", net profit: {net_profit:.2f}" if net_profit is not None else ""
        self.logger._log("DEBUG", f"Triggered {reason.lower()}: {action} {abs_quantity} shares at {adjusted_price:.2f}, fees: {fees:.2f}{profit_str}", date, {})
        if net_profit is not None and net_profit < 0:
            self.logger._log("INFO", f"Losing trade: {action} at {adjusted_price:.2f}, net profit: {net_profit:.2f}", date, {})

        self.last_trade_date = date
        return True

    def process_bar(self, date: pd.Timestamp) -> None:
        if date not in self.data_handler.data.index:
            self.logger._log("DEBUG", f"Skipping {date}: not in data index", date, {})
            return

        price = self.data_handler.data.loc[date, 'Close']
        symbol = self.data_handler.tradingsymbol
        max_tradeable_volume = self.data_handler.data.loc[date, 'MaxTradeableVolume'] if 'MaxTradeableVolume' in self.data_handler.data.columns else float('inf')

        # Override max_tradeable_volume for final bar
        if date == self.data_handler.data.index[-1]:
            max_tradeable_volume = float('inf')

        if self.cash < 0.1 * self.initial_cash:
            self.logger._log("WARNING", f"Low cash: {self.cash:.2f} (<10% of initial)", date, {})

        # Apply stop-loss and take-profit
        if self._current_quantity != 0 and self.position_queue:
            total_quantity = sum(abs(entry['quantity']) for entry in self.position_queue)
            if total_quantity <= 0:
                self.logger._log("ERROR", "Zero/negative position quantity", date, {})
                return
            avg_entry_price = sum(abs(entry['quantity']) * entry['entry_price'] for entry in self.position_queue) / total_quantity
            price_change = (price - avg_entry_price) / avg_entry_price
            if self._current_quantity > 0:
                if price_change <= -self.stop_loss_pct:
                    self._execute_trade(date, price, symbol, -self._current_quantity, 'Sell', 'stop-loss', max_tradeable_volume)
                elif price_change >= self.take_profit_pct:
                    self._execute_trade(date, price, symbol, -self._current_quantity, 'Sell', 'take-profit', max_tradeable_volume)
            elif self._current_quantity < 0:
                if price_change >= self.stop_loss_pct:
                    self._execute_trade(date, price, symbol, -self._current_quantity, 'Cover', 'stop-loss', max_tradeable_volume)
                elif price_change <= -self.take_profit_pct:
                    self._execute_trade(date, price, symbol, -self._current_quantity, 'Cover', 'take-profit', max_tradeable_volume)

        # Close positions on final bar
        if date == self.data_handler.data.index[-1] and self._current_quantity != 0:
            action = 'Sell' if self._current_quantity > 0 else 'Cover'
            self._execute_trade(date, price, symbol, -self._current_quantity, action, 'end-of-backtest', max_tradeable_volume, force_close=True)

        # Generate signal
        signal = self.strategy.generate_signal(date, self)
        if signal is None:
            self._update_portfolio_value(date, price)
            return
        
        # Check cooldown period
        cooldown_minutes = getattr(self.strategy, 'cooldown_minutes', 0)
        if self.last_trade_date is not None and cooldown_minutes > 0:
            time_since_last_trade = (date - self.last_trade_date).total_seconds() / 60
            if time_since_last_trade < cooldown_minutes:
                self.logger._log("DEBUG", f"Skipped signal {signal}: within {cooldown_minutes}-minute cooldown period (time since last trade: {time_since_last_trade:.2f} minutes)", date, {})
                self._update_portfolio_value(date, price)
                return

        # Execute trade based on signal
        if signal == 1:
            if self._current_quantity < 0:
                self._execute_trade(date, price, symbol, -self._current_quantity, 'Cover', 'strategy-cover', max_tradeable_volume)
            elif self._current_quantity == 0:
                max_shares = (self.buy_cash_pct * self.cash) // price
                quantity = min(int(np.floor(max_shares)), max_tradeable_volume)
                self._execute_trade(date, price, symbol, quantity, 'Buy', 'strategy-buy', max_tradeable_volume)
            else:
                self.logger._log("DEBUG", f"Skipped Buy: already holding {self._current_quantity}", date, {})
        elif signal == -1:
            if self._current_quantity > 0:
                self._execute_trade(date, price, symbol, -self._current_quantity, 'Sell', 'strategy-sell', max_tradeable_volume)
            elif self._current_quantity == 0:
                max_shares = (self.short_cash_pct * self.cash) // price
                quantity = min(int(np.floor(max_shares)), max_tradeable_volume)
                self._execute_trade(date, price, symbol, -quantity, 'Short', 'strategy-short', max_tradeable_volume)
            else:
                self.logger._log("DEBUG", f"Skipped Short: already short {self._current_quantity}", date, {})

        self._update_portfolio_value(date, price)

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
        # Convert trade list to DataFrame
        if self._trade_list:
            self.trades = pd.DataFrame(self._trade_list)
            self.trades['Date'] = pd.to_datetime(self.trades['Date'])
            self.trades = self.trades[['trade_id', 'position_id', 'Date', 'Symbol', 'Action', 'Quantity', 
                                       'Price', 'Value', 'Fees', 'NetProfit', 'Reason']]
        else:
            self.trades = pd.DataFrame(columns=[
                'trade_id', 'position_id', 'Date', 'Symbol', 'Action', 'Quantity', 
                'Price', 'Value', 'Fees', 'NetProfit', 'Reason'
            ])

        print("\nNon-zero Positions:")
        print(self.positions[self.positions['Quantity'] != 0])
        metrics = Metrics(self.portfolio_value, self.trades, self.initial_cash)
        metrics_dict = metrics.get_metrics()
        metrics_dict['debt'] = self.debt
        # Log metrics to database
        self.logger.log_metrics(metrics_dict)
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
            'skipped_trades': self.skipped_trades,
            'debt': self.debt
        }