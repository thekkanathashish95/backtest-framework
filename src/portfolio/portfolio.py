import pandas as pd
import numpy as np
from src.logging.trade_logger import TradeLogger

class Portfolio:
    def __init__(self, initial_cash: float, data_handler, strategy, logger: TradeLogger):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.data_handler = data_handler
        self.strategy = strategy
        self.logger = logger
        self.positions = pd.DataFrame(
            index=self.data_handler.data.index,
            columns=['Quantity', 'Price'],
            dtype=float
        ).fillna(0)
        # Initialize trades with explicit dtypes
        self.trades = pd.DataFrame({
            'Date': pd.Series(dtype='datetime64[ns, Asia/Kolkata]'),
            'Symbol': pd.Series(dtype=str),
            'Action': pd.Series(dtype=str),
            'Quantity': pd.Series(dtype=int),
            'Price': pd.Series(dtype=float),
            'Value': pd.Series(dtype=float)
        })
        # Initialize portfolio_value with float dtype
        self.portfolio_value = pd.DataFrame(
            index=self.data_handler.data.index,
            columns=['Cash', 'Holdings', 'Total'],
            dtype=float
        ).fillna(0)

    def execute_trades(self):
        """Execute trades based on strategy signals, including short selling."""
        signals = self.strategy.generate_signals()
        current_quantity = 0  # Track quantity explicitly

        # Validate index alignment
        assert signals.index.equals(self.positions.index), "Signal and position indices misaligned"

        for date, row in signals.iterrows():
            signal = row['Signal']
            price = row['Close']
            symbol = self.data_handler.tradingsymbol

            # Log signal with context
            self.logger._log(
                "SIGNAL",
                f"Signal: {signal}, RSI: {row.get('RSI', float('nan')):.2f}, Close: {price}, Current Quantity: {current_quantity}, Cash: {self.cash:.2f}",
                date,
                {"Signal": signal, "RSI": row.get('RSI', float('nan')), "Close": price, "CurrentQuantity": current_quantity, "Cash": self.cash}
            )

            if signal == 1:  # Buy or Cover Short
                if current_quantity < 0:  # Cover short position
                    cover_quantity = -current_quantity  # Buy back all shorted shares
                    cost = cover_quantity * price
                    self.cash -= cost
                    current_quantity = 0
                    self.positions.loc[date, 'Quantity'] = current_quantity
                    self.positions.loc[date, 'Price'] = price
                    self.trades = pd.concat([self.trades, pd.DataFrame([{
                        'Date': date,
                        'Symbol': symbol,
                        'Action': 'Cover',
                        'Quantity': cover_quantity,
                        'Price': price,
                        'Value': cost
                    }])], ignore_index=True)
                    self.logger.log_trade(date, 'Cover', cover_quantity, price, cost)
                elif current_quantity == 0:  # Buy new long position
                    quantity = int(self.cash // price)
                    if quantity > 0:
                        cost = quantity * price
                        self.cash -= cost
                        current_quantity += quantity
                        self.positions.loc[date, 'Quantity'] = current_quantity
                        self.positions.loc[date, 'Price'] = price
                        self.trades = pd.concat([self.trades, pd.DataFrame([{
                            'Date': date,
                            'Symbol': symbol,
                            'Action': 'Buy',
                            'Quantity': quantity,
                            'Price': price,
                            'Value': cost
                        }])], ignore_index=True)
                        self.logger.log_trade(date, 'Buy', quantity, price, cost)
                else:
                    self.logger._log("DEBUG", f"Skipped Buy signal: already holding {current_quantity}", date, {})

            elif signal == -1:  # Sell or Short
                if current_quantity > 0:  # Sell existing long position
                    revenue = current_quantity * price
                    self.cash += revenue
                    self.positions.loc[date, 'Quantity'] = 0
                    self.trades = pd.concat([self.trades, pd.DataFrame([{
                        'Date': date,
                        'Symbol': symbol,
                        'Action': 'Sell',
                        'Quantity': current_quantity,
                        'Price': price,
                        'Value': revenue
                    }])], ignore_index=True)
                    self.logger.log_trade(date, 'Sell', current_quantity, price, revenue)
                    current_quantity = 0
                elif current_quantity == 0:  # Initiate short position
                    short_quantity = int((0.5 * self.cash) // price)  # Use 50% of cash for shorts
                    if short_quantity > 0:
                        short_value = short_quantity * price
                        self.cash += short_value
                        current_quantity = -short_quantity
                        self.positions.loc[date, 'Quantity'] = current_quantity
                        self.positions.loc[date, 'Price'] = price
                        self.trades = pd.concat([self.trades, pd.DataFrame([{
                            'Date': date,
                            'Symbol': symbol,
                            'Action': 'Short',
                            'Quantity': short_quantity,
                            'Price': price,
                            'Value': short_value
                        }])], ignore_index=True)
                        self.logger.log_trade(date, 'Short', short_quantity, price, short_value)
                    else:
                        self.logger._log("DEBUG", f"Skipped Short signal: insufficient cash {self.cash:.2f} for price {price}", date, {})
                else:
                    self.logger._log("DEBUG", f"Skipped Sell signal: already short {current_quantity}", date, {})

            # Update portfolio value
            holdings_value = current_quantity * price
            self.portfolio_value.loc[date] = {
                'Cash': self.cash,
                'Holdings': holdings_value,
                'Total': self.cash + holdings_value
            }
            self.logger.log_portfolio(date, self.cash, holdings_value, self.cash + holdings_value, current_quantity)

    def get_portfolio_summary(self):
        """Return portfolio value and trades."""
        print("\nNon-zero Positions:")
        print(self.positions[self.positions['Quantity'] != 0])
        return {
            'portfolio_value': self.portfolio_value,
            'trades': self.trades,
            'final_cash': self.cash,
            'final_holdings': self.positions.iloc[-1]['Quantity']
        }