import pandas as pd
import numpy as np

class Portfolio:
    def __init__(self, initial_cash: float, data_handler, strategy):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.data_handler = data_handler
        self.strategy = strategy
        # Initialize positions with float dtype
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
        for date, row in signals.iterrows():
            signal = row['Signal']
            price = row['Close']
            symbol = self.data_handler.tradingsymbol

            if signal == 1:  # Buy or Cover Short
                current_quantity = int(self.positions.loc[:date].iloc[-1]['Quantity'])
                if current_quantity < 0:  # Cover short position
                    cover_quantity = -current_quantity  # Buy back all shorted shares
                    cost = cover_quantity * price
                    self.cash -= cost
                    self.positions.loc[date, 'Quantity'] = 0
                    self.positions.loc[date, 'Price'] = price
                    self.trades = pd.concat([self.trades, pd.DataFrame([{
                        'Date': date,
                        'Symbol': symbol,
                        'Action': 'Cover',
                        'Quantity': cover_quantity,
                        'Price': price,
                        'Value': cost
                    }])], ignore_index=True)
                elif current_quantity == 0:  # Buy new long position
                    quantity = int(self.cash // price)
                    if quantity > 0:
                        cost = quantity * price
                        self.cash -= cost
                        self.positions.loc[date, 'Quantity'] += quantity
                        self.positions.loc[date, 'Price'] = price
                        self.trades = pd.concat([self.trades, pd.DataFrame([{
                            'Date': date,
                            'Symbol': symbol,
                            'Action': 'Buy',
                            'Quantity': quantity,
                            'Price': price,
                            'Value': cost
                        }])], ignore_index=True)

            elif signal == -1:  # Sell or Short
                current_quantity = int(self.positions.loc[:date].iloc[-1]['Quantity'])
                if current_quantity > 0:  # Sell existing long position
                    revenue = current_quantity * price
                    self.cash += revenue
                    self.positions.loc[date, 'Quantity'] -= current_quantity
                    self.positions.loc[date, 'Price'] = price
                    self.trades = pd.concat([self.trades, pd.DataFrame([{
                        'Date': date,
                        'Symbol': symbol,
                        'Action': 'Sell',
                        'Quantity': current_quantity,
                        'Price': price,
                        'Value': revenue
                    }])], ignore_index=True)
                elif current_quantity == 0:  # Initiate short position
                    short_quantity = int(self.cash // price)  # Short as many shares as cash allows
                    if short_quantity > 0:
                        short_value = short_quantity * price
                        self.cash += short_value
                        self.positions.loc[date, 'Quantity'] = -short_quantity
                        self.positions.loc[date, 'Price'] = price
                        self.trades = pd.concat([self.trades, pd.DataFrame([{
                            'Date': date,
                            'Symbol': symbol,
                            'Action': 'Short',
                            'Quantity': short_quantity,
                            'Price': price,
                            'Value': short_value
                        }])], ignore_index=True)

            # Update portfolio value
            holdings_value = self.positions.loc[:date].iloc[-1]['Quantity'] * price
            self.portfolio_value.loc[date] = {
                'Cash': self.cash,
                'Holdings': holdings_value,
                'Total': self.cash + holdings_value
            }

    def get_portfolio_summary(self):
        """Return portfolio value and trades."""
        return {
            'portfolio_value': self.portfolio_value,
            'trades': self.trades,
            'final_cash': self.cash,
            'final_holdings': self.positions.iloc[-1]['Quantity']
        }