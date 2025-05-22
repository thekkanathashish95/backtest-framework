import logging
import sqlite3
import json
import uuid
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import Optional, Dict

class TradeLogger:
    def __init__(self, log_file: str, db_path: str, strategy_type: str, strategy_config: Dict):
        self.run_id = str(uuid.uuid4())
        self.run_initiated_time = datetime.now().astimezone()
        self.db_path = db_path
        self.strategy_type = strategy_type
        self.strategy_config = strategy_config
        self.logger = logging.getLogger(f"TradeLogger_{self.run_id}")
        self.logger.setLevel(logging.INFO)

        # File handler
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(
            '[%(asctime)s][%(run_id)s][%(log_type)s] %(message)s'
        ))
        self.logger.addHandler(file_handler)

        # SQLite connection
        self.conn = sqlite3.connect(self.db_path, timeout=10)
        self.cursor = self.conn.cursor()

        # Log run initiation to runs table
        self._log_run_initiation()

    def _log_run_initiation(self):
        """Log run metadata to the runs table."""
        try:
            self.cursor.execute(
                """
                INSERT INTO runs (run_id, initiated_time, strategy_type, strategy_config)
                VALUES (?, ?, ?, ?)
                """,
                (self.run_id, self.run_initiated_time.isoformat(), self.strategy_type, json.dumps(self.strategy_config))
            )
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Failed to log run initiation: {e}", extra={'run_id': self.run_id, 'log_type': 'ERROR'})
            raise

    def log_signal(self, timestamp: pd.Timestamp, signal: int, indicators: Dict, price: float):
        message = f"Signal: {signal}, Indicators: {json.dumps(indicators)}, Price: {price:.2f}"
        details = {"Signal": signal, "Indicators": indicators, "Price": price}
        self._log("SIGNAL", message, timestamp, details)

    def log_signal_data(self, timestamp: pd.Timestamp, price: float, indicators: Dict, signal: Optional[int]):
        """Log indicator data and signal to signal_logs table."""
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize('Asia/Kolkata')
        else:
            timestamp = timestamp.tz_convert('Asia/Kolkata')
        
        message = f"Signal Data: Price: {price:.2f}, Indicators: {json.dumps(indicators)}, Signal: {signal}"
        details = {"Price": price, "Indicators": indicators, "Signal": signal}
        self._log("SIGNAL_DATA", message, timestamp, details)

        try:
            self.cursor.execute(
                """
                INSERT INTO signal_logs (run_id, timestamp, price, strategy_type, indicators, signal)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, timestamp.isoformat(), price, self.strategy_type, json.dumps(indicators), signal)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Failed to log signal data: {e}", extra={'run_id': self.run_id, 'log_type': 'ERROR'})
            raise

    def log_trade(self, timestamp: pd.Timestamp, action: str, quantity: int, price: float, 
                  value: float, symbol: str = "ADANIENT", fees: float = 0.0, 
                  net_profit: Optional[float] = None, reason: Optional[str] = None,
                  trade_id: Optional[str] = None, position_id: Optional[int] = None):
        message = f"{action} {quantity} shares at {price}, Value: {value}"
        details = {
            "Action": action, 
            "Quantity": quantity, 
            "Price": price, 
            "Value": value, 
            "Symbol": symbol, 
            "Fees": fees, 
            "NetProfit": net_profit,
            "Reason": reason,
            "TradeID": trade_id,
            "PositionID": position_id
        }
        self._log("TRADE", message, timestamp, details)

        # Log to trades table with correct column order
        try:
            self.cursor.execute(
                """
                INSERT INTO trades (run_id, trade_id, position_id, timestamp, symbol, action, quantity, price, value, fees, net_profit, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, trade_id, position_id, timestamp.isoformat(), symbol, action, quantity, price, value, fees, net_profit, reason)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Failed to log trade: {e}", extra={'run_id': self.run_id, 'log_type': 'ERROR'})
            raise

    def log_portfolio(self, timestamp: pd.Timestamp, cash: float, holdings: float, total: float, quantity: float):
        message = f"Cash: {cash:.2f}, Holdings: {holdings:.2f}, Total: {total:.2f}, Quantity: {quantity}"
        details = {"Cash": cash, "Holdings": holdings, "Total": total, "Quantity": quantity}
        self._log("PORTFOLIO", message, timestamp, details)

        # Log to portfolio_logs
        try:
            self.cursor.execute(
                """
                INSERT INTO portfolio_logs (run_id, date, cash, holdings, total, quantity)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, timestamp.isoformat(), cash, holdings, total, int(quantity))
            )
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Failed to log portfolio: {e}", extra={'run_id': self.run_id, 'log_type': 'ERROR'})
            raise

    def log_metrics(self, metrics: Dict[str, any]):
        """Log performance metrics to the metrics table."""
        try:
            self.cursor.execute(
                """
                INSERT INTO metrics (
                    run_id, annualized_return, max_drawdown, max_drawdown_start, max_drawdown_end,
                    win_rate, sharpe_ratio, sortino_ratio, calmar_ratio, profit_factor,
                    total_trades, avg_trade_duration
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.run_id,
                    metrics.get('Annualized Return (%)'),
                    metrics.get('Max Drawdown (%)'),
                    metrics.get('Max Drawdown Start').isoformat() if metrics.get('Max Drawdown Start') else None,
                    metrics.get('Max Drawdown End').isoformat() if metrics.get('Max Drawdown End') else None,
                    metrics.get('Win Rate (%)'),
                    metrics.get('Sharpe Ratio'),
                    metrics.get('Sortino Ratio'),
                    metrics.get('Calmar Ratio'),
                    metrics.get('Profit Factor'),
                    metrics.get('Total Trades'),
                    metrics.get('Avg Trade Duration (min)')
                )
            )
            self.conn.commit()
            self.logger.info(f"Logged metrics for run_id {self.run_id}", extra={'run_id': self.run_id, 'log_type': 'METRICS'})
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Failed to log metrics: {e}", extra={'run_id': self.run_id, 'log_type': 'ERROR'})
            raise

    def _log(self, log_type: str, message: str, timestamp: pd.Timestamp, details: dict):
        # Ensure timestamp is timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize('Asia/Kolkata')
        else:
            timestamp = timestamp.tz_convert('Asia/Kolkata')
        
        # Log to file
        extra = {'run_id': self.run_id, 'log_type': log_type}
        self.logger.info(message, extra=extra)

        # Log to SQLite
        try:
            self.cursor.execute(
                """
                INSERT INTO trade_logs (run_id, log_type, timestamp, message, details)
                VALUES (?, ?, ?, ?, ?)
                """,
                (self.run_id, log_type, timestamp.isoformat(), message, json.dumps(details))
            )
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Failed to log to trade_logs: {e}", extra={'run_id': self.run_id, 'log_type': 'ERROR'})
            raise

    def close(self):
        if self.conn:
            self.conn.close()