import logging
import sqlite3
import json
import uuid
from datetime import datetime
from pathlib import Path
import pandas as pd

class TradeLogger:
    def __init__(self, log_file: str, db_path: str):
        self.run_id = str(uuid.uuid4())
        self.db_path = db_path
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
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def log_signal(self, timestamp: pd.Timestamp, signal: int, rsi: float, close: float):
        message = f"Signal: {signal}, RSI: {rsi:.2f}, Close: {close}"
        details = {"Signal": signal, "RSI": rsi, "Close": close}
        self._log("SIGNAL", message, timestamp, details)

    def log_trade(self, timestamp: pd.Timestamp, action: str, quantity: int, price: float, value: float):
        message = f"{action} {quantity} shares at {price}, Value: {value}"
        details = {"Action": action, "Quantity": quantity, "Price": price, "Value": value}
        self._log("TRADE", message, timestamp, details)

    def log_portfolio(self, timestamp: pd.Timestamp, cash: float, holdings: float, total: float, quantity: float):
        message = f"Cash: {cash:.2f}, Holdings: {holdings:.2f}, Total: {total:.2f}, Quantity: {quantity}"
        details = {"Cash": cash, "Holdings": holdings, "Total": total, "Quantity": quantity}
        self._log("PORTFOLIO", message, timestamp, details)

    def _log(self, log_type: str, message: str, timestamp: pd.Timestamp, details: dict):
        # Ensure timestamp is timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize('Asia/Kolkata')
        else:
            timestamp = timestamp.tz_convert('Asia/Kolkata')
        
        # Log to file and Application Insights
        extra = {'run_id': self.run_id, 'log_type': log_type}
        self.logger.info(message, extra=extra)

        # Log to SQLite
        self.cursor.execute(
            """
            INSERT INTO trade_logs (run_id, timestamp, log_type, message, details)
            VALUES (?, ?, ?, ?, ?)
            """,
            (self.run_id, timestamp.isoformat(), log_type, message, json.dumps(details))
        )
        self.conn.commit()

    def close(self):
        self.conn.close()