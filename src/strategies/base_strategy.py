from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional

class BaseStrategy(ABC):
    def __init__(self, data_handler):
        self.data_handler = data_handler
        self.data = self.data_handler.data

    @abstractmethod
    def generate_signal(self, date: pd.Timestamp, portfolio: 'Portfolio') -> Optional[int]:
        """
        Generate a trading signal for a single data point based on the current date and portfolio state.
        Args:
            date: Timestamp of the current data point (timezone-aware).
            portfolio: Portfolio instance to access current state (e.g., cash, holdings).
        Returns:
            int: Signal (1: Buy, -1: Sell, 0: Hold) or None if no signal is generated.
        """
        pass