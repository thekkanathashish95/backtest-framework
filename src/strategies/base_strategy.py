from abc import ABC, abstractmethod
import pandas as pd

class BaseStrategy(ABC):
    def __init__(self, data_handler):
        self.data_handler = data_handler
        self.data = self.data_handler.data

    @abstractmethod
    def generate_signals(self) -> pd.DataFrame:
        """
        Generate trading signals based on the strategy.
        Returns a DataFrame with 'Signal' column (1: Buy, -1: Sell, 0: Hold).
        """
        pass