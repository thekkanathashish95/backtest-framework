import pandas as pd
import numpy as np
from typing import Optional

class StressTester:
    def __init__(self, data: pd.DataFrame, seed: Optional[int] = None):
        """
        Apply stress tests to OHLCV data.
        Args:
            data: DataFrame with OHLCV columns.
            seed: Random seed for reproducibility.
        """
        self.data = data.copy()
        self.rng = np.random.default_rng(seed)

    def apply_price_shock(self, shock_factor: float = 0.1, probability: float = 0.01) -> pd.DataFrame:
        """
        Apply random price shocks to simulate extreme movements.
        Args:
            shock_factor: Magnitude of shock (e.g., 0.1 for ±10%).
            probability: Probability of shock per bar.
        Returns:
            DataFrame with shocked prices.
        """
        shocked_data = self.data.copy()
        mask = self.rng.random(len(shocked_data)) < probability
        shock = self.rng.uniform(-shock_factor, shock_factor, len(shocked_data))
        shocked_data.loc[mask, ['Open', 'High', 'Low', 'Close']] *= (1 + shock[mask])
        return shocked_data

    def apply_liquidity_constraint(self, max_volume_pct: float = 0.1) -> pd.DataFrame:
        """
        Add max tradeable volume column based on bar volume.
        Args:
            max_volume_pct: Max percentage of bar volume tradeable (e.g., 0.1 for 10%).
        Returns:
            DataFrame with 'MaxTradeableVolume' column.
        """
        data = self.data.copy()
        data['MaxTradeableVolume'] = data['Volume'] * max_volume_pct
        return data