from src.strategies.rsi_strategy import RSIStrategy
from src.strategies.macd_strategy import MACDStrategy

# Strategy registry for dynamic strategy selection
STRATEGY_REGISTRY = {
    "RSI": RSIStrategy,
    "MACD": MACDStrategy
}