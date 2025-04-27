# Backtest Framework

A Python-based backtesting framework for algorithmic trading, supporting 1-minute OHLCV data.

## Setup

1. **Clone the repository**:
   

2. **Create a virtual environment**:
   

3. **Install dependencies**:
   

4. **Database Setup**:
   - The framework uses a SQLite database () with 1-minute OHLCV data.
   - **Schema**: Table  with columns: Fri Apr 25 11:13:25 IST 2025 (TIMESTAMP), , , , , , .
   - Obtain data from a provider (e.g., Yahoo Finance, Alpha Vantage) or use a sample database (not included).
   - Update  with the correct .

5. **Run the example**:
   

## Structure

- : Loads OHLCV data from SQLite.
- : RSI strategy implementation.
- : Example script.
- : Configuration file.

## Notes

- Data is timezone-aware (, ).
- RSI strategy uses a 14-period RSI with thresholds (Buy: RSI < 30, Sell: RSI > 70).

