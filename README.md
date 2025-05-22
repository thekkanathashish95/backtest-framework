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

5. **Run the example**:
   

## Structure

- : Loads OHLCV data from SQLite.
- : RSI strategy implementation.
- : Example script.
- : Configuration file.

## Configuration

The application uses an SQLite database to store and retrieve data. The path to this database can be configured using an environment variable.

-   **`ALGO_DB_PATH`**:
    -   **Purpose**: Specifies the absolute or relative path to the SQLite database file (e.g., `algo_data.db`).
    -   **Default**: If this environment variable is not set, the application will default to using `database/algo_data.db` relative to the project root.
    -   **Example**:
        ```bash
        export ALGO_DB_PATH="/custom/path/to/your/algo_data.db"
        # or for a relative path from where the app is run
        export ALGO_DB_PATH="my_data/algo_data.db"
        ```
    -   **Note**: Ensure the directory for the database file exists and the application has the necessary permissions to read/write to it. If using the default path, you might need to create a `database` directory in the project root.

## Notes

- Data is timezone-aware (, ).
- RSI strategy uses a 14-period RSI with thresholds (Buy: RSI < 30, Sell: RSI > 70).

