import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd
import numpy as np
import json
from typing import List
import logging

# Initialize logger
logger = logging.getLogger(__name__)

# General Note on NaN/inf values:
# NaN (Not a Number) or inf (infinity) values can appear in DataFrames
# due to various reasons:
# 1. Upstream data issues: Missing data from the database or source files.
# 2. Calculation errors:
#    - Division by zero (e.g., in financial ratios like Sharpe if std dev is 0, or if a denominator in a custom metric is zero).
#    - Mathematical operations that are undefined (e.g., log of a negative number).
# 3. Data type issues: Operations on incompatible dtypes.
# These values are converted to 'None' before JSON serialization to prevent errors,
# but their occurrence (logged as warnings) should be investigated if frequent,
# as they might indicate underlying problems in data generation or calculations.

app = FastAPI()

# Allow CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.environ.get("ALGO_DB_PATH", "database/algo_data.db")

@app.get("/runs", response_model=List[dict])
async def get_runs():
    """Fetch all run_ids and their initiated_time."""
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT run_id, initiated_time, strategy_type, strategy_config FROM runs ORDER BY initiated_time DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    df['strategy_config'] = df['strategy_config'].apply(lambda x: json.loads(x) if x else {})
    return df.to_dict(orient="records")

@app.get("/signals/{run_id}")
async def get_signals(run_id: str):
    """Fetch signal_logs data for a given run_id."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT timestamp, price, strategy_type, indicators, signal
        FROM signal_logs
        WHERE run_id = ?
        ORDER BY timestamp
    """
    df = pd.read_sql_query(query, conn, params=(run_id,))
    conn.close()
    
    print(f"Fetched {len(df)} signals for run_id {run_id}")
    df['timestamp'] = pd.to_datetime(df['timestamp']).apply(lambda x: x.isoformat())
    df['indicators'] = df['indicators'].apply(lambda x: json.loads(x) if x else {})

    # Log NaN and Inf occurrences before replacement
    for col in df.columns:
        # NaN check (safe for all dtypes)
        nan_count = df[col].isna().sum()
        if nan_count > 0:
            logger.warning(f"NaN values found in column '{col}' of get_signals for run_id {run_id}: {nan_count} occurrences.")

        # Inf check
        if pd.api.types.is_numeric_dtype(df[col]):
            inf_count = np.isinf(df[col]).sum()
            if inf_count > 0:
                logger.warning(f"Infinite values (inf, -inf) found in numeric column '{col}' of get_signals for run_id {run_id}: {inf_count} occurrences.")
        elif df[col].dtype == object: # Includes strings, mixed types
            try:
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                if numeric_col.dtype.kind == 'f': # Check inf only if coerced to float
                    inf_count = np.isinf(numeric_col).sum()
                    if inf_count > 0:
                        logger.warning(f"Infinite values (inf, -inf) found in object column '{col}' (after numeric coercion) of get_signals for run_id {run_id}: {inf_count} occurrences.")
            except Exception as e: # Should be rare with errors='coerce'
                logger.debug(f"Could not perform numeric coercion for Inf check on object column '{col}' in get_signals for run_id {run_id}: {e}")
                
    # Simplified replacement of NaN/inf to None
    df = df.where(pd.notna(df), None)
    
    none_counts = df.isnull().sum()
    if none_counts.sum() > 0: # Only print if there are None values
        print(f"None values in columns after conversion (get_signals for {run_id}): {none_counts[none_counts > 0].to_dict()}")
    return df.to_dict(orient="records")

@app.get("/trades/{run_id}")
async def get_trades(run_id: str):
    """Fetch trades data for a given run_id."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT timestamp, symbol, action, quantity, price, value, fees, net_profit, reason
        FROM trades
        WHERE run_id = ?
        ORDER BY timestamp
    """
    df = pd.read_sql_query(query, conn, params=(run_id,))
    conn.close()
    
    print(f"Fetched {len(df)} trades for run_id {run_id}")
    df['timestamp'] = pd.to_datetime(df['timestamp']).apply(lambda x: x.isoformat())

    # Log NaN and Inf occurrences before replacement
    for col in df.columns:
        # NaN check (safe for all dtypes)
        nan_count = df[col].isna().sum()
        if nan_count > 0:
            logger.warning(f"NaN values found in column '{col}' of get_trades for run_id {run_id}: {nan_count} occurrences.")

        # Inf check
        if pd.api.types.is_numeric_dtype(df[col]):
            inf_count = np.isinf(df[col]).sum()
            if inf_count > 0:
                logger.warning(f"Infinite values (inf, -inf) found in numeric column '{col}' of get_trades for run_id {run_id}: {inf_count} occurrences.")
        elif df[col].dtype == object: # Includes strings, mixed types
            try:
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                if numeric_col.dtype.kind == 'f': # Check inf only if coerced to float
                    inf_count = np.isinf(numeric_col).sum()
                    if inf_count > 0:
                        logger.warning(f"Infinite values (inf, -inf) found in object column '{col}' (after numeric coercion) of get_trades for run_id {run_id}: {inf_count} occurrences.")
            except Exception as e: # Should be rare with errors='coerce'
                logger.debug(f"Could not perform numeric coercion for Inf check on object column '{col}' in get_trades for run_id {run_id}: {e}")

    # Simplified replacement of NaN/inf to None
    df = df.where(pd.notna(df), None)

    none_counts = df.isnull().sum()
    if none_counts.sum() > 0: # Only print if there are None values
        print(f"None values in columns after conversion (get_trades for {run_id}): {none_counts[none_counts > 0].to_dict()}")
    return df.to_dict(orient="records")

@app.get("/metrics/{run_id}")
async def get_metrics(run_id: str):
    """Fetch metrics data for a given run_id."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT annualized_return, max_drawdown, max_drawdown_start, max_drawdown_end,
               win_rate, sharpe_ratio, sortino_ratio, calmar_ratio, profit_factor,
               total_trades, avg_trade_duration
        FROM metrics
        WHERE run_id = ?
    """
    df = pd.read_sql_query(query, conn, params=(run_id,))
    conn.close()
    
    print(f"Fetched metrics for run_id {run_id}")
    if df.empty:
        return {}
    
    # Specific datetime conversions before general NaN/inf logging
    df['max_drawdown_start'] = pd.to_datetime(df['max_drawdown_start']).apply(lambda x: x.isoformat() if pd.notna(x) else None)
    df['max_drawdown_end'] = pd.to_datetime(df['max_drawdown_end']).apply(lambda x: x.isoformat() if pd.notna(x) else None)

    # Log NaN and Inf occurrences before replacement
    for col in df.columns:
        # Skip already processed datetime columns that are now strings or None
        if col in ['max_drawdown_start', 'max_drawdown_end'] and df[col].apply(lambda x: isinstance(x, (str, type(None)))).all():
            # Already handled, may log NaNs if original was NaT before conversion to None
            nan_count = df[col].isna().sum() # isna() still works if column became None due to NaT
            if nan_count > 0:
                 logger.warning(f"NaN values (likely from NaT) found in datetime-converted column '{col}' of get_metrics for run_id {run_id}: {nan_count} occurrences.")
            continue

        # NaN check (safe for all dtypes)
        nan_count = df[col].isna().sum()
        if nan_count > 0:
            logger.warning(f"NaN values found in column '{col}' of get_metrics for run_id {run_id}: {nan_count} occurrences.")

        # Inf check
        if pd.api.types.is_numeric_dtype(df[col]):
            inf_count = np.isinf(df[col]).sum()
            if inf_count > 0:
                logger.warning(f"Infinite values (inf, -inf) found in numeric column '{col}' of get_metrics for run_id {run_id}: {inf_count} occurrences.")
        elif df[col].dtype == object: # Includes strings, mixed types
            try:
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                if numeric_col.dtype.kind == 'f': # Check inf only if coerced to float
                    inf_count = np.isinf(numeric_col).sum()
                    if inf_count > 0:
                        logger.warning(f"Infinite values (inf, -inf) found in object column '{col}' (after numeric coercion) of get_metrics for run_id {run_id}: {inf_count} occurrences.")
            except Exception as e: # Should be rare with errors='coerce'
                 logger.debug(f"Could not perform numeric coercion for Inf check on object column '{col}' in get_metrics for run_id {run_id}: {e}")

    # Simplified replacement of NaN/inf to None
    # This handles actual np.nan, np.inf, -np.inf. String 'inf' etc. are not directly handled by pd.notna unless coerced.
    # However, the database query for metrics likely returns numeric types for these.
    df = df.where(pd.notna(df), None)

    none_counts = df.isnull().sum()
    if none_counts.sum() > 0: # Only print if there are None values
        print(f"None values in columns after conversion (get_metrics for {run_id}): {none_counts[none_counts > 0].to_dict()}")
    return df.to_dict(orient="records")[0]

@app.get("/portfolio_final/{run_id}")
async def get_portfolio_final(run_id: str):
    """Fetch the final portfolio value for a given run_id."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT total
        FROM portfolio_logs
        WHERE run_id = ?
        ORDER BY date DESC
        LIMIT 1
    """
    df = pd.read_sql_query(query, conn, params=(run_id,))
    conn.close()
    
    print(f"Fetched final portfolio value for run_id {run_id}")
    if df.empty:
        return {"total": None}
    total = df.iloc[0]['total']
    total = None if pd.isna(total) else float(total)
    return {"total": total}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)