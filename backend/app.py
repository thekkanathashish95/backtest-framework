from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd
import numpy as np
from typing import List

app = FastAPI()

# Allow CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "/Users/ashishmathew/Documents/Development/AlgoTrader/database/algo_data.db"

@app.get("/runs", response_model=List[dict])
async def get_runs():
    """Fetch all run_ids and their initiated_time."""
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT run_id, initiated_time FROM runs ORDER BY initiated_time DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df.to_dict(orient="records")

@app.get("/signals/{run_id}")
async def get_signals(run_id: str):
    """Fetch signal_logs data for a given run_id."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT timestamp, price, rsi, signal
        FROM signal_logs
        WHERE run_id = ?
        ORDER BY timestamp
    """
    df = pd.read_sql_query(query, conn, params=(run_id,))
    conn.close()
    
    print(f"Fetched {len(df)} signals for run_id {run_id}")
    # Convert timestamp to ISO format and prepare data
    df['timestamp'] = pd.to_datetime(df['timestamp']).apply(lambda x: x.isoformat())
    df = df.where(df.notna(), None)  # Replace NaN with None for JSON compatibility
    # Replace NaN, inf, -inf with None
    df = df.replace([np.nan, np.inf, -np.inf], None)

    # Log any rows with None values to debug
    none_counts = df.isnull().sum()
    print(f"None values in columns: {none_counts.to_dict()}")    
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
    # Convert timestamp to ISO format and prepare data
    df['timestamp'] = pd.to_datetime(df['timestamp']).apply(lambda x: x.isoformat())
    df = df.where(df.notna(), None)  # Replace NaN with None for JSON compatibility
    df = df.replace([np.nan, np.inf, -np.inf], None)

    # Log any rows with None values to debug
    none_counts = df.isnull().sum()
    print(f"None values in columns: {none_counts.to_dict()}")    
    return df.to_dict(orient="records")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)