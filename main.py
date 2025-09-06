import asyncio
import json
import os
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import pyarrow.parquet as pq
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

clients = set()

PARQUET_FILE = os.getenv("PARQUET_FILE", "trades_sample.parquet")
SPEED = float(os.getenv("SPEED", "1.0"))  # 1.0 = realtime, >1 faster
MIN_DELAY = float(os.getenv("MIN_DELAY", "0.0"))  # seconds

async def replay_trades(df: pd.DataFrame):
    """Replay trades to all connected clients, grouping lazily."""
    prev_ts = None
    for ts, group in df.groupby("timestamp", sort=True):
        if prev_ts is not None:
            delta = (ts - prev_ts).total_seconds() / SPEED
            # Enforce a minimum delay for readability
            sleep_s = max(MIN_DELAY, delta)
            if sleep_s > 0:
                await asyncio.sleep(sleep_s)
        prev_ts = ts

        message = json.dumps({
            "type": "trades",
            "timestamp": ts.isoformat(),
            "trades": group.to_dict(orient="records")
        }, default=str)

        for ws in list(clients):
            try:
                await ws.send_text(message)
            except Exception:
                clients.remove(ws)

# We'll store the DataFrame globally after loading
trade_df: pd.DataFrame = pd.DataFrame()

def load_dataframe() -> pd.DataFrame:
    """Load and sort trades from Parquet without grouping."""
    print(f"DEBUG: loading parquet file {PARQUET_FILE}")
    table = pq.read_table(PARQUET_FILE)
    df = table.to_pandas()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    print(f"DEBUG: loaded {len(df)} rows")
    return df

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()  # keep alive
    except WebSocketDisconnect:
        clients.remove(ws)

@app.on_event("startup")
async def startup_event():
    global trade_df
    print("DEBUG: startup_event called")
    trade_df = load_dataframe()
    # Start replay in background so startup completes immediately
    asyncio.create_task(replay_trades(trade_df))
    print("DEBUG: replay task started")