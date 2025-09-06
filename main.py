import asyncio
import json
import os
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import pyarrow.parquet as pq
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

# Connected WebSocket clients
clients = set()

# Configurable via environment variables
PARQUET_FILE = os.getenv("PARQUET_FILE", "trades_sample.parquet")
SPEED = float(os.getenv("SPEED", "10.0"))  # 1.0 = realtime, >1 faster

def load_batches() -> List[Tuple[datetime, list]]:
    print("DEBUG: starting load_batches()")
    table = pq.read_table(PARQUET_FILE)
    print("DEBUG: parquet read complete")
    df = table.to_pandas()
    print("DEBUG: converted to pandas")
    """Load and group trades by exact timestamp."""
    table = pq.read_table(PARQUET_FILE)
    df = table.to_pandas()

    # Ensure timestamp is datetime with UTC
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Sort chronologically
    df = df.sort_values("timestamp")

    # Group trades with identical timestamps
    batches = []
    for ts, group in df.groupby("timestamp", sort=True):
        trades = group.to_dict(orient="records")
        batches.append((ts, trades))
    return batches

async def replay_trades():
    """Replay trades to all connected clients."""
    batches = load_batches()
    if not batches:
        return

    prev_ts = batches[0][0]
    for ts, trades in batches:
        # Sleep according to time delta / SPEED
        delta = (ts - prev_ts).total_seconds() / SPEED
        if delta > 0:
            await asyncio.sleep(delta)
        prev_ts = ts

        message = json.dumps({
            "type": "trades",
            "timestamp": ts.isoformat(),
            "trades": trades
        }, default=str)

        # Broadcast to all clients
        for ws in list(clients):
            try:
                await ws.send_text(message)
            except Exception:
                clients.remove(ws)

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """Handle WebSocket connections."""
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()  # keep connection alive
    except WebSocketDisconnect:
        clients.remove(ws)

@app.on_event("startup")
async def startup_event():
    print("DEBUG: startup_event called")
    batches = load_batches()
    print(f"DEBUG: loaded {len(batches)} batches")
    asyncio.create_task(replay_trades())
    print("DEBUG: replay task started")