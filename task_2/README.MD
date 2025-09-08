# **Task 1 - Trade Replay WebSocket Service**

This service replays historical trades from a Parquet file over a WebSocket connection.

## Features
- Reads `.parquet` trade data
- Streams trades in chronological order
- Groups trades with identical timestamps
- Adjustable speed (`SPEED`) and minimum delay (`MIN_DELAY`)

## Usage
1. Install dependencies:
pip install -r requirements.txt
2. Run: `uvicorn main:app --reload`
3. Run: `python3 client.py` or `wscat -c ws://127.0.0.1:8000/ws` (install globally via `npm install -g wscat` command for CLI WebSocket testing)