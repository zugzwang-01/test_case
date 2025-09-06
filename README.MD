# Trade Replay WebSocket Service - TEST
This service replays historical trades from a Parquet file over a WebSocket connection.

## Features:
- Reads `.parquet` trade data
- Sends trades in chronological order
- Groups trades with the same timestamp into one message
- Adjustable replay speed

## Usage
1. Install dependencies:
pip install -r requirements.txt