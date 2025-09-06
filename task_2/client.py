import asyncio
import json
import sys
from datetime import datetime

import websockets

# WebSocket endpoint of your server
URI = "ws://127.0.0.1:8000/ws"

# Required trade fields for your dataset
REQUIRED_FIELDS = {"timestamp", "price", "volume", "ticker"}

async def main():
    last_ts = None
    total_msgs = 0
    total_trades = 0

    async with websockets.connect(URI) as ws:
        print(f"Connected to {URI}")
        async for message in ws:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                print("❌ Received non‑JSON message:", message)
                sys.exit(1)

            if data.get("type") != "trades":
                print("ℹ️ Info message:", data)
                continue

            ts_str = data.get("timestamp")
            trades = data.get("trades", [])

            # Validate timestamp format
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except Exception:
                print(f"❌ Invalid timestamp format: {ts_str}")
                sys.exit(1)

            # Check monotonicity
            if last_ts and ts < last_ts:
                print(f"❌ Timestamp went backwards: {ts} after {last_ts}")
                sys.exit(1)
            last_ts = ts

            # Check all trades share the same timestamp
            for t in trades:
                if t.get("timestamp") != ts_str:
                    print(f"❌ Trade timestamp mismatch in batch: {t}")
                    sys.exit(1)

            # Check required fields
            for t in trades:
                missing = REQUIRED_FIELDS - t.keys()
                if missing:
                    print(f"❌ Missing fields {missing} in trade: {t}")
                    sys.exit(1)

            total_msgs += 1
            total_trades += len(trades)
            print(f"✅ {ts_str} -> {len(trades)} trades "
                  f"(total msgs: {total_msgs}, total trades: {total_trades})")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Connection or runtime error: {e}")
        sys.exit(1)