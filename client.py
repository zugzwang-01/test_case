import asyncio
import json
import websockets

async def main():
    uri = "ws://localhost:8000/ws"
    async with websockets.connect(uri) as ws:
        async for message in ws:
            data = json.loads(message)
            if data.get("type") == "trades":
                ts = data["timestamp"]
                trades = data["trades"]
                print(f"{ts} -> {len(trades)} trades")
            else:
                print("INFO:", data)

if __name__ == "__main__":
    asyncio.run(main())