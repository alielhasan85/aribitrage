import asyncio
import json
import websockets
import pandas as pd
from datetime import datetime, timedelta, timezone

# Configuration
MIN_SPREAD_PERCENT = 0.035  # Minimum arbitrage spread to record (covers fees)

# Price tracking
prices = {
    "binance": {"BTCUSDT": None, "ETHUSDT": None},
    "bybit": {"BTCUSDT": None, "ETHUSDT": None}
}

# Data storage
arb_df = pd.DataFrame(columns=["time", "pair", "binance", "bybit", "spread", "percent"])
last_dump_time = datetime.now(timezone.utc)

# Store qualifying opportunities
def store_arbitrage_row(pair, binance_price, bybit_price, spread, percent):
    global arb_df
    now = datetime.now(timezone.utc).isoformat()
    new_row = {
        "time": now,
        "pair": pair,
        "binance": binance_price,
        "bybit": bybit_price,
        "spread": spread,
        "percent": percent
    }
    arb_df = pd.concat([arb_df, pd.DataFrame([new_row])], ignore_index=True)

# Export hourly
def export_if_needed():
    global arb_df, last_dump_time
    now = datetime.now(timezone.utc)
    if (now - last_dump_time) >= timedelta(hours=1) and not arb_df.empty:
        filename = now.strftime("arbitrage_log_%Y-%m-%d_%H-%M.csv")
        arb_df.to_csv(filename, index=False)
        print(f"✅ Exported {len(arb_df)} rows to {filename}")
        arb_df = pd.DataFrame(columns=["time", "pair", "binance", "bybit", "spread", "percent"])
        last_dump_time = now

# Binance Futures (USDT-M)
async def binance_ws(symbol):
    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice"
    async with websockets.connect(url) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            prices["binance"][symbol] = float(data["p"])

# Bybit Futures (v5)
async def bybit_ws():
    url = "wss://stream.bybit.com/v5/public/linear"
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": ["tickers.BTCUSDT", "tickers.ETHUSDT"]
        }))
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            if "data" in data:
                try:
                    items = [data["data"]] if isinstance(data["data"], dict) else data["data"]
                    for item in items:
                        symbol = item.get("symbol")
                        last_price = float(item.get("lastPrice", 0))
                        if symbol in prices["bybit"]:
                            prices["bybit"][symbol] = last_price
                except:
                    continue

# Compare and log
async def compare_prices():
    while True:
        for pair in ["BTCUSDT", "ETHUSDT"]:
            b_price = prices["binance"][pair]
            y_price = prices["bybit"][pair]

            if b_price and y_price:
                spread = y_price - b_price
                percent = (spread / b_price) * 100
                now = datetime.now().strftime("%H:%M:%S")

                # Debug print (optional)
                print(f"[{now}] {pair} | Binance: ${b_price:.2f} | Bybit: ${y_price:.2f} | Spread: ${spread:.2f} ({percent:.3f}%)")

                # Only store if meaningful
                if abs(percent) >= MIN_SPREAD_PERCENT:
                    store_arbitrage_row(pair, b_price, y_price, spread, percent)

        export_if_needed()
        await asyncio.sleep(0.5)

# Main runner
async def main():
    await asyncio.gather(
        binance_ws("BTCUSDT"),
        binance_ws("ETHUSDT"),
        bybit_ws(),
        compare_prices()
    )

if __name__ == "__main__":
    asyncio.run(main())
