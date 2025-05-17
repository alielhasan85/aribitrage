import asyncio
import json
import websockets
import pandas as pd
from datetime import datetime, timedelta, timezone

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────
PAIR = "BTCUSDT"
CAPITAL = 10_000  # Capital per trade
TARGET_PROFIT_USD = -5  # Minimum profit you want after fees

# Fees (as decimals)
# Binance: maker 0.02% + taker 0.04%
# Bybit:   maker 0.01% + taker 0.055%
TOTAL_FEES_PCT = 0.0002 + 0.0004 + 0.0001 + 0.00055  # 0.125%

# Dynamically calculated minimum spread
MIN_SPREAD_USD = CAPITAL * TOTAL_FEES_PCT + TARGET_PROFIT_USD

# Price storage
prices = {
    "binance": {PAIR: None},
    "bybit": {PAIR: None},
}

# ─────────────────────────────────────────────────────────────
# Arbitrage storage
# ─────────────────────────────────────────────────────────────
arb_df = pd.DataFrame(columns=["time", "pair", "exchange_a", "exchange_b", "price_a", "price_b", "spread", "percent"])
last_dump_time = datetime.now(timezone.utc)

def store_arbitrage_row(exchange_a, exchange_b, price_a, price_b, spread, percent):
    global arb_df
    now = datetime.now(timezone.utc).isoformat()
    new_row = {
        "time": now,
        "pair": PAIR,
        "exchange_a": exchange_a,
        "exchange_b": exchange_b,
        "price_a": price_a,
        "price_b": price_b,
        "spread": spread,
        "percent": percent
    }
    arb_df = pd.concat([arb_df, pd.DataFrame([new_row])], ignore_index=True)

def export_if_needed():
    global arb_df, last_dump_time
    now = datetime.now(timezone.utc)
    if (now - last_dump_time) >= timedelta(hours=1) and not arb_df.empty:
        filename = now.strftime(f"arbitrage_log_{PAIR}_%Y-%m-%d_%H-%M.csv")
        arb_df.to_csv(filename, index=False)
        print(f"✅ Exported {len(arb_df)} rows to {filename}")
        arb_df = pd.DataFrame(columns=["time", "pair", "exchange_a", "exchange_b", "price_a", "price_b", "spread", "percent"])
        last_dump_time = now

# ─────────────────────────────────────────────────────────────
# Binance WebSocket (USDT-M Futures)
# ─────────────────────────────────────────────────────────────
async def binance_ws():
    url = f"wss://fstream.binance.com/ws/{PAIR.lower()}@markPrice"
    while True:
        try:
            async with websockets.connect(url) as ws:
                print(f"✅ Connected to Binance {PAIR}")
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    prices["binance"][PAIR] = float(data["p"])
        except Exception as e:
            print(f"❌ Binance WS error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# ─────────────────────────────────────────────────────────────
# Bybit WebSocket (USDT Perpetual)
# ─────────────────────────────────────────────────────────────
async def bybit_ws():
    url = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            async with websockets.connect(url) as ws:
                print("✅ Connected to Bybit")
                await ws.send(json.dumps({
                    "op": "subscribe",
                    "args": [f"tickers.{PAIR}"]
                }))
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if "data" in data:
                        item = data["data"]
                        last_price = float(item.get("lastPrice", 0))
                        prices["bybit"][PAIR] = last_price
        except Exception as e:
            print(f"❌ Bybit WS error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# ─────────────────────────────────────────────────────────────
# Price Comparison
# ─────────────────────────────────────────────────────────────
async def compare_prices():
    while True:
        binance_price = prices["binance"][PAIR]
        bybit_price = prices["bybit"][PAIR]
        now = datetime.now().strftime("%H:%M:%S")

        if binance_price and bybit_price:
            spread = bybit_price - binance_price
            percent = (spread / binance_price) * 100

            print(f"[{now}] {PAIR} | binance: ${binance_price:.2f} | bybit: ${bybit_price:.2f} | Spread: ${spread:.2f} ({percent:.3f}%)")

            if abs(spread) >= MIN_SPREAD_USD:
                store_arbitrage_row("binance", "bybit", binance_price, bybit_price, spread, percent)

        export_if_needed()
        await asyncio.sleep(0.5)

# ─────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────
async def main():
    await asyncio.gather(
        binance_ws(),
        bybit_ws(),
        compare_prices()
    )

if __name__ == "__main__":
    asyncio.run(main())
