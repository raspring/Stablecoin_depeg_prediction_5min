"""
Collect 5-minute OHLCV data from Binance public API.

No API key required. Covers trading pairs (BTCUSDT, USDCUSDT, etc.).
Note: Binance launched July 2017, so data before that date is unavailable.
"""

import time
from datetime import datetime
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR, STABLECOINS

import requests

BASE_URL = "https://api.binance.com/api/v3"
KLINE_LIMIT = 1000  # Max per request
RATE_LIMIT_DELAY = 0.2  # seconds between requests


def get_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    url = f"{BASE_URL}/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": KLINE_LIMIT,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    time.sleep(RATE_LIMIT_DELAY)
    return response.json()


def collect_pair(
    symbol: str,
    interval: str = "5m",
    start_date: datetime = None,
    end_date: datetime = None,
) -> pd.DataFrame:
    """
    Collect full historical klines for a symbol with pagination.

    Returns DataFrame with columns:
        timestamp, open, high, low, close, volume, quote_volume,
        trades, taker_buy_volume, taker_buy_quote_volume, buy_ratio, spread_proxy
    """
    start_ms = int(start_date.timestamp() * 1000) if start_date else 0
    end_ms = int(end_date.timestamp() * 1000) if end_date else int(datetime.now().timestamp() * 1000)

    all_klines = []
    current = start_ms

    print(f"  Collecting {symbol} {interval}...")

    while current < end_ms:
        klines = get_klines(symbol, interval, current, end_ms)
        if not klines:
            break

        all_klines.extend(klines)
        current = klines[-1][0] + 1  # next ms after last candle

        if len(klines) < KLINE_LIMIT:
            break

        if len(all_klines) % 50000 == 0:
            print(f"    {len(all_klines):,} records...")

    if not all_klines:
        print(f"    No data returned for {symbol}")
        return pd.DataFrame()

    df = pd.DataFrame(all_klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_volume",
        "taker_buy_quote_volume", "ignore",
    ])

    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume", "quote_volume",
                "taker_buy_volume", "taker_buy_quote_volume"]:
        df[col] = df[col].astype(float)
    df["trades"] = df["trades"].astype(int)

    df["buy_ratio"] = df["taker_buy_volume"] / df["volume"].replace(0, float("nan"))
    df["spread_proxy"] = (df["high"] - df["low"]) / df["close"].replace(0, float("nan"))
    df["symbol"] = symbol

    return df[[
        "timestamp", "symbol", "open", "high", "low", "close",
        "volume", "quote_volume", "trades", "taker_buy_volume",
        "taker_buy_quote_volume", "buy_ratio", "spread_proxy",
    ]]


def collect_coin(
    coin_key: str,
    interval: str = "5m",
    start_date: datetime = None,
    end_date: datetime = None,
) -> dict[str, pd.DataFrame]:
    """Collect all Binance pairs for a stablecoin. Returns {symbol: DataFrame}."""
    config = STABLECOINS.get(coin_key)
    if not config:
        raise ValueError(f"Unknown coin: {coin_key}")

    pairs = config.get("binance_pairs", [])
    if not pairs:
        print(f"  No Binance pairs configured for {coin_key}")
        return {}

    # Use coin-level start date if not specified
    if start_date is None:
        start_date = datetime.fromisoformat(config["start_date"])

    results = {}
    for symbol in pairs:
        df = collect_pair(symbol, interval=interval, start_date=start_date, end_date=end_date)
        if not df.empty:
            results[symbol] = df
            print(f"    {symbol}: {len(df):,} records ({df['timestamp'].min()} → {df['timestamp'].max()})")

    return results


def save(data: dict[str, pd.DataFrame], coin_key: str) -> list[Path]:
    """Save each pair as Parquet under data/raw/binance/."""
    out_dir = RAW_DIR / "binance"
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []

    for symbol, df in data.items():
        path = out_dir / f"{coin_key}_{symbol.lower()}.parquet"
        df.to_parquet(path, index=False)
        print(f"  Saved {path} ({len(df):,} rows)")
        paths.append(path)

    return paths


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("coin", nargs="?", default="usdt", help="Coin key or 'all'")
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    args = parser.parse_args()

    coins = list(STABLECOINS.keys()) if args.coin == "all" else [args.coin]
    start = datetime.fromisoformat(args.start) if args.start else None

    for coin in coins:
        print(f"\n=== {coin.upper()} ===")
        data = collect_coin(coin, start_date=start)
        save(data, coin)
