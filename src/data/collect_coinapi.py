"""
Collect 5-minute OHLCV data from CoinAPI for direct fiat pairs.

Requires COINAPI_KEY environment variable (or .env file).
Used for: USDTUSD, USDCUSD, DAIUSD, and other exchange-native fiat pairs
that are unavailable via Binance's free public API.

CoinAPI docs: https://docs.coinapi.io/market-data/rest-api/ohlcv
Symbol format: {EXCHANGE}_SPOT_{BASE}_{QUOTE}  e.g. KRAKEN_SPOT_USDT_USD
"""

import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR, STABLECOINS

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests

BASE_URL = "https://rest.coinapi.io/v1"
PERIOD_ID = "5MIN"
PAGE_LIMIT = 100_000  # CoinAPI max per request
RATE_LIMIT_DELAY = 0.5  # seconds between requests


def _get_api_key() -> str:
    key = os.getenv("COINAPI_KEY")
    if not key:
        raise EnvironmentError(
            "COINAPI_KEY not set. Add it to your .env file or environment.\n"
            "Sign up at https://www.coinapi.io/"
        )
    return key


def get_ohlcv(
    symbol_id: str,
    start: datetime,
    end: datetime,
    api_key: str,
) -> list[dict]:
    """Fetch one page of OHLCV records from CoinAPI."""
    url = f"{BASE_URL}/ohlcv/{symbol_id}/history"
    headers = {"X-CoinAPI-Key": api_key}
    params = {
        "period_id": PERIOD_ID,
        "time_start": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "time_end": end.strftime("%Y-%m-%dT%H:%M:%S"),
        "limit": PAGE_LIMIT,
    }
    response = requests.get(url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    time.sleep(RATE_LIMIT_DELAY)
    return response.json()


def collect_symbol(
    symbol_id: str,
    start_date: datetime,
    end_date: datetime = None,
    api_key: str = None,
) -> pd.DataFrame:
    """
    Collect full historical 5m OHLCV for a CoinAPI symbol with pagination.

    Returns DataFrame with columns:
        timestamp, symbol_id, open, high, low, close, volume, trades
    """
    if api_key is None:
        api_key = _get_api_key()

    if end_date is None:
        end_date = datetime.now(timezone.utc)

    all_records = []
    current_start = start_date

    print(f"  Collecting {symbol_id}...")

    while current_start < end_date:
        records = get_ohlcv(symbol_id, current_start, end_date, api_key)
        if not records:
            break

        all_records.extend(records)

        # Advance start to just after the last returned timestamp
        last_ts = records[-1]["time_period_start"]
        last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
        current_start = last_dt + pd.Timedelta(minutes=5)

        if len(records) < PAGE_LIMIT:
            break

        if len(all_records) % 100_000 == 0:
            print(f"    {len(all_records):,} records...")

    if not all_records:
        print(f"    No data returned for {symbol_id}")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)

    df["timestamp"] = pd.to_datetime(df["time_period_start"], utc=True)
    df = df.rename(columns={
        "price_open": "open",
        "price_high": "high",
        "price_low": "low",
        "price_close": "close",
        "volume_traded": "volume",
        "trades_count": "trades",
    })
    df["symbol_id"] = symbol_id

    cols = ["timestamp", "symbol_id", "open", "high", "low", "close", "volume", "trades"]
    available = [c for c in cols if c in df.columns]
    df = df[available].sort_values("timestamp").reset_index(drop=True)

    print(f"    {symbol_id}: {len(df):,} records ({df['timestamp'].min()} → {df['timestamp'].max()})")
    return df


def collect_coin(
    coin_key: str,
    start_date: datetime = None,
    end_date: datetime = None,
    api_key: str = None,
) -> dict[str, pd.DataFrame]:
    """Collect all CoinAPI symbols for a stablecoin. Returns {symbol_id: DataFrame}."""
    config = STABLECOINS.get(coin_key)
    if not config:
        raise ValueError(f"Unknown coin: {coin_key}")

    symbols = config.get("coinapi_symbols", [])
    if not symbols:
        print(f"  No CoinAPI symbols configured for {coin_key}")
        return {}

    if api_key is None:
        api_key = _get_api_key()

    if start_date is None:
        start_date = datetime.fromisoformat(config["start_date"]).replace(tzinfo=timezone.utc)

    results = {}
    for symbol_id in symbols:
        df = collect_symbol(symbol_id, start_date=start_date, end_date=end_date, api_key=api_key)
        if not df.empty:
            results[symbol_id] = df

    return results


def save(data: dict[str, pd.DataFrame], coin_key: str) -> list[Path]:
    """Save each symbol as Parquet under data/raw/coinapi/."""
    out_dir = RAW_DIR / "coinapi"
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []

    for symbol_id, df in data.items():
        # e.g. KRAKEN_SPOT_USDT_USD -> kraken_spot_usdt_usd
        filename = symbol_id.lower().replace("/", "_")
        path = out_dir / f"{coin_key}_{filename}.parquet"
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
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc) if args.start else None

    for coin in coins:
        print(f"\n=== {coin.upper()} ===")
        data = collect_coin(coin, start_date=start)
        save(data, coin)
