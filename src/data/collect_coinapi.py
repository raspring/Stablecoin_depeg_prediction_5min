"""
Collect 5-minute OHLCV data from CoinAPI Indexes API.

Uses the cross-exchange VWAP index (IDX_REFRATE_VWAP_{COIN}) which provides
a volume-weighted price aggregated across all exchanges — more robust than
any single exchange feed for depeg detection.

Requires COINAPI_KEY environment variable (or .env file).
Sign up at https://www.coinapi.io/products/indexes-api

API base: https://rest-api.indexes.coinapi.io/v1
Endpoint: GET /indexes/{index_id}/timeseries
  Required params: period_id, time_start, time_end
  Max records per request: 100,000
"""

import os
import time
from datetime import datetime, timezone, timedelta
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

BASE_URL = "https://rest-api.indexes.coinapi.io/v1"
PERIOD_ID = "5MIN"
PAGE_LIMIT = 100_000  # max records per request (~347 days of 5m data)
RATE_LIMIT_DELAY = 0.5


def _get_api_key() -> str:
    key = os.getenv("COINAPI_KEY")
    if not key:
        raise EnvironmentError(
            "COINAPI_KEY not set. Add it to your .env file.\n"
            "Sign up at https://www.coinapi.io/products/indexes-api"
        )
    return key


def fetch_timeseries(
    index_id: str,
    time_start: datetime,
    time_end: datetime,
    api_key: str,
) -> list[dict]:
    """Fetch one page of timeseries from the Index API."""
    url = f"{BASE_URL}/indexes/{index_id}/timeseries"
    headers = {"X-CoinAPI-Key": api_key}
    params = {
        "period_id": PERIOD_ID,
        "time_start": time_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "time_end": time_end.strftime("%Y-%m-%dT%H:%M:%S"),
        "limit": PAGE_LIMIT,
    }
    response = requests.get(url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    time.sleep(RATE_LIMIT_DELAY)
    return response.json()


def collect_index(
    index_id: str,
    start_date: datetime,
    end_date: datetime = None,
    api_key: str = None,
) -> pd.DataFrame:
    """
    Collect full historical 5m timeseries for a CoinAPI index with pagination.

    The Index API requires time_end on every request, so we chunk in ~347-day
    windows (100k records × 5 min each).

    Returns DataFrame with columns:
        timestamp, index_id, open, high, low, close
    """
    if api_key is None:
        api_key = _get_api_key()
    if end_date is None:
        end_date = datetime.now(timezone.utc)

    all_records = []
    current_start = start_date

    print(f"  Collecting {index_id}...")

    while current_start < end_date:
        # Each page covers at most ~347 days; use full remaining range and rely on limit
        records = fetch_timeseries(index_id, current_start, end_date, api_key)

        if not records:
            break

        all_records.extend(records)

        # Advance start to just after the last returned candle
        last_ts = records[-1]["time_period_end"]
        current_start = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))

        if len(records) < PAGE_LIMIT:
            break  # No more pages

        if len(all_records) % 200_000 == 0:
            print(f"    {len(all_records):,} records...")

    if not all_records:
        print(f"    No data returned for {index_id}")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df["timestamp"] = pd.to_datetime(df["time_period_start"], utc=True)
    df = df.rename(columns={
        "value_open":  "open",
        "value_high":  "high",
        "value_low":   "low",
        "value_close": "close",
        "value_count": "tick_count",
    })
    df["index_id"] = index_id

    cols = ["timestamp", "index_id", "open", "high", "low", "close", "tick_count"]
    available = [c for c in cols if c in df.columns]
    df = df[available].sort_values("timestamp").reset_index(drop=True)

    print(f"    {index_id}: {len(df):,} records ({df['timestamp'].min()} → {df['timestamp'].max()})")
    return df


def collect_coin(
    coin_key: str,
    start_date: datetime = None,
    end_date: datetime = None,
    api_key: str = None,
) -> dict[str, pd.DataFrame]:
    """Collect all CoinAPI index symbols for a stablecoin. Returns {index_id: DataFrame}."""
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
    for index_id in symbols:
        df = collect_index(index_id, start_date=start_date, end_date=end_date, api_key=api_key)
        if not df.empty:
            results[index_id] = df

    return results


def save(data: dict[str, pd.DataFrame], coin_key: str) -> list[Path]:
    """Save each index as Parquet under data/raw/coinapi/."""
    out_dir = RAW_DIR / "coinapi"
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []

    for index_id, df in data.items():
        filename = index_id.lower()
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
