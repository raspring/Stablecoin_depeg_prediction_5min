"""
Collect daily market sentiment data.

- BTC and ETH daily prices via CoinGecko (free)
- Fear & Greed Index via alternative.me (free)

Resolution: daily. Forward-filled to 5m in merge_sources.py.
"""

import time
from pathlib import Path

import pandas as pd
import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR, GLOBAL_START_DATE

COINGECKO_URL = "https://api.coingecko.com/api/v3"
FEAR_GREED_URL = "https://api.alternative.me/fng/"
RATE_LIMIT_DELAY = 1.5


def get_coingecko_prices(coin_id: str, days: int = 4000) -> pd.DataFrame:
    url = f"{COINGECKO_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": str(days), "interval": "daily"}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    time.sleep(RATE_LIMIT_DELAY)

    data = response.json()
    df = pd.DataFrame({
        "date": pd.to_datetime([x[0] for x in data["prices"]], unit="ms", utc=True).normalize(),
        f"{coin_id}_price": [x[1] for x in data["prices"]],
        f"{coin_id}_volume": [x[1] for x in data["total_volumes"]],
    })
    return df.drop_duplicates(subset=["date"]).reset_index(drop=True)


def get_fear_greed(limit: int = 4000) -> pd.DataFrame:
    response = requests.get(FEAR_GREED_URL, params={"limit": limit, "format": "json"}, timeout=30)
    response.raise_for_status()

    data = response.json().get("data", [])
    df = pd.DataFrame({
        "date": pd.to_datetime([int(r["timestamp"]) for r in data], unit="s", utc=True).normalize(),
        "fear_greed": [int(r["value"]) for r in data],
    })
    return df.sort_values("date").reset_index(drop=True)


def collect_all() -> pd.DataFrame:
    """
    Returns daily DataFrame with columns:
        date, btc_price, btc_volume, eth_price, eth_volume, fear_greed
    """
    print("  Collecting BTC prices...")
    btc = get_coingecko_prices("bitcoin")

    print("  Collecting ETH prices...")
    eth = get_coingecko_prices("ethereum")

    print("  Collecting Fear & Greed index...")
    fg = get_fear_greed()

    merged = btc.merge(eth, on="date", how="outer").merge(fg, on="date", how="left")
    merged = merged.sort_values("date").reset_index(drop=True)

    # Filter to project start date
    start = pd.Timestamp(GLOBAL_START_DATE, tz="UTC")
    merged = merged[merged["date"] >= start].reset_index(drop=True)

    print(f"  Market: {len(merged)} daily records ({merged['date'].min().date()} → {merged['date'].max().date()})")
    return merged


def save(df: pd.DataFrame) -> Path:
    out_dir = RAW_DIR / "market"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "market_daily.parquet"
    df.to_parquet(path, index=False)
    print(f"  Saved {path}")
    return path


if __name__ == "__main__":
    df = collect_all()
    save(df)
