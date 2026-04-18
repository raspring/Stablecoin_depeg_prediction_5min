"""
Collect daily Fear & Greed Index via alternative.me (free, no API key).

Resolution: daily. Forward-filled to 5m in merge_sources.py.

BTC/ETH prices are collected at 5m resolution by collect_binance.py
(binance_btc_close, binance_eth_close) — no daily CoinGecko prices needed.

Output: data/raw/market/market_daily.parquet
  Columns: date, fear_greed
"""

from pathlib import Path

import pandas as pd
import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR, GLOBAL_START_DATE

FEAR_GREED_URL = "https://api.alternative.me/fng/"


def get_fear_greed(limit: int = 4000) -> pd.DataFrame:
    response = requests.get(FEAR_GREED_URL, params={"limit": limit, "format": "json"}, timeout=30)
    response.raise_for_status()

    data = response.json().get("data", [])
    df = pd.DataFrame({
        "date":       pd.to_datetime([int(r["timestamp"]) for r in data], unit="s", utc=True).normalize(),
        "fear_greed": [int(r["value"]) for r in data],
    })
    return df.sort_values("date").reset_index(drop=True)


def collect_all() -> pd.DataFrame:
    print("  Collecting Fear & Greed index...")
    fg = get_fear_greed()
    start = pd.Timestamp(GLOBAL_START_DATE, tz="UTC")
    fg = fg[fg["date"] >= start].reset_index(drop=True)
    print(f"  Fear & Greed: {len(fg)} daily records "
          f"({fg['date'].min().date()} → {fg['date'].max().date()})")
    return fg


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
