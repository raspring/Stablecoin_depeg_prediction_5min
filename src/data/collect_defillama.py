"""
Collect daily circulating supply data from DefiLlama.

Resolution: daily. Will be forward-filled to 5m in merge_sources.py.
"""

import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR, STABLECOINS

BASE_URL = "https://stablecoins.llama.fi"
RATE_LIMIT_DELAY = 1.0


def get_stablecoin_history(defillama_id: int) -> list[dict]:
    url = f"{BASE_URL}/stablecoincharts/all?stablecoin={defillama_id}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    time.sleep(RATE_LIMIT_DELAY)
    return response.json()


def collect_coin(coin_key: str) -> pd.DataFrame:
    """
    Collect daily supply data for a stablecoin.

    Returns DataFrame with columns:
        date, circulating_usd, circulating_usd_change
    """
    config = STABLECOINS.get(coin_key)
    if not config:
        raise ValueError(f"Unknown coin: {coin_key}")

    defillama_id = config.get("defillama_id")
    if defillama_id is None:
        print(f"  No DefiLlama ID for {coin_key}, skipping.")
        return pd.DataFrame()

    print(f"  Collecting DefiLlama supply for {coin_key}...")
    records = get_stablecoin_history(defillama_id)

    rows = []
    for r in records:
        rows.append({
            "date": pd.to_datetime(r["date"], unit="s", utc=True).normalize(),
            "circulating_usd": r.get("totalCirculatingUSD", {}).get("peggedUSD", float("nan")),
        })

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    df["circulating_usd_change"] = df["circulating_usd"].pct_change()
    df["coin"] = coin_key

    print(f"    {len(df)} daily records ({df['date'].min().date()} → {df['date'].max().date()})")
    return df


def save(df: pd.DataFrame, coin_key: str) -> Path:
    out_dir = RAW_DIR / "defillama"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{coin_key}_supply.parquet"
    df.to_parquet(path, index=False)
    print(f"  Saved {path}")
    return path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("coin", nargs="?", default="usdt", help="Coin key or 'all'")
    args = parser.parse_args()

    coins = list(STABLECOINS.keys()) if args.coin == "all" else [args.coin]

    for coin in coins:
        print(f"\n=== {coin.upper()} ===")
        df = collect_coin(coin)
        if not df.empty:
            save(df, coin)
