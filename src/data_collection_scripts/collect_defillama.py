"""
Collect daily stablecoin circulating supply (≈ market cap) from DeFiLlama.

Resolution: daily. Forward-filled to 5m in feature engineering.

Output: data/raw/defillama/mcap_daily.parquet
  Columns: date, usdt_mcap, usdc_mcap, dai_mcap, busd_mcap, ust_mcap, usde_mcap, rlusd_mcap

DeFiLlama stablecoin IDs (confirmed):
  1   = USDT (Tether)
  2   = USDC (USD Coin)
  5   = DAI
  4   = BUSD (Binance USD)
  3   = USTC / TerraClassicUSD (UST pre-depeg)
  146 = USDe (Ethena)
  250 = RLUSD (Ripple USD)
"""

from pathlib import Path
import sys
import time

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR, GLOBAL_START_DATE

BASE_URL = "https://stablecoins.llama.fi/stablecoin/{id}"

COIN_IDS = {
    "usdt":  1,
    "usdc":  2,
    "dai":   5,
    "busd":  4,
    "ust":   3,
    "usde":  146,
    "rlusd": 250,
}


def fetch_mcap(coin: str, llama_id: int) -> pd.Series:
    """Fetch daily circulating supply for one coin. Returns a Series indexed by date."""
    url = BASE_URL.format(id=llama_id)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    tokens = resp.json().get("tokens", [])
    if not tokens:
        print(f"  WARNING: no data returned for {coin} (id={llama_id})")
        return pd.Series(dtype=float, name=f"{coin}_mcap")

    dates  = pd.to_datetime([t["date"] for t in tokens], unit="s", utc=True).normalize()
    values = [t["circulating"].get("peggedUSD", 0) for t in tokens]

    s = pd.Series(values, index=dates, name=f"{coin}_mcap", dtype=float)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s


def collect_all() -> pd.DataFrame:
    start = pd.Timestamp(GLOBAL_START_DATE, tz="UTC")
    series_list = []

    for coin, llama_id in COIN_IDS.items():
        print(f"  Fetching {coin.upper()} (DeFiLlama id={llama_id})...")
        s = fetch_mcap(coin, llama_id)
        s = s[s.index >= start]
        if len(s):
            print(f"    {len(s)} days  |  "
                  f"{s.index.min().date()} → {s.index.max().date()}  |  "
                  f"latest: ${s.iloc[-1]/1e9:.2f}B")
        series_list.append(s)
        time.sleep(0.3)   # be polite to the API

    df = pd.concat(series_list, axis=1)
    df.index.name = "date"
    df = df.reset_index()
    return df


def save(df: pd.DataFrame) -> Path:
    out_dir = RAW_DIR / "defillama"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "mcap_daily.parquet"
    df.to_parquet(path, index=False)
    print(f"  Saved {path}  ({len(df)} rows × {len(df.columns)} cols)")
    return path


if __name__ == "__main__":
    df = collect_all()
    save(df)
    print("\nSample (last 5 rows):")
    print(df.tail())
