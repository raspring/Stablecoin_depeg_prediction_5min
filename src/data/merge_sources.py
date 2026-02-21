"""
Merge all raw sources into per-coin 5-minute Parquet files.

Strategy:
  - Base index: 5-minute UTC timestamps spanning each coin's date range
  - 5m sources (Binance, CoinAPI): joined directly on timestamp
  - Daily sources (DefiLlama, FRED, market): forward-filled into 5m index

Output: data/processed/{coin}_5m.parquet

Usage:
    python src/data/merge_sources.py usdt
    python src/data/merge_sources.py all
"""

import argparse
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR, PROCESSED_DIR, STABLECOINS


def build_5m_index(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    """Create a UTC 5-minute index from start to end."""
    return pd.date_range(start=start, end=end, freq="5min", tz="UTC")


def load_binance(coin_key: str) -> pd.DataFrame:
    """Load and aggregate all Binance pairs for a coin into a single DataFrame."""
    binance_dir = RAW_DIR / "binance"
    files = list(binance_dir.glob(f"{coin_key}_*.parquet"))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        # Prefix columns with symbol to avoid collisions
        symbol = df["symbol"].iloc[0].lower() if "symbol" in df.columns else f.stem
        df = df.rename(columns={
            col: f"{symbol}_{col}"
            for col in ["open", "high", "low", "close", "volume", "quote_volume",
                        "trades", "taker_buy_volume", "taker_buy_quote_volume",
                        "buy_ratio", "spread_proxy"]
            if col in df.columns
        })
        df = df.drop(columns=["symbol"], errors="ignore")
        dfs.append(df.set_index("timestamp"))

    return pd.concat(dfs, axis=1).sort_index()


def load_coinapi(coin_key: str) -> pd.DataFrame:
    """Load and aggregate all CoinAPI fiat pair files for a coin."""
    coinapi_dir = RAW_DIR / "coinapi"
    files = list(coinapi_dir.glob(f"{coin_key}_*.parquet"))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        symbol_id = df["symbol_id"].iloc[0].lower() if "symbol_id" in df.columns else f.stem
        # Use a short prefix from the symbol (e.g. kraken_spot_usdt_usd -> kraken)
        prefix = symbol_id.split("_")[0]
        df = df.rename(columns={
            col: f"{prefix}_{col}"
            for col in ["open", "high", "low", "close", "volume", "trades"]
            if col in df.columns
        })
        df = df.drop(columns=["symbol_id"], errors="ignore")
        dfs.append(df.set_index("timestamp"))

    return pd.concat(dfs, axis=1).sort_index()


def load_defillama(coin_key: str) -> pd.DataFrame:
    path = RAW_DIR / "defillama" / f"{coin_key}_supply.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.drop(columns=["coin"], errors="ignore")
    return df.set_index("date").sort_index()


def load_fred() -> pd.DataFrame:
    path = RAW_DIR / "fred" / "macro.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df.set_index("date").sort_index()


def load_market() -> pd.DataFrame:
    path = RAW_DIR / "market" / "market_daily.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df.set_index("date").sort_index()


def merge_coin(coin_key: str) -> pd.DataFrame:
    config = STABLECOINS[coin_key]

    print(f"  Loading 5m sources...")
    binance_df = load_binance(coin_key)
    coinapi_df = load_coinapi(coin_key)

    # Determine date range from available 5m data
    all_5m = [df for df in [binance_df, coinapi_df] if not df.empty]
    if not all_5m:
        print(f"  No 5m data found for {coin_key}. Run collectors first.")
        return pd.DataFrame()

    start = min(df.index.min() for df in all_5m)
    end = max(df.index.max() for df in all_5m)

    print(f"  Building 5m index: {start} → {end}")
    idx = build_5m_index(start, end)
    result = pd.DataFrame(index=idx)
    result.index.name = "timestamp"

    # Join 5m sources
    if not binance_df.empty:
        result = result.join(binance_df, how="left")
    if not coinapi_df.empty:
        result = result.join(coinapi_df, how="left")

    # Forward-fill daily sources
    print(f"  Loading and forward-filling daily sources...")

    for name, daily_df in [
        ("supply", load_defillama(coin_key)),
        ("macro", load_fred()),
        ("market", load_market()),
    ]:
        if daily_df.empty:
            print(f"    {name}: not found, skipping")
            continue

        # Reindex to 5m and forward-fill
        daily_reindexed = daily_df.reindex(result.index, method="ffill")
        result = result.join(daily_reindexed, how="left")
        print(f"    {name}: joined ({len(daily_df)} daily rows → forward-filled)")

    # Add coin metadata
    result["coin"] = coin_key
    result["peg"] = config["peg"]
    result["coin_type"] = config["type"]
    result["coin_status"] = config["status"]

    # Trim to coin's known active date range
    coin_start = pd.Timestamp(config["start_date"], tz="UTC")
    result = result[result.index >= coin_start]

    if config.get("end_date"):
        coin_end = pd.Timestamp(config["end_date"], tz="UTC")
        result = result[result.index <= coin_end]

    result = result.sort_index()
    print(f"  Final shape: {result.shape} ({result.index.min()} → {result.index.max()})")
    return result


def save(df: pd.DataFrame, coin_key: str) -> Path:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    path = PROCESSED_DIR / f"{coin_key}_5m.parquet"
    df.to_parquet(path)
    size_mb = path.stat().st_size / 1e6
    print(f"  Saved {path} ({len(df):,} rows, {size_mb:.1f} MB)")
    return path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("coin", nargs="?", default="all", help="Coin key or 'all'")
    args = parser.parse_args()

    coins = list(STABLECOINS.keys()) if args.coin == "all" else [args.coin]

    for coin in coins:
        print(f"\n{'='*50}")
        print(f"  Merging {coin.upper()}")
        print(f"{'='*50}")
        df = merge_coin(coin)
        if not df.empty:
            save(df, coin)


if __name__ == "__main__":
    main()
