"""
Merge all raw sources into per-coin 5-minute Parquet files.

This is a pure join — no cleaning, filling, or derived columns.
Run clean_data.py afterwards to zero-fill event columns, forward-fill
daily series, and compute derived features.

Strategy:
  - Base index: 5-minute UTC timestamps spanning each coin's date range
  - 5m sources (Binance, CoinAPI, on-chain, Curve): joined directly on timestamp
  - Daily sources (FRED, market): reindexed to 5m (raw NaN where no prior value)

Output: data/processed/{coin}_5m_raw.parquet

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

# Global data cutoff — trim all coins to this date regardless of source coverage
GLOBAL_END_DATE = pd.Timestamp("2026-02-28 23:55:00", tz="UTC")


def build_5m_index(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    """Create a UTC 5-minute index from start to end."""
    return pd.date_range(start=start, end=end, freq="5min", tz="UTC")


def load_binance(coin_key: str) -> pd.DataFrame:
    """Load and aggregate all Binance pairs for a coin into a single DataFrame.
    BTC/ETH market context pairs are excluded here — handled by load_btc_eth_5m instead."""
    binance_dir = RAW_DIR / "binance"
    files = [f for f in binance_dir.glob(f"{coin_key}_*.parquet")
             if not any(m in f.stem.upper() for m in ["BTCUSDT", "ETHUSDT"])]
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        # Prefix columns with symbol to avoid collisions
        symbol = df["symbol"].iloc[0].lower() if "symbol" in df.columns else f.stem
        df = df.rename(columns={
            col: f"binance_{symbol}_{col}"
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
        df = df.rename(columns={
            col: f"coinapi_{col}"
            for col in ["open", "high", "low", "close", "volume", "trades", "tick_count"]
            if col in df.columns
        })
        df = df.drop(columns=["symbol_id", "index_id"], errors="ignore")
        dfs.append(df.set_index("timestamp"))

    return pd.concat(dfs, axis=1).sort_index()


def load_fred() -> pd.DataFrame:
    path = RAW_DIR / "fred" / "macro.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df.set_index("date").sort_index()


def load_orderbook(coin_key: str) -> pd.DataFrame:
    path = RAW_DIR / "orderbook" / f"{coin_key}_orderbook_5m.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.drop(columns=["coin", "symbol_id"], errors="ignore").set_index("timestamp").sort_index()


def load_market() -> pd.DataFrame:
    path = RAW_DIR / "market" / "market_daily.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df.set_index("date").sort_index()


def load_onchain(coin_key: str) -> pd.DataFrame:
    """Load 5-min on-chain ETH events (mint/burn + USDT treasury flows)."""
    path = RAW_DIR / "onchain" / f"{coin_key}_eth_5m.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.set_index("timestamp").sort_index()


def load_tron(coin_key: str) -> pd.DataFrame:
    """Load 5-min USDT TRON treasury flows (USDT only)."""
    if coin_key != "usdt":
        return pd.DataFrame()
    path = RAW_DIR / "onchain" / "usdt_tron_5m.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    # Prefix to distinguish from ETH treasury columns
    df = df.rename(columns={c: f"tron_{c}" for c in df.columns if c != "timestamp"})
    return df.set_index("timestamp").sort_index()


# Curve pool → coins it covers (for routing per-coin joins)
_CURVE_POOL_COINS = {
    "3pool":      ["usdt", "usdc", "dai"],
    "usde_usdc":  ["usde"],
    "rlusd_usdc": ["rlusd"],
}


def load_curve(coin_key: str) -> pd.DataFrame:
    """Load 5-min Curve TokenExchange data for pools relevant to this coin."""
    frames = []
    for pool, coins in _CURVE_POOL_COINS.items():
        if coin_key not in coins:
            continue
        path = RAW_DIR / "curve" / f"{pool}_5m.parquet"
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        # Prefix all columns with pool name for clarity
        df = df.rename(columns={c: f"curve_{pool}_{c}" for c in df.columns if c != "timestamp"})
        frames.append(df.set_index("timestamp").sort_index())
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=1).sort_index()


def load_btc_eth_5m() -> pd.DataFrame:
    """Load 5m BTC and ETH closes from Binance as shared market context for all coins."""
    binance_dir = RAW_DIR / "binance"
    frames = []
    for symbol_file, col_name in [("usdt_btcusdt.parquet", "btc_close"),
                                   ("usdt_ethusdt.parquet", "eth_close")]:
        path = binance_dir / symbol_file
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.rename(columns={"close": f"binance_{col_name}"})[["timestamp", f"binance_{col_name}"]]
        frames.append(df.set_index("timestamp"))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=1).sort_index()


def merge_coin(coin_key: str) -> pd.DataFrame:
    config = STABLECOINS[coin_key]

    print(f"  Loading 5m sources...")
    binance_df  = load_binance(coin_key)
    coinapi_df  = load_coinapi(coin_key)
    onchain_df  = load_onchain(coin_key)
    tron_df     = load_tron(coin_key)
    curve_df    = load_curve(coin_key)

    # Determine date range from available 5m data
    all_5m = [df for df in [binance_df, coinapi_df, onchain_df, tron_df, curve_df] if not df.empty]
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

    # 5m BTC/ETH closes as shared market context for all coins
    btc_eth_df = load_btc_eth_5m()
    if not btc_eth_df.empty:
        result = result.join(btc_eth_df, how="left")
        print(f"    btc/eth 5m: joined ({len(btc_eth_df):,} rows)")

    ob_df = load_orderbook(coin_key)
    if not ob_df.empty:
        result = result.join(ob_df, how="left")
        print(f"    orderbook: joined ({len(ob_df):,} 5m rows)")

    for name, df in [
        ("onchain",  onchain_df),
        ("tron",     tron_df),
        ("curve",    curve_df),
    ]:
        if not df.empty:
            result = result.join(df, how="left")
            print(f"    {name}: joined ({len(df):,} 5m rows)")

    # Forward-fill daily sources
    print(f"  Loading and forward-filling daily sources...")

    for name, daily_df in [
        ("macro", load_fred()),
        ("market", load_market()),
    ]:
        if daily_df.empty:
            print(f"    {name}: not found, skipping")
            continue

        # ffill within daily data first (monthly series like fedfunds have NaN on
        # non-release days; reindex exact-matches those NaN rows rather than looking back)
        daily_df = daily_df.ffill()
        # Reindex to 5m and forward-fill
        daily_reindexed = daily_df.reindex(result.index, method="ffill")
        result = result.join(daily_reindexed, how="left")
        print(f"    {name}: joined ({len(daily_df)} daily rows → forward-filled)")

    # Date and time columns derived from the timestamp index
    result["date"] = result.index.date
    result["time"] = result.index.time

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

    # Apply global cutoff
    result = result[result.index <= GLOBAL_END_DATE]

    result = result.sort_index()
    print(f"  Final shape: {result.shape} ({result.index.min()} → {result.index.max()})")
    return result


def save(df: pd.DataFrame, coin_key: str) -> Path:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    path = PROCESSED_DIR / f"{coin_key}_5m_raw.parquet"
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
