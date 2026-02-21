"""
Collect historical order book snapshots from CoinAPI Market Data API.

Requires COINAPI_MARKETDATA_KEY environment variable (separate from the
Indexes API key — Market Data is a different CoinAPI product).

From each snapshot we compute:
  - obi_1pct   : Order Book Imbalance within 1% of mid price
  - obi_2pct   : Order Book Imbalance within 2% of mid price
  - bid_slope  : Rate at which bid-side liquidity drops off from mid
  - ask_slope  : Rate at which ask-side liquidity drops off from mid
  - spread_bps : Best bid/ask spread in basis points

These are then aggregated to 5-minute windows (mean + std) to align with
the rest of the pipeline.

API docs: https://rest.coinapi.io/v1/orderbooks/:symbol_id/history

NOTE: Run explore_orderbook_symbols.py after getting the key to verify
symbol availability and snapshot frequency before running a full collection.
"""

import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

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
PAGE_LIMIT = 100_000
RATE_LIMIT_DELAY = 0.5

# Depth thresholds for OBI calculation (% from mid price)
OBI_LEVELS = [0.01, 0.02]


def _get_api_key() -> str:
    key = os.getenv("COINAPI_MARKETDATA_KEY")
    if not key:
        raise EnvironmentError(
            "COINAPI_MARKETDATA_KEY not set. Add it to your .env file.\n"
            "This requires a CoinAPI Market Data API key (separate from the Indexes key)."
        )
    return key


def fetch_orderbook_history(
    symbol_id: str,
    time_start: datetime,
    time_end: datetime,
    api_key: str,
    limit: int = PAGE_LIMIT,
) -> list[dict]:
    """Fetch one page of order book snapshots."""
    url = f"{BASE_URL}/orderbooks/{symbol_id}/history"
    headers = {"X-CoinAPI-Key": api_key}
    params = {
        "time_start": time_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "time_end": time_end.strftime("%Y-%m-%dT%H:%M:%S"),
        "limit": limit,
    }
    response = requests.get(url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    time.sleep(RATE_LIMIT_DELAY)
    return response.json()


def compute_obi(bids: list, asks: list, mid: float, pct: float) -> float:
    """
    Order Book Imbalance within `pct` of mid price.

    OBI = (bid_vol - ask_vol) / (bid_vol + ask_vol)
    Range: -1 (all asks) to +1 (all bids). Negative = selling pressure.
    """
    threshold = mid * pct
    bid_vol = sum(b["size"] for b in bids if abs(b["price"] - mid) <= threshold)
    ask_vol = sum(a["size"] for a in asks if abs(a["price"] - mid) <= threshold)
    total = bid_vol + ask_vol
    return (bid_vol - ask_vol) / total if total > 0 else float("nan")


def compute_slope(levels: list, mid: float, side: str) -> float:
    """
    Order Book Slope: regression of cumulative volume vs price distance from mid.

    A steeper (more negative) slope = liquidity drops off quickly = thin book.
    Returns the slope coefficient (vol per unit price distance from mid).
    """
    if len(levels) < 3:
        return float("nan")

    distances = [abs(lvl["price"] - mid) for lvl in levels]
    cum_vols = list(np.cumsum([lvl["size"] for lvl in levels]))

    if max(distances) == 0:
        return float("nan")

    # Normalize distances
    norm_dist = [d / mid for d in distances]

    # Simple linear regression: cum_vol ~ distance
    x = np.array(norm_dist)
    y = np.array(cum_vols)
    if len(x) < 2 or np.std(x) == 0:
        return float("nan")

    slope = np.polyfit(x, y, 1)[0]
    return float(slope)


def process_snapshot(snapshot: dict) -> dict:
    """Extract OBI, slope, and spread from a single order book snapshot."""
    bids = snapshot.get("bids", [])
    asks = snapshot.get("asks", [])

    if not bids or not asks:
        return {}

    best_bid = bids[0]["price"]
    best_ask = asks[0]["price"]
    mid = (best_bid + best_ask) / 2

    spread_bps = ((best_ask - best_bid) / mid) * 10_000 if mid > 0 else float("nan")

    result = {
        "time_exchange": snapshot.get("time_exchange"),
        "spread_bps": spread_bps,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_slope": compute_slope(bids, mid, "bid"),
        "ask_slope": compute_slope(asks, mid, "ask"),
    }

    for pct in OBI_LEVELS:
        label = f"obi_{int(pct * 100)}pct"
        result[label] = compute_obi(bids, asks, mid, pct)

    return result


def collect_symbol(
    symbol_id: str,
    start_date: datetime,
    end_date: datetime = None,
    api_key: str = None,
) -> pd.DataFrame:
    """
    Collect and process order book snapshots for a symbol.

    Returns DataFrame of per-snapshot metrics, NOT yet aggregated to 5m.
    Call aggregate_to_5m() to align with the rest of the pipeline.
    """
    if api_key is None:
        api_key = _get_api_key()
    if end_date is None:
        end_date = datetime.now(timezone.utc)

    all_rows = []
    current_start = start_date

    print(f"  Collecting order book: {symbol_id}...")

    while current_start < end_date:
        snapshots = fetch_orderbook_history(symbol_id, current_start, end_date, api_key)
        if not snapshots:
            break

        for snap in snapshots:
            row = process_snapshot(snap)
            if row:
                all_rows.append(row)

        last_ts = snapshots[-1].get("time_exchange") or snapshots[-1].get("time_coinapi")
        current_start = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
        current_start = current_start.replace(microsecond=0) + pd.Timedelta(seconds=1)

        if len(snapshots) < PAGE_LIMIT:
            break

        if len(all_rows) % 100_000 == 0:
            print(f"    {len(all_rows):,} snapshots processed...")

    if not all_rows:
        print(f"    No data for {symbol_id}")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["timestamp"] = pd.to_datetime(df["time_exchange"], utc=True)
    df = df.drop(columns=["time_exchange"]).sort_values("timestamp").reset_index(drop=True)
    df["symbol_id"] = symbol_id

    print(f"    {symbol_id}: {len(df):,} snapshots ({df['timestamp'].min()} → {df['timestamp'].max()})")
    return df


def aggregate_to_5m(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate per-snapshot order book metrics to 5-minute windows.

    For each 5m candle: mean and std of each metric across all snapshots
    that fall within that window.
    """
    if df.empty:
        return df

    df = df.set_index("timestamp").drop(columns=["symbol_id"], errors="ignore")
    metric_cols = [c for c in df.columns if c not in ["best_bid", "best_ask"]]

    agg = df[metric_cols].resample("5min").agg(["mean", "std"])
    agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
    agg = agg.reset_index()
    return agg


def collect_coin(
    coin_key: str,
    start_date: datetime = None,
    end_date: datetime = None,
    api_key: str = None,
) -> pd.DataFrame:
    """Collect and aggregate order book data for a stablecoin to 5m resolution."""
    config = STABLECOINS.get(coin_key)
    if not config:
        raise ValueError(f"Unknown coin: {coin_key}")

    symbol_id = config.get("orderbook_symbol")
    if not symbol_id:
        print(f"  No order book symbol configured for {coin_key}")
        return pd.DataFrame()

    if api_key is None:
        api_key = _get_api_key()

    if start_date is None:
        start_date = datetime.fromisoformat(config["start_date"]).replace(tzinfo=timezone.utc)

    raw = collect_symbol(symbol_id, start_date=start_date, end_date=end_date, api_key=api_key)
    if raw.empty:
        return pd.DataFrame()

    df = aggregate_to_5m(raw)
    df["coin"] = coin_key
    df["symbol_id"] = symbol_id
    return df


def save(df: pd.DataFrame, coin_key: str) -> Path:
    out_dir = RAW_DIR / "orderbook"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{coin_key}_orderbook_5m.parquet"
    df.to_parquet(path, index=False)
    print(f"  Saved {path} ({len(df):,} rows)")
    return path


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
        df = collect_coin(coin, start_date=start)
        if not df.empty:
            save(df, coin)
