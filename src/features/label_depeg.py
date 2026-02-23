"""
Apply depeg labels to 5-minute VWAP data.

Primary label definition (from literature survey):
    depeg = 1  if  |VWAP_close - 1.00| > 0.005
                   for >= 3 CONSECUTIVE 5-minute candles  (15 min sustained)

This matches Bertsch (2023, BIS) and Anadu et al. (2023, NY Fed) for
fiat-backed stablecoins at sub-daily resolution, using cross-exchange VWAP
as the price source (Caramichael & Liao 2022, Fed Board).

Additional columns produced:
    deviation       : close - 1.00  (signed, so negative = below peg)
    abs_deviation   : |close - 1.00|
    exceeds_05pct   : bool, abs_deviation > 0.005 (raw single-candle signal)
    depeg           : 1 if sustained >= 3 consecutive candles, else 0
    depeg_moderate  : 1 if |dev| > 0.01 sustained >= 3 candles
    depeg_severe    : 1 if |dev| > 0.05 (any duration)

Per-coin peg overrides can be added to COIN_PEGS if a coin targets != $1.00.

Usage:
    python src/features/label_depeg.py            # all coins
    python src/features/label_depeg.py usdt       # single coin
    python src/features/label_depeg.py --summary  # print label stats only
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR, PROCESSED_DIR, STABLECOINS

# Threshold constants (basis of labeling scheme)
THRESHOLD_PRIMARY  = 0.005   # ±0.5%  — primary depeg label
THRESHOLD_MODERATE = 0.010   # ±1.0%  — moderate depeg label
THRESHOLD_SEVERE   = 0.050   # ±5.0%  — severe depeg label
CONSECUTIVE_MIN    = 3       # minimum consecutive candles to confirm a depeg

# Per-coin peg target (all current coins target $1.00)
COIN_PEGS = {coin: 1.0 for coin in STABLECOINS}


def label_coin(coin: str) -> pd.DataFrame:
    """
    Load the raw VWAP parquet for a coin, compute deviation columns,
    and apply the consecutive-candle depeg labels.

    Returns the labeled DataFrame.
    """
    # Locate raw file
    symbol = STABLECOINS[coin]["coinapi_symbols"][0].lower()
    raw_path = RAW_DIR / "coinapi" / f"{coin}_{symbol}.parquet"
    if not raw_path.exists():
        print(f"  [{coin}] raw file not found: {raw_path}")
        return pd.DataFrame()

    df = pd.read_parquet(raw_path)

    # Sort by time — essential for rolling window correctness
    df = df.sort_values("timestamp").reset_index(drop=True)

    peg = COIN_PEGS[coin]

    # ── Deviation columns ────────────────────────────────────────────────────
    df["deviation"]     = df["close"] - peg
    df["abs_deviation"] = df["deviation"].abs()

    # ── Single-candle exceedance flags ───────────────────────────────────────
    df["exceeds_05pct"] = df["abs_deviation"] > THRESHOLD_PRIMARY
    df["exceeds_1pct"]  = df["abs_deviation"] > THRESHOLD_MODERATE
    df["exceeds_5pct"]  = df["abs_deviation"] > THRESHOLD_SEVERE

    # ── Consecutive-candle labels ─────────────────────────────────────────────
    # rolling(n).min() == 1  →  all n candles in the window were True
    # This means candle t is labeled 1 once candles [t-2, t-1, t] all deviate.
    # The first (CONSECUTIVE_MIN - 1) candles of each episode get 0 (conservative).
    df["depeg"] = (
        df["exceeds_05pct"]
        .rolling(CONSECUTIVE_MIN, min_periods=CONSECUTIVE_MIN)
        .min()
        .fillna(0)
        .astype(int)
    )

    df["depeg_moderate"] = (
        df["exceeds_1pct"]
        .rolling(CONSECUTIVE_MIN, min_periods=CONSECUTIVE_MIN)
        .min()
        .fillna(0)
        .astype(int)
    )

    # Severe: single-candle is sufficient (no duration filter)
    df["depeg_severe"] = df["exceeds_5pct"].astype(int)

    # Drop intermediate boolean helpers — keep output clean
    df = df.drop(columns=["exceeds_05pct", "exceeds_1pct", "exceeds_5pct"])

    df["coin"] = coin
    return df


def save(df: pd.DataFrame, coin: str) -> Path:
    out_dir = PROCESSED_DIR / "labeled"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{coin}_vwap_labeled.parquet"
    df.to_parquet(path, index=False)
    return path


def print_summary(coin: str, df: pd.DataFrame):
    n = len(df)
    if n == 0:
        return

    date_range = f"{df['timestamp'].min().date()} → {df['timestamp'].max().date()}"

    for col, label in [
        ("depeg",          f"Primary  (±0.5%, ≥{CONSECUTIVE_MIN} candles)"),
        ("depeg_moderate", f"Moderate (±1.0%, ≥{CONSECUTIVE_MIN} candles)"),
        ("depeg_severe",    "Severe   (±5.0%, any duration       )"),
    ]:
        count = df[col].sum()
        pct   = 100 * count / n
        print(f"    {label} : {count:>7,}  /  {n:,}  ({pct:.2f} %)")

    # Contiguous depeg episodes
    episodes = (df["depeg"].diff().fillna(df["depeg"]) == 1).sum()
    print(f"    Depeg episodes (primary)         : {episodes:>7,}")
    print(f"    Date range                       : {date_range}")


def run(coins: list[str], summary_only: bool = False):
    for coin in coins:
        cfg = STABLECOINS.get(coin)
        if not cfg or not cfg.get("coinapi_symbols"):
            print(f"\n[{coin}] no CoinAPI symbols configured, skipping")
            continue

        print(f"\n=== {coin.upper()} ===")
        df = label_coin(coin)
        if df.empty:
            continue

        if not summary_only:
            path = save(df, coin)
            print(f"  Saved → {path}  ({len(df):,} rows)")

        print_summary(coin, df)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("coin", nargs="?", default="all",
                        help="Coin key or 'all' (default: all)")
    parser.add_argument("--summary", action="store_true",
                        help="Print label stats without writing files")
    args = parser.parse_args()

    coins = list(STABLECOINS.keys()) if args.coin == "all" else [args.coin]
    run(coins, summary_only=args.summary)
