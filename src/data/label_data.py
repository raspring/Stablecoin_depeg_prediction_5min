"""
Add depeg labels to merged 5m coin files.

Labels added:
  price_dev          — coinapi_close - 1.0 (continuous deviation from peg)
  depeg              — abs(price_dev) > DEPEG_THRESHOLD (binary, current 5m bar)
  depeg_next_5min    — any depeg in the next 1 bar  (5 min)
  depeg_next_30min   — any depeg in the next 6 bars (30 min)
  depeg_next_1h      — any depeg in the next 12 bars (1 hour)
  depeg_next_4h      — any depeg in the next 48 bars (4 hours)

Forward labels are 1 if ANY bar in the lookahead window is depegged, else 0.
The last N rows of each forward label are NaN (future not available).

Outlier handling:
  CoinAPI occasionally emits clearly erroneous prices (e.g. 192.x on 2019-09-16,
  ~0.45-0.50 on 2020-01-25). These are nulled before labeling for all coins
  except UST, which legitimately collapsed to near zero in May 2022.

Usage:
  python src/data/label_data.py [coin|all]
"""

import argparse
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import CLEANSED_DIR, STABLECOINS, DEPEG_THRESHOLD, DEPEG_CONSECUTIVE_BARS

# Coins whose price can legitimately go far outside the normal stablecoin range
_ALLOW_EXTREME_PRICES = {"ust"}

# Clip bounds for CoinAPI feed errors on normal stablecoins
_OUTLIER_LO, _OUTLIER_HI = 0.50, 2.0

# Forward-looking horizons: (label_suffix, bars_ahead)
_HORIZONS = [
    ("5min",  1),
    ("30min", 6),
    ("1h",   12),
    ("4h",   48),
]


def label_coin(coin_key: str) -> pd.DataFrame:
    path = CLEANSED_DIR / f"{coin_key}_5m.parquet"
    df = pd.read_parquet(path)

    price = df["coinapi_close"].copy()

    # Null out obvious feed errors for non-failed coins
    if coin_key not in _ALLOW_EXTREME_PRICES:
        bad = (price < _OUTLIER_LO) | (price > _OUTLIER_HI)
        n_bad = bad.sum()
        if n_bad:
            print(f"  Nulling {n_bad} outlier rows (coinapi_close outside [{_OUTLIER_LO}, {_OUTLIER_HI}])")
            price[bad] = pd.NA

    # Continuous deviation from $1.00 peg
    df["price_dev"] = (price - 1.0).astype("float64")

    # Binary current-state label: threshold must be breached for DEPEG_CONSECUTIVE_BARS in a row
    # rolling(n).min() == 1 only if all n bars are 1 — filters single-bar noise/artifacts
    single_breach = (df["price_dev"].abs() > DEPEG_THRESHOLD)
    df["depeg"] = (
        single_breach.rolling(DEPEG_CONSECUTIVE_BARS, min_periods=DEPEG_CONSECUTIVE_BARS)
        .min()
        .astype("Int8")
    )
    df.loc[price.isna(), "depeg"] = pd.NA

    # Forward-looking labels — rolling max over next N bars
    depeg_f = df["depeg"].astype(float)  # float so rolling/shift works cleanly
    for suffix, bars in _HORIZONS:
        col = f"depeg_next_{suffix}"
        # rolling(bars).max() at T gives max(T-bars+1 .. T)
        # .shift(-bars) moves it to T-(bars) → so at T we see max(T+1 .. T+bars)
        fwd = depeg_f.rolling(bars, min_periods=bars).max().shift(-bars)
        df[col] = fwd.astype("Int8")

    # Summary
    current_rate = df["depeg"].mean()
    print(f"  depeg rate (current): {current_rate:.2%}  |  "
          f"rows: {len(df):,}  |  "
          f"price_dev range: [{df['price_dev'].min():.4f}, {df['price_dev'].max():.4f}]")

    return df


def save(df: pd.DataFrame, coin_key: str) -> None:
    path = CLEANSED_DIR / f"{coin_key}_5m.parquet"
    df.to_parquet(path)
    print(f"  Saved {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("coin", nargs="?", default="all")
    args = parser.parse_args()

    coins = list(STABLECOINS.keys()) if args.coin == "all" else [args.coin]

    for coin in coins:
        print(f"\n{'='*50}")
        print(f"  Labeling {coin.upper()}")
        print(f"{'='*50}")
        df = label_coin(coin)
        save(df, coin)


if __name__ == "__main__":
    main()
