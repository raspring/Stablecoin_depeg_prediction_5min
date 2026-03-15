"""
build_pooled_dataset.py — Stack all 7 coins into one modeling-ready Parquet.

Selects the 76 common feature columns, adds coin_key, drops rows where
depeg_next_1h is NaN (tail of each coin's history, 13 rows per coin).

Output: data/processed/features/pooled_5m.parquet

Usage:
    python src/features/build_pooled_dataset.py
"""

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import FEATURES_DIR, STABLECOINS

# Columns to keep from each coin's feature file (order is preserved)
META_COLS = ["date", "time", "coin", "peg", "coin_type", "coin_status"]
LABEL_COLS = ["depeg", "depeg_next_1h"]

FEATURE_COLS = [
    # Raw price
    "coinapi_open", "coinapi_high", "coinapi_low", "coinapi_close", "coinapi_tick_count",
    # Market context (raw)
    "binance_btc_close", "binance_eth_close",
    "dxy", "vix", "t10y", "fedfunds", "fear_greed",
    # Price deviation (raw + engineered)
    "price_dev",
    "price_dev_mean_15min", "price_dev_std_15min", "price_dev_absmax_15min",
    "price_dev_mean_1h",    "price_dev_std_1h",    "price_dev_absmax_1h",
    "price_dev_mean_4h",    "price_dev_std_4h",    "price_dev_absmax_4h",
    "price_dev_diff1",
    "bars_above_01pct_15min", "bars_above_03pct_15min",
    "bars_above_01pct_1h",   "bars_above_03pct_1h",
    "bars_above_01pct_4h",   "bars_above_03pct_4h",
    "intrabar_range",
    # On-chain flows
    "net_flow_sum_1h", "net_flow_sum_4h", "net_flow_sum_1d", "net_flow_zscore_30d",
    "mint_sum_1h", "burn_sum_1h", "mint_sum_4h", "burn_sum_4h",
    "mint_burn_ratio_1h",
    # Curve DEX pressure
    "curve_net_sell_sum_15min", "curve_net_sell_sum_1h", "curve_net_sell_sum_4h",
    "curve_net_sell_zscore_30d", "curve_sell_buy_ratio_1h",
    # Market returns & volatility
    "btc_return_1h", "btc_return_4h", "eth_return_1h", "eth_return_4h",
    "btc_vol_4h", "btc_vol_1d",
    "vix_diff_1d", "fear_greed_diff_1d",
    # Temporal
    "hour_of_day", "day_of_week", "is_weekend", "is_us_market_hours",
    # Lags
    "lag1_price_dev", "lag1_net_flow_usd", "lag1_curve_net_sell",
    "lag3_price_dev", "lag3_net_flow_usd", "lag3_curve_net_sell",
    "lag6_price_dev", "lag6_net_flow_usd", "lag6_curve_net_sell",
    "lag12_price_dev", "lag12_net_flow_usd", "lag12_curve_net_sell",
]

ALL_COLS = META_COLS + LABEL_COLS + FEATURE_COLS


def main() -> None:
    pieces = []

    for coin_key in STABLECOINS:
        path = FEATURES_DIR / f"{coin_key}_5m_features.parquet"
        if not path.exists():
            print(f"[warn] {path} not found — skipping")
            continue

        df = pd.read_parquet(path, columns=ALL_COLS)

        # Add coin_key identifier for model
        df.insert(0, "coin_key", coin_key)

        # Drop the 13 tail rows per coin where label is undefined
        before = len(df)
        df = df.dropna(subset=["depeg_next_1h"])
        dropped = before - len(df)

        depeg_rows = int((df["depeg_next_1h"] == 1).sum())
        print(f"[{coin_key}] {len(df):>8,} rows | dropped {dropped} NaN-label rows "
              f"| depeg_next_1h=1: {depeg_rows:,} ({100*depeg_rows/len(df):.2f}%)")

        pieces.append(df)

    pooled = pd.concat(pieces, axis=0)
    pooled.sort_index(inplace=True)

    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    out = FEATURES_DIR / "pooled_5m.parquet"
    pooled.to_parquet(out)

    total_depeg = int((pooled["depeg_next_1h"] == 1).sum())
    print(f"\nPooled: {len(pooled):,} rows × {len(pooled.columns)} cols")
    print(f"depeg_next_1h=1: {total_depeg:,} ({100*total_depeg/len(pooled):.2f}%)")
    print(f"coin_key distribution:\n{pooled['coin_key'].value_counts().to_string()}")
    print(f"\nSaved → {out}")


if __name__ == "__main__":
    main()
