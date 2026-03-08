"""
Clean merged 5m raw data for modeling.

Reads:  data/processed/merged/{coin}_5m_raw.parquet
Writes: data/processed/cleansed/{coin}_5m.parquet

Cleaning steps:
  1. Zero-fill event-based columns — on-chain mint/burn, treasury flows, Curve swaps.
     Absence of an event means zero activity, not missing data.
  2. Forward-fill daily series — FRED macro (DXY, VIX, T10Y, fedfunds) and Fear & Greed.
     These are released at daily or lower frequency; ffill propagates last known value.
  3. Forward-fill BTC/ETH closes — 5m but may have occasional gaps; ffill is safe.
  4. Null and forward-fill CoinAPI price anomalies — bars with prices outside [0.50, 2.00]
     are CoinAPI feed errors (confirmed against Binance). UST is exempt since its collapse
     legitimately took the price below $0.50 in May 2022.
  5. Trim head rows before first valid CoinAPI price — unfillable nulls at the very start.
  6. Compute total_net_flow_usd — unified institutional flow signal across coins.

Usage:
    python src/data/clean_data.py usdt
    python src/data/clean_data.py all
"""

import argparse
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import MERGED_DIR, CLEANSED_DIR, STABLECOINS

# ── Column groups ────────────────────────────────────────────────────────────

# Event-based 5m sources: NaN means no events occurred → zero
_EVENT_PREFIXES = (
    "mint_", "burn_", "net_flow_usd",
    "treasury_inflow_", "treasury_outflow_", "treasury_net_flow_usd",
    "tron_treasury_",
    "curve_",
)

# Daily series: NaN means no new value published → forward-fill
_DAILY_COLS = ["dxy", "vix", "t10y", "fedfunds", "fear_greed"]

# 5m market context with occasional gaps → forward-fill
_FFILL_5M_COLS = ["binance_btc_close", "binance_eth_close"]

# CoinAPI price bounds — outside these is a feed error, not a real market event
# UST is exempt: price legitimately collapsed below $0.50 in May 2022
_COINAPI_PRICE_COLS = ["coinapi_open", "coinapi_high", "coinapi_low", "coinapi_close"]
_COINAPI_LO, _COINAPI_HI = 0.50, 2.0
_COINAPI_PRICE_EXEMPT = {"ust"}


def _is_event_col(col: str) -> bool:
    return any(col == p or col.startswith(p) for p in _EVENT_PREFIXES)


def clean_coin(coin_key: str) -> pd.DataFrame:
    raw_path = MERGED_DIR / f"{coin_key}_5m_raw.parquet"
    if not raw_path.exists():
        print(f"  Raw file not found: {raw_path}. Run merge_sources.py first.")
        return pd.DataFrame()

    df = pd.read_parquet(raw_path)
    print(f"  Loaded {len(df):,} rows, {df.shape[1]} columns")

    # 1. Zero-fill event-based columns
    event_cols = [c for c in df.columns if _is_event_col(c)]
    if event_cols:
        df[event_cols] = df[event_cols].fillna(0)
        print(f"  Zero-filled {len(event_cols)} event columns")

    # 2. Forward-fill daily series
    daily_cols = [c for c in _DAILY_COLS if c in df.columns]
    if daily_cols:
        df[daily_cols] = df[daily_cols].ffill()
        print(f"  Forward-filled daily cols: {daily_cols}")

    # 3. Forward-fill 5m market context gaps
    ffill_cols = [c for c in _FFILL_5M_COLS if c in df.columns]
    if ffill_cols:
        df[ffill_cols] = df[ffill_cols].ffill()
        print(f"  Forward-filled 5m market cols: {ffill_cols}")

    # 4. Null and forward-fill CoinAPI price anomalies and gaps
    price_cols = [c for c in _COINAPI_PRICE_COLS if c in df.columns]
    if price_cols:
        if coin_key not in _COINAPI_PRICE_EXEMPT:
            bad_mask = (df["coinapi_close"] < _COINAPI_LO) | (df["coinapi_close"] > _COINAPI_HI)
            n_bad = bad_mask.sum()
            if n_bad:
                df.loc[bad_mask, price_cols] = pd.NA
                print(f"  Nulled {n_bad} CoinAPI price anomalies: {bad_mask[bad_mask].index[0]} … {bad_mask[bad_mask].index[-1]}")
        n_null = df["coinapi_close"].isna().sum()
        if n_null:
            df[price_cols] = df[price_cols].ffill()
            print(f"  Forward-filled {n_null} CoinAPI gaps")

    # 5. Trim head rows before all key columns have valid data (nothing to forward-fill from)
    head_cols = [c for c in ["coinapi_close", "binance_btc_close", "binance_eth_close"]
                 if c in df.columns]
    first_valids = [df[c].first_valid_index() for c in head_cols]
    first_valids = [ts for ts in first_valids if ts is not None]
    if first_valids:
        trim_to = max(first_valids)
        if trim_to != df.index[0]:
            n_trimmed = df.index.get_loc(trim_to)
            df = df.loc[trim_to:]
            print(f"  Trimmed {n_trimmed} head rows → first valid row: {trim_to}")

    # 6. Compute unified institutional flow signal
    # USDT: ETH treasury + TRON treasury (mint/burn is sparse/OFAC-only on ETH)
    # Others: on-chain mint/burn net flow
    # Sign convention: positive = redemption/outflow pressure for USDT; positive = demand for others
    if coin_key == "usdt":
        eth = df["treasury_net_flow_usd"] if "treasury_net_flow_usd" in df.columns else 0
        tron = df["tron_treasury_net_flow_usd"] if "tron_treasury_net_flow_usd" in df.columns else 0
        df["total_net_flow_usd"] = eth + tron
    elif "net_flow_usd" in df.columns:
        df["total_net_flow_usd"] = df["net_flow_usd"]

    # Summary
    remaining_nulls = df.isnull().sum()
    remaining_nulls = remaining_nulls[remaining_nulls > 0]
    if not remaining_nulls.empty:
        print(f"  Remaining nulls (expected for sparse 5m OHLCV / tail rows):")
        for col, n in remaining_nulls.sort_values(ascending=False).items():
            print(f"    {col}: {n/len(df)*100:.1f}% ({n:,})")
    else:
        print(f"  No remaining nulls")

    return df


def save(df: pd.DataFrame, coin_key: str) -> None:
    CLEANSED_DIR.mkdir(parents=True, exist_ok=True)
    path = CLEANSED_DIR / f"{coin_key}_5m.parquet"
    df.to_parquet(path)
    size_mb = path.stat().st_size / 1e6
    print(f"  Saved {path} ({len(df):,} rows, {size_mb:.1f} MB)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("coin", nargs="?", default="all", help="Coin key or 'all'")
    args = parser.parse_args()

    coins = list(STABLECOINS.keys()) if args.coin == "all" else [args.coin]

    for coin in coins:
        print(f"\n{'='*50}")
        print(f"  Cleaning {coin.upper()}")
        print(f"{'='*50}")
        df = clean_coin(coin)
        if not df.empty:
            save(df, coin)


if __name__ == "__main__":
    main()
