"""
feature_engineering.py — Build model-ready features for stablecoin depeg prediction.

Usage:
    python src/features/feature_engineering.py usdt
    python src/features/feature_engineering.py all

Output: data/processed/features/{coin}_5m_features.parquet
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import CLEANSED_DIR, FEATURES_DIR, STABLECOINS

# ---------------------------------------------------------------------------
# Time-bar constants
# ---------------------------------------------------------------------------
BARS_PER_HOUR = 12
BARS_PER_4H   = 48
BARS_PER_DAY  = 288
BARS_PER_30D  = 8_640

# ---------------------------------------------------------------------------
# Curve routing tables  (primary coin column per pool)
# ---------------------------------------------------------------------------
_COIN_CURVE_NET_SELL_COL = {
    "usdt":  "curve_3pool_usdt_net_sell_volume_usd",
    "usdc":  "curve_3pool_usdc_net_sell_volume_usd",
    "dai":   "curve_3pool_dai_net_sell_volume_usd",
    "busd":  "curve_busd_3crv_busd_net_sell_volume_usd",
    "ust":   None,   # summed from two pools below
    "usde":  "curve_usde_usdc_usde_net_sell_volume_usd",
    "rlusd": "curve_rlusd_usdc_rlusd_net_sell_volume_usd",
}

_COIN_CURVE_SOLD_COL = {
    "usdt":  "curve_3pool_usdt_sold_volume_usd",
    "usdc":  "curve_3pool_usdc_sold_volume_usd",
    "dai":   "curve_3pool_dai_sold_volume_usd",
    "busd":  "curve_busd_3crv_busd_sold_volume_usd",
    "ust":   None,
    "usde":  "curve_usde_usdc_usde_sold_volume_usd",
    "rlusd": "curve_rlusd_usdc_rlusd_sold_volume_usd",
}

_COIN_CURVE_BOUGHT_COL = {
    "usdt":  "curve_3pool_usdt_bought_volume_usd",
    "usdc":  "curve_3pool_usdc_bought_volume_usd",
    "dai":   "curve_3pool_dai_bought_volume_usd",
    "busd":  "curve_busd_3crv_busd_bought_volume_usd",
    "ust":   None,
    "usde":  "curve_usde_usdc_usde_bought_volume_usd",
    "rlusd": "curve_rlusd_usdc_rlusd_bought_volume_usd",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(df: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    """Safe column accessor — returns zeros if column is absent."""
    return df[name].fillna(default) if name in df.columns \
        else pd.Series(default, index=df.index, dtype="float64")


def _safe_zscore(series: pd.Series, window: int) -> pd.Series:
    """Rolling z-score; returns 0.0 where std is zero (sparse columns)."""
    roll = series.rolling(window, min_periods=1)
    mu   = roll.mean()
    std  = roll.std(ddof=0)   # population std — no NaN at window=1
    z    = (series - mu) / std.replace(0.0, float("nan"))
    return z.fillna(0.0)


# ---------------------------------------------------------------------------
# Feature functions
# ---------------------------------------------------------------------------

def add_price_features(df: pd.DataFrame, coin_key: str) -> pd.DataFrame:
    """Price momentum & volatility features (16 cols)."""
    pd_ = _col(df, "price_dev")

    # Rolling stats of price_dev
    for w, tag in [(3, "15min"), (BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h")]:
        df[f"price_dev_mean_{tag}"]    = pd_.rolling(w, min_periods=1).mean()
        df[f"price_dev_std_{tag}"]     = pd_.rolling(w, min_periods=1).std(ddof=0)
        df[f"price_dev_absmax_{tag}"]  = pd_.abs().rolling(w, min_periods=1).max()

    # 1-bar momentum
    df["price_dev_diff1"] = pd_.diff(1).fillna(0.0)

    # Bars above threshold
    flag_01 = (pd_.abs() > 0.001).astype(float)
    flag_03 = (pd_.abs() > 0.003).astype(float)
    for w, tag in [(3, "15min"), (BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h")]:
        df[f"bars_above_01pct_{tag}"] = flag_01.rolling(w, min_periods=1).sum()
        df[f"bars_above_03pct_{tag}"] = flag_03.rolling(w, min_periods=1).sum()

    # Intra-bar range
    hi  = _col(df, "coinapi_high")
    lo  = _col(df, "coinapi_low")
    cl  = _col(df, "coinapi_close", default=1.0)
    cl  = cl.replace(0.0, float("nan")).fillna(1.0)
    df["intrabar_range"] = (hi - lo) / cl

    return df


def add_onchain_features(df: pd.DataFrame, coin_key: str) -> pd.DataFrame:
    """On-chain flow features (~9 universal + coin-specific)."""
    net = _col(df, "total_net_flow_usd")

    # Universal rolling sums
    for w, tag in [(BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h"), (BARS_PER_DAY, "1d")]:
        df[f"net_flow_sum_{tag}"] = net.rolling(w, min_periods=1).sum()

    # Z-score vs 30-day window
    df["net_flow_zscore_30d"] = _safe_zscore(net, BARS_PER_30D)

    # Mint / burn rolling sums
    mint = _col(df, "mint_volume_usd")
    burn = _col(df, "burn_volume_usd")
    for w, tag in [(BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h")]:
        df[f"mint_sum_{tag}"] = mint.rolling(w, min_periods=1).sum()
        df[f"burn_sum_{tag}"] = burn.rolling(w, min_periods=1).sum()

    # Mint/burn ratio (12-bar)
    mint12 = mint.rolling(BARS_PER_HOUR, min_periods=1).sum()
    burn12 = burn.rolling(BARS_PER_HOUR, min_periods=1).sum()
    df["mint_burn_ratio_1h"] = mint12 / (burn12 + 1.0)

    # ---- USDT-specific: ETH + TRON treasury flows ----
    if coin_key == "usdt":
        for src, prefix in [("eth", "treasury"), ("tron", "tron_treasury")]:
            inflow  = _col(df, f"{prefix}_inflow_volume_usd")
            outflow = _col(df, f"{prefix}_outflow_volume_usd")
            for w, tag in [(BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h")]:
                df[f"{src}_treasury_inflow_sum_{tag}"]  = inflow.rolling(w, min_periods=1).sum()
                df[f"{src}_treasury_outflow_sum_{tag}"] = outflow.rolling(w, min_periods=1).sum()

    # ---- USDC-specific: Solana flows ----
    if coin_key == "usdc":
        sol_net = _col(df, "sol_net_flow_usd")
        for w, tag in [(BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h")]:
            df[f"sol_net_flow_sum_{tag}"] = sol_net.rolling(w, min_periods=1).sum()
        df["sol_net_flow_zscore_30d"] = _safe_zscore(sol_net, BARS_PER_30D)

    # ---- RLUSD-specific: XRPL flows ----
    if coin_key == "rlusd":
        xrpl_net = _col(df, "xrpl_net_flow_usd")
        for w, tag in [(BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h")]:
            df[f"xrpl_net_flow_sum_{tag}"] = xrpl_net.rolling(w, min_periods=1).sum()

    return df


def add_curve_features(df: pd.DataFrame, coin_key: str) -> pd.DataFrame:
    """Curve DEX sell-pressure features (5 cols)."""

    # UST: sum both pools
    if coin_key == "ust":
        net_sell = (
            _col(df, "curve_ust_3crv_ust_net_sell_volume_usd")
            + _col(df, "curve_ust_wormhole_3crv_wust_net_sell_volume_usd")
        )
        sold   = (
            _col(df, "curve_ust_3crv_ust_sold_volume_usd")
            + _col(df, "curve_ust_wormhole_3crv_wust_sold_volume_usd")
        )
        bought = (
            _col(df, "curve_ust_3crv_ust_bought_volume_usd")
            + _col(df, "curve_ust_wormhole_3crv_wust_bought_volume_usd")
        )
    else:
        net_sell = _col(df, _COIN_CURVE_NET_SELL_COL.get(coin_key, ""))
        sold     = _col(df, _COIN_CURVE_SOLD_COL.get(coin_key, ""))
        bought   = _col(df, _COIN_CURVE_BOUGHT_COL.get(coin_key, ""))

    for w, tag in [(3, "15min"), (BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h")]:
        df[f"curve_net_sell_sum_{tag}"] = net_sell.rolling(w, min_periods=1).sum()

    df["curve_net_sell_zscore_30d"] = _safe_zscore(net_sell, BARS_PER_30D)

    # Sell/buy ratio over 1h
    sold12   = sold.rolling(BARS_PER_HOUR, min_periods=1).sum()
    bought12 = bought.rolling(BARS_PER_HOUR, min_periods=1).sum()
    df["curve_sell_buy_ratio_1h"] = sold12 / (bought12 + 1.0)

    return df


def add_market_features(df: pd.DataFrame) -> pd.DataFrame:
    """BTC/ETH returns, BTC volatility, VIX/Fear&Greed momentum (8 cols)."""
    btc = _col(df, "binance_btc_close", default=float("nan")).ffill()
    eth = _col(df, "binance_eth_close", default=float("nan")).ffill()
    vix = _col(df, "vix", default=float("nan")).ffill()
    fg  = _col(df, "fear_greed", default=float("nan")).ffill()

    df["btc_return_1h"]  = btc.pct_change(BARS_PER_HOUR).fillna(0.0)
    df["btc_return_4h"]  = btc.pct_change(BARS_PER_4H).fillna(0.0)
    df["eth_return_1h"]  = eth.pct_change(BARS_PER_HOUR).fillna(0.0)
    df["eth_return_4h"]  = eth.pct_change(BARS_PER_4H).fillna(0.0)

    btc_ret1 = btc.pct_change(1).fillna(0.0)
    df["btc_vol_4h"]  = btc_ret1.rolling(BARS_PER_4H,  min_periods=1).std(ddof=0)
    df["btc_vol_1d"]  = btc_ret1.rolling(BARS_PER_DAY, min_periods=1).std(ddof=0)

    df["vix_diff_1d"]       = vix.diff(BARS_PER_DAY).fillna(0.0)
    df["fear_greed_diff_1d"] = fg.diff(BARS_PER_DAY).fillna(0.0)

    return df


def add_cross_coin_features(
    df: pd.DataFrame,
    coin_key: str,
    price_dev_map: dict[str, pd.Series],
) -> pd.DataFrame:
    """Other coins' price_dev at same timestamp (6 cols, safe reindex)."""
    for other, series in price_dev_map.items():
        if other == coin_key:
            continue
        aligned = series.reindex(df.index).fillna(0.0)
        df[f"cross_{other}_price_dev"] = aligned.values  # avoid index alignment issues
    return df


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Hour-of-day, day-of-week, weekend flag, US market hours (4 cols)."""
    idx = df.index
    if hasattr(idx, "tz") and idx.tz is not None:
        utc_idx = idx
    else:
        utc_idx = idx.tz_localize("UTC")

    df["hour_of_day"]       = utc_idx.hour
    df["day_of_week"]       = utc_idx.dayofweek   # 0=Mon … 6=Sun
    df["is_weekend"]        = (utc_idx.dayofweek >= 5).astype("int8")
    # US market hours: 13:30–20:00 UTC Mon–Fri
    us_market = (
        (utc_idx.dayofweek < 5)
        & (utc_idx.hour * 60 + utc_idx.minute >= 13 * 60 + 30)
        & (utc_idx.hour * 60 + utc_idx.minute < 20 * 60)
    )
    df["is_us_market_hours"] = us_market.astype("int8")

    return df


def add_lag_features(df: pd.DataFrame, coin_key: str) -> pd.DataFrame:
    """Lag 1, 3, 6, 12 bars of price_dev, net_flow, Curve net_sell (12 cols)."""
    pd_  = _col(df, "price_dev")
    nf_  = _col(df, "total_net_flow_usd")

    # Primary Curve net sell (already computed in add_curve_features as raw col)
    if coin_key == "ust":
        cv_ = (
            _col(df, "curve_ust_3crv_ust_net_sell_volume_usd")
            + _col(df, "curve_ust_wormhole_3crv_wust_net_sell_volume_usd")
        )
    else:
        cv_ = _col(df, _COIN_CURVE_NET_SELL_COL.get(coin_key, ""))

    for lag in [1, 3, 6, 12]:
        df[f"lag{lag}_price_dev"]           = pd_.shift(lag).fillna(0.0)
        df[f"lag{lag}_net_flow_usd"]        = nf_.shift(lag).fillna(0.0)
        df[f"lag{lag}_curve_net_sell"]      = cv_.shift(lag).fillna(0.0)

    return df


def select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the unwanted label columns; keep everything else."""
    drop_cols = [c for c in ["depeg_next_5min", "depeg_next_30min", "depeg_next_4h"]
                 if c in df.columns]
    return df.drop(columns=drop_cols)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def load_all_price_devs() -> dict[str, pd.Series]:
    """Pre-load price_dev from all coins for cross-coin features."""
    price_dev_map: dict[str, pd.Series] = {}
    for coin_key in STABLECOINS:
        path = CLEANSED_DIR / f"{coin_key}_5m.parquet"
        if not path.exists():
            print(f"  [warn] {path} not found — skipping cross-coin for {coin_key}")
            continue
        df = pd.read_parquet(path, columns=["price_dev"])
        price_dev_map[coin_key] = df["price_dev"].astype("float64")
    return price_dev_map


def engineer_coin(coin_key: str, price_dev_map: dict[str, pd.Series]) -> pd.DataFrame:
    path = CLEANSED_DIR / f"{coin_key}_5m.parquet"
    print(f"[{coin_key}] Loading {path} …")
    df = pd.read_parquet(path)

    # Cast nullable-int label columns to float64 to avoid rolling issues
    for col in ["depeg", "depeg_next_5min", "depeg_next_30min", "depeg_next_1h", "depeg_next_4h"]:
        if col in df.columns and pd.api.types.is_extension_array_dtype(df[col]):
            df[col] = df[col].astype("float64")

    # Run feature categories
    df = add_price_features(df, coin_key)
    df = add_onchain_features(df, coin_key)
    df = add_curve_features(df, coin_key)
    df = add_market_features(df)
    df = add_cross_coin_features(df, coin_key, price_dev_map)
    df = add_temporal_features(df)
    df = add_lag_features(df, coin_key)
    df = select_output_columns(df)

    return df


def save(df: pd.DataFrame, coin_key: str) -> None:
    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    out = FEATURES_DIR / f"{coin_key}_5m_features.parquet"
    df.to_parquet(out)
    n_feat = len(df.columns)
    print(f"[{coin_key}] Saved {len(df):,} rows × {n_feat} cols → {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Feature engineering for stablecoin depeg prediction")
    parser.add_argument("coin", help="Coin key (e.g. usdt) or 'all'")
    args = parser.parse_args()

    coins = list(STABLECOINS.keys()) if args.coin == "all" else [args.coin.lower()]

    print("Pre-loading price_dev for cross-coin features …")
    price_dev_map = load_all_price_devs()

    for coin_key in coins:
        if coin_key not in STABLECOINS:
            print(f"Unknown coin: {coin_key}")
            continue
        df = engineer_coin(coin_key, price_dev_map)
        save(df, coin_key)

    print("Done.")


if __name__ == "__main__":
    main()
