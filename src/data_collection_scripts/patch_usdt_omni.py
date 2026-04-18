"""
patch_usdt_omni.py — Merge Omni treasury data into the USDT pipeline

Steps:
  1. Add Omni columns to data/processed/cleansed/usdt_5m.parquet
  2. Recompute affected features in data/processed/features/usdt_5m_features.parquet
     - net_flow now = ETH_treasury + TRON_treasury + Omni_treasury
     - All net_flow_* derivatives recomputed
     - New omni_treasury_inflow_sum_1h/4h, omni_treasury_outflow_sum_1h/4h added
  3. Rebuild data/processed/features/pooled_5m.parquet (replace USDT rows)

Run: python src/data_collection_scripts/patch_usdt_omni.py
"""

from pathlib import Path
import pandas as pd

ROOT         = Path(__file__).parents[2]
CLEANSED_DIR = ROOT / "data" / "processed" / "cleansed"
FEATURES_DIR = ROOT / "data" / "processed" / "features"
OMNI_5M      = ROOT / "data" / "raw" / "omni" / "usdt_omni_5m.parquet"

# Bar constants (must match feature_engineering notebook)
BARS_PER_HOUR = 12
BARS_PER_4H   = 48
BARS_PER_DAY  = 288
BARS_PER_7D   = 2_016
BARS_PER_30D  = 8_640


def _col(df, name, default=0.0):
    return df[name].fillna(default) if name in df.columns \
        else pd.Series(default, index=df.index, dtype="float64")


def _safe_zscore(series, window):
    roll = series.rolling(window, min_periods=1)
    mu   = roll.mean()
    std  = roll.std(ddof=0)
    return (series - mu).div(std.replace(0.0, float("nan"))).fillna(0.0)


# ── Step 1: patch cleansed USDT ───────────────────────────────────────────────

print("Step 1: patching cleansed USDT parquet...")

usdt = pd.read_parquet(CLEANSED_DIR / "usdt_5m.parquet")
omni = pd.read_parquet(OMNI_5M)

# Drop any Omni columns already present (idempotent)
omni_cols = list(omni.columns)
for c in omni_cols:
    if c in usdt.columns:
        usdt.drop(columns=[c], inplace=True)

# Reindex Omni to USDT's full index, zero-fill (event columns, not ffill)
omni_aligned = omni.reindex(usdt.index).fillna(0.0)
usdt = pd.concat([usdt, omni_aligned], axis=1)

usdt.to_parquet(CLEANSED_DIR / "usdt_5m.parquet")
print(f"  Added {omni_cols}")
print(f"  Omni non-zero rows: {(omni_aligned['omni_treasury_inflow_volume_usd'] > 0).sum()} inflows, "
      f"{(omni_aligned['omni_treasury_outflow_volume_usd'] > 0).sum()} outflows")
print(f"  Saved → {CLEANSED_DIR / 'usdt_5m.parquet'}")


# ── Step 2: recompute affected USDT features ──────────────────────────────────

print("\nStep 2: recomputing USDT features...")

feat = pd.read_parquet(FEATURES_DIR / "usdt_5m_features.parquet")

# Build updated net flow: ETH + TRON + Omni
eth_net  = _col(usdt, "treasury_net_flow_usd")
tron_net = _col(usdt, "tron_treasury_net_flow_usd")
omni_net = _col(usdt, "omni_treasury_net_flow_usd")
net      = eth_net + tron_net + omni_net

print(f"  Old net non-zero: {(feat['total_net_flow_usd'] != 0).sum()}")
print(f"  New net non-zero: {(net != 0).sum()}")

# Overwrite total_net_flow_usd
feat["total_net_flow_usd"] = net.values

# Recompute net_flow rolling features
for w, tag in [(BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h"), (BARS_PER_DAY, "1d")]:
    feat[f"net_flow_sum_{tag}"] = net.rolling(w, min_periods=1).sum().values

feat["net_flow_zscore_7d"]  = _safe_zscore(net, BARS_PER_7D).values
feat["net_flow_zscore_30d"] = _safe_zscore(net, BARS_PER_30D).values

for w, tag in [(BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h")]:
    feat[f"net_flow_vol_{tag}"] = net.rolling(w, min_periods=2).std(ddof=0).fillna(0.0).values

feat["net_flow_vol_zscore_30d"] = _safe_zscore(
    net.rolling(BARS_PER_4H, min_periods=2).std(ddof=0).fillna(0.0), BARS_PER_30D
).values

# Update lag features for net_flow
for lag in [1, 3, 6, 12]:
    feat[f"lag{lag}_net_flow_usd"] = net.shift(lag).fillna(0.0).values

# Add Omni rolling features
omni_in  = _col(usdt, "omni_treasury_inflow_volume_usd")
omni_out = _col(usdt, "omni_treasury_outflow_volume_usd")
for w, tag in [(BARS_PER_HOUR, "1h"), (BARS_PER_4H, "4h")]:
    feat[f"omni_treasury_inflow_sum_{tag}"]  = omni_in.rolling(w, min_periods=1).sum().values
    feat[f"omni_treasury_outflow_sum_{tag}"] = omni_out.rolling(w, min_periods=1).sum().values

feat.to_parquet(FEATURES_DIR / "usdt_5m_features.parquet")
new_omni_cols = [c for c in feat.columns if c.startswith("omni_")]
print(f"  New Omni feature columns: {new_omni_cols}")
print(f"  Saved → {FEATURES_DIR / 'usdt_5m_features.parquet'}")


# ── Step 3: rebuild pooled dataset ───────────────────────────────────────────

print("\nStep 3: rebuilding pooled_5m.parquet...")

pooled = pd.read_parquet(FEATURES_DIR / "pooled_5m.parquet")
print(f"  Pooled before: {pooled.shape}")

# Drop old USDT rows
non_usdt = pooled[pooled["coin"] != "usdt"]

# Build new USDT rows from updated features file
# Add coin column
feat["coin"] = "usdt"
usdt_new = feat.copy()

# Align columns — add any missing columns with NaN
all_cols = list(pooled.columns)
for c in all_cols:
    if c not in usdt_new.columns:
        usdt_new[c] = float("nan")

# Add new Omni columns to pooled (other coins get NaN)
new_cols = [c for c in usdt_new.columns if c not in pooled.columns]
if new_cols:
    for c in new_cols:
        non_usdt[c] = float("nan")

usdt_new = usdt_new[[c for c in usdt_new.columns if c in all_cols + new_cols]]
pooled_new = pd.concat([non_usdt, usdt_new], axis=0).sort_index()

print(f"  Pooled after:  {pooled_new.shape}")
print(f"  New columns added to pooled: {new_cols}")

pooled_new.to_parquet(FEATURES_DIR / "pooled_5m.parquet")
print(f"  Saved → {FEATURES_DIR / 'pooled_5m.parquet'}")

print("\nDone.")
