# notebooks/

All post-collection processing, EDA, feature engineering, and modeling live here. Notebooks are designed to run in order — each reads the outputs of the previous stage.

## Pipeline Overview

### Shared (all depeg types)

| Notebook | Reads | Writes | Description |
|----------|-------|--------|-------------|
| `01_merge_raw_data.ipynb` | `data/raw/` | `data/processed/merged/{coin}_5m_raw.parquet` | Pure join of all raw sources onto a common UTC 5m index. No cleaning. |
| `02_clean_merged_data.ipynb` | `merged/` | `data/processed/cleansed/{coin}_5m.parquet` | Zero-fill event columns, forward-fill daily series, attach depeg labels. |
| `03_eda.ipynb` | `cleansed/` | — | Full EDA across all coins and all depeg directions — price timelines, landmark events, on-chain signal elevation. |

### Downside_Depeg/ — Below-Peg Prediction Pipeline

Scoped to **downside depeg events only** (`price_dev < −0.5%` for ≥3 consecutive bars). Target: `depeg_next_4h_down`.

| Notebook | Reads | Writes | Description |
|----------|-------|--------|-------------|
| `03b_eda_downside.ipynb` | `cleansed/` | — | EDA scoped to below-peg events only; signal elevation analysis for on-chain and Curve flows. |
| `04_feature_engineering.ipynb` | `cleansed/` | `features/{coin}_5m_features.parquet` | ~87 features per coin: price deviation, rolling stats, on-chain flows, Curve DEX pressure, market context, lags. |
| `05_build_pooled_dataset.ipynb` | `features/` | `features/pooled_5m.parquet` | Stack all 7 coins into a single modeling-ready file (3.3M rows, `coin_key` column added). |
| `06_eda_features.ipynb` | `features/pooled_5m.parquet` | — | Feature distributions, class imbalance, correlation heatmap, depeg-rate by coin. |
| `07_depeg_event_study.ipynb` | `features/{coin}_5m_features.parquet` | — | Event study around historical depegs — do treasury flows and Curve sell pressure lead price breaks? |
| `08_feature_selection.ipynb` | `features/pooled_5m.parquet` | `features/selected_features.json` | Variance filter → correlation pruning → mutual information → L1 logistic → Random Forest. Outputs final feature list. |
| `09_baseline_models.ipynb` | `pooled_5m.parquet` + `selected_features.json` | — | Horizon comparison (1h / 4h / 24h) and algorithm comparison (LR / RF / LightGBM / XGBoost / CatBoost) at default hyperparameters. |
| `10_final_model.ipynb` | `pooled_5m.parquet` + `selected_features.json` | `data/models/nb10_catboost.cbm` `nb10_test_predictions.parquet` `nb10_val_predictions.parquet` `nb10_meta.json` | Tuned CatBoost via 50-trial Optuna TPE study. Saves model, predictions, and metadata for downstream notebooks. |
| `11_threshold_and_ops.ipynb` | `data/models/nb10_meta.json` | — | Threshold sweep on val set (max F2), test performance, alert rate, lead time per event, false-alert days. |
| `12_loeo_validation.ipynb` | `data/models/nb10_meta.json` | — | Leave-One-Event-Out validation — retrain without each known depeg event and check whether the model still fires. |

## Running the Pipeline

```bash
BASE=/path/to/capstone_5min_global

# Shared
jupyter nbconvert --to notebook --execute --inplace $BASE/notebooks/01_merge_raw_data.ipynb
jupyter nbconvert --to notebook --execute --inplace $BASE/notebooks/02_clean_merged_data.ipynb
jupyter nbconvert --to notebook --execute --inplace $BASE/notebooks/03_eda.ipynb

# Downside pipeline (NB09 and NB10 are slow — 20–60 min each)
for nb in 03b 04 05 06 07 08 09 10 11 12; do
  jupyter nbconvert --to notebook --execute --inplace \
    $BASE/notebooks/Downside_Depeg/${nb}*.ipynb
done
```

## Notes

- All notebooks auto-detect the project root by walking up from `cwd` looking for `config/settings.py` — they run correctly from any subfolder.
- NB10 runs a 50-trial Optuna HPO study; expect 30–60 minutes depending on hardware.
- NB11 and NB12 depend on outputs from NB10 (`nb10_catboost.cbm`, `nb10_meta.json`).
