# Stablecoin Depeg Prediction at 5-Minute Resolution

CMU MSBA Capstone Project — predicting stablecoin depeg events using high-frequency
on-chain and market data.

## Research Question

Can we predict stablecoin depeg events 5 minutes to 4 hours in advance using a combination
of on-chain mint/burn flows, DeFi liquidity signals, and macro market context — all at
5-minute resolution?

---

## Stablecoins

| Coin | Type | Status | Coverage |
|------|------|--------|----------|
| USDT (Tether) | Fiat-backed | Active | Aug 2017 → Feb 2026 |
| USDC (Circle) | Fiat-backed | Active | Oct 2018 → Feb 2026 |
| DAI (MakerDAO) | Crypto-collateralized | Active | Dec 2017 → Feb 2026 |
| BUSD (Paxos) | Fiat-backed | Discontinued Mar 2023 | Sep 2019 → Mar 2023 |
| UST (Terra) | Algorithmic | Failed May 2022 | Sep 2020 → May 2022 |
| USDe (Ethena) | Synthetic | Active | Apr 2024 → Feb 2026 |
| RLUSD (Ripple) | Fiat-backed | Active | Apr 2025 → Feb 2026 |

Failed and discontinued coins are included intentionally — they provide real depeg examples
for model training.

---

## Depeg Definition

A depeg episode is **3 or more consecutive 5-minute bars** where `|price − $1.00| > 0.5%`.
The 15-minute persistence requirement filters transient tick noise.

---

## Feature Sources

| Source | Signal | Frequency |
|--------|--------|-----------|
| CoinAPI VWAP | Stablecoin price (primary target) | 5m |
| CoinAPI Market Data | Order book snapshots | 5m |
| Binance | BTC/ETH market context | 5m |
| Etherscan V2 | Ethereum mint/burn events + USDT treasury flows | 5m (event-level) |
| TronGrid | USDT TRON treasury flows | 5m (event-level) |
| OmniExplorer | USDT Omni Layer (Bitcoin) treasury flows | 5m (event-level) |
| Helius | USDC Solana mint/burn events (Aug 2024 →) | 5m (event-level) |
| Dune Analytics | USDC Solana mint/burn historical baseline (Oct 2020 →) | 5m (event-level) |
| XRPL public RPC | RLUSD mint/burn + DEX flows | 5m (event-level) |
| Curve Finance | 3pool, USDe/USDC, RLUSD/USDC swap flows | 5m (event-level) |
| DeFiLlama | Stablecoin circulating supply | Daily → 5m ffill |
| FRED | DXY, VIX, T10Y, Fed Funds rate | Daily → 5m ffill |
| AltIndex | CNN Fear & Greed Index | Daily → 5m ffill |

See `data/README.md` for full column reference and pipeline documentation.

---

## Repo Structure

```
├── config/
│   └── settings.py               # Coin configs, date ranges, API settings
├── data/
│   ├── README.md                  # Full column reference + pipeline docs
│   ├── raw/                       # Raw source files organized by provider
│   │   ├── binance/
│   │   ├── coinapi/
│   │   ├── curve/
│   │   ├── defillama/
│   │   ├── fred/
│   │   ├── market/
│   │   ├── omni/
│   │   └── onchain/
│   └── processed/
│       ├── merged/                # NB01 output — wide join, no cleaning ({coin}_5m_raw.parquet)
│       ├── cleansed/              # NB02 output — zero-filled, ffilled, labeled ({coin}_5m.parquet)
│       ├── features/              # NB04/05 output — engineered features + pooled dataset
│       └── predictions/           # NB05 output — test set predictions
├── notebooks/
│   ├── 01_merge_raw_data.ipynb    # Raw source join → merged parquet
│   ├── 02_clean_merged_data.ipynb # Zero-fill, ffill, label → cleansed parquet
│   ├── 03_eda_all_coins.ipynb     # EDA + severity labelling → cleansed parquet with severity
│   ├── 04_feature_engineering.ipynb # Feature engineering + selection → pooled_5m.parquet
│   └── 05_modeling.ipynb          # CatBoost training, LOEO validation, predictions
├── src/
│   └── data_collection_scripts/   # Data ingestion scripts (see src/README.md)
└── docs/                          # Reference documents and literature
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API keys

Copy `.env.example` to `.env` and fill in your keys:

```bash
cp .env.example .env
```

| Key | Required | Source |
|-----|----------|--------|
| `COINAPI_KEY` | Yes | coinapi.io (VWAP fiat pairs) |
| `COINAPI_MARKETDATA_KEY` | Yes | coinapi.io (order book — separate product) |
| `ETHERSCAN_API_KEY` | Yes | etherscan.io |
| `FRED_API_KEY` | Yes | fred.stlouisfed.org |
| `HELIUS_API_KEY` | Yes | helius.xyz (Solana USDC) |
| `DUNE_API_KEY` | Yes | dune.com (Solana USDC historical) |
| `TRONGRID_API_KEY` | No | trongrid.io (free tier works without key) |
| `COINGECKO_API_KEY` | No | coingecko.com (higher rate limits) |

---

## Running the Pipeline

### Collect data

```bash
# All sources for one coin
python src/data_collection_scripts/collect_all.py usdt

# All sources for all coins
python src/data_collection_scripts/collect_all.py all

# Individual sources
python src/data_collection_scripts/collect_binance.py all
python src/data_collection_scripts/collect_onchain.py all
python src/data_collection_scripts/collect_curve.py all
python src/data_collection_scripts/collect_xrpl.py
python src/data_collection_scripts/collect_dune.py --query-id <id>
python src/data_collection_scripts/collect_fred.py
python src/data_collection_scripts/collect_market.py
```

### Build modeling-ready dataset

Run the notebooks in order from the project root:

```bash
jupyter nbconvert --to notebook --execute --inplace notebooks/01_merge_raw_data.ipynb
jupyter nbconvert --to notebook --execute --inplace notebooks/02_clean_merged_data.ipynb
jupyter nbconvert --to notebook --execute --inplace notebooks/03_eda_all_coins.ipynb
jupyter nbconvert --to notebook --execute --inplace notebooks/04_feature_engineering.ipynb
jupyter nbconvert --to notebook --execute --inplace notebooks/05_modeling.ipynb
```

Output:
- NB01 → `data/processed/merged/{coin}_5m_raw.parquet`
- NB02 → `data/processed/cleansed/{coin}_5m.parquet`
- NB03 → `data/processed/cleansed/{coin}_5m_clean.parquet` (adds severity labels)
- NB04 → `data/processed/features/pooled_5m.parquet`
- NB05 → `data/models/` (CatBoost models), `data/processed/predictions/predictions_test.parquet`

---

## Dataset Summary

| Coin | Rows | Columns | Date Range |
|------|-----:|--------:|------------|
| USDT | 897,936 | 66 | Aug 2017 → Feb 2026 |
| USDC | 775,506 | 45 | Oct 2018 → Feb 2026 |
| DAI | 828,963 | 45 | Dec 2017 → Feb 2026 |
| BUSD | 370,848 | 30 | Sep 2019 → Mar 2023 |
| UST | 153,961 | 24 | Sep 2020 → May 2022 |
| USDe | 200,928 | 40 | Apr 2024 → Feb 2026 |
| RLUSD | 96,192 | 40 | Apr 2025 → Feb 2026 |
