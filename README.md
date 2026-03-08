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
| Binance | BTC/ETH market context | 5m |
| Etherscan V2 | Ethereum mint/burn events | 5m (event-level) |
| TronGrid | USDT TRON treasury flows | 5m (event-level) |
| Dune Analytics | Solana USDC mint/burn flows | 5m (event-level) |
| XRPL public RPC | RLUSD mint/burn + DEX flows | 5m (event-level) |
| Curve Finance | 3pool, USDe/USDC, RLUSD/USDC swap flows | 5m (event-level) |
| FRED | DXY, VIX, T10Y, Fed Funds rate | Daily → 5m ffill |
| AltIndex | CNN Fear & Greed Index | Daily → 5m ffill |

See `data/README.md` for full column reference and pipeline documentation.

---

## Repo Structure

```
├── config/
│   └── settings.py          # Coin configs, date ranges, API settings
├── data/
│   ├── README.md            # Full column reference + pipeline docs
│   ├── processed/           # Modeling-ready Parquet files ({coin}_5m.parquet)
│   └── raw/                 # Raw source files organized by provider
│       ├── binance/
│       ├── coinapi/
│       ├── curve/
│       ├── fred/
│       ├── market/
│       └── onchain/
├── src/
│   └── data/
│       ├── collect_all.py          # Runs all collectors
│       ├── collect_binance.py      # Binance 5m OHLCV
│       ├── collect_coinapi.py      # CoinAPI VWAP fiat pairs
│       ├── collect_onchain.py      # Ethereum mint/burn + USDT treasury
│       ├── collect_tron.py         # USDT TRON treasury flows
│       ├── collect_curve.py        # Curve pool swap events
│       ├── collect_xrpl.py         # RLUSD XRPL mint/burn + DEX
│       ├── collect_dune.py         # Solana USDC mint/burn via Dune API
│       ├── collect_fred.py         # FRED macro data
│       ├── collect_market.py       # BTC/ETH daily + Fear & Greed
│       ├── merge_sources.py        # Join all sources → {coin}_5m_raw.parquet
│       ├── clean_data.py           # Fill/patch → {coin}_5m.parquet
│       └── label_data.py           # Add depeg labels → {coin}_5m.parquet
└── scripts/                 # Utility and exploration scripts
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
| `COINAPI_KEY` | Yes | coinapi.io |
| `ETHERSCAN_API_KEY` | Yes | etherscan.io |
| `FRED_API_KEY` | Yes | fred.stlouisfed.org |
| `HELIUS_API_KEY` | Yes | helius.xyz (Solana) |
| `DUNE_API_KEY` | Yes | dune.com |
| `TRONGRID_API_KEY` | No | trongrid.io (free tier works without key) |
| `COINGECKO_API_KEY` | No | coingecko.com (higher rate limits) |

---

## Running the Pipeline

### Collect data

```bash
# All sources for one coin
python src/data/collect_all.py usdt

# All sources for all coins
python src/data/collect_all.py all

# Individual sources
python src/data/collect_binance.py all
python src/data/collect_onchain.py all
python src/data/collect_curve.py all
python src/data/collect_xrpl.py
python src/data/collect_dune.py --query-id <id>
python src/data/collect_fred.py
python src/data/collect_market.py
```

### Build modeling-ready dataset

Run in order:

```bash
# 1. Join all sources into a wide 5m dataframe (no cleaning)
python src/data/merge_sources.py all

# 2. Zero-fill events, forward-fill daily series, patch price anomalies
python src/data/clean_data.py all

# 3. Add depeg labels
python src/data/label_data.py all
```

Output: `data/processed/{coin}_5m.parquet` — one file per coin, modeling-ready.

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
