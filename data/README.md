# Data Directory — Stablecoin Depeg Prediction

## Overview

5-minute resolution dataset covering 7 stablecoins from their respective launch dates through
February 28, 2026. Each coin has a single modeling-ready Parquet file in `processed/`.

## Files

### Processed (modeling-ready)

| File | Coin | Rows | Cols | Date Range | Notes |
|------|------|-----:|-----:|------------|-------|
| `usdt_5m.parquet` | Tether (USDT) | 897,936 | 66 | 2017-08-17 → 2026-02-28 | Richest feature set |
| `usdc_5m.parquet` | USD Coin (USDC) | 775,506 | 45 | 2018-10-16 → 2026-02-28 | |
| `dai_5m.parquet`  | Dai (DAI) | 828,963 | 45 | 2018-04-13 → 2026-02-28 | |
| `busd_5m.parquet` | Binance USD (BUSD) | 370,848 | 30 | 2019-09-20 → 2023-03-31 | Discontinued Mar 2023 |
| `ust_5m.parquet`  | TerraUSD (UST) | 153,961 | 24 | 2020-11-23 → 2022-05-12 | Failed May 2022 |
| `usde_5m.parquet` | Ethena USDe (USDe) | 200,928 | 40 | 2024-04-02 → 2026-02-28 | |
| `rlusd_5m.parquet`| Ripple USD (RLUSD) | 96,192 | 40 | 2025-04-01 → 2026-02-28 | |

CSV copies are in `csv/processed/` for convenience.

`{coin}_5m_raw.parquet` files are the pre-cleaning joins (before zero-fill, forward-fill, and
anomaly patching). Use `{coin}_5m.parquet` for modeling.

---

## Column Reference

All columns are present in every file unless noted under **Availability**.

---

### Price — CoinAPI VWAP

Source: CoinAPI `IDX_REFRATE_VWAP_{COIN}` — volume-weighted average price across major exchanges,
reported as a direct USD fiat pair. Feed errors (prices outside [0.50, 2.00]) are nulled and
forward-filled; UST is exempt since its collapse legitimately breached those bounds.

| Column | Type | Description |
|--------|------|-------------|
| `coinapi_open` | float | 5m bar open price (USD) |
| `coinapi_high` | float | 5m bar high price (USD) |
| `coinapi_low` | float | 5m bar low price (USD) |
| `coinapi_close` | float | 5m bar close price (USD) — primary price signal |
| `coinapi_tick_count` | float | Number of underlying trades aggregated; NaN during forward-filled gaps |

---

### Market Context — Binance (shared across all coins)

Source: Binance public API. These columns are identical for all 7 coins — they provide shared
crypto market context. BTC/ETH prices are from Binance rather than CoinAPI as they are freely
available and sufficient precision for market context.

| Column | Type | Description |
|--------|------|-------------|
| `binance_btc_close` | float | BTC/USDT 5m close price (USD) |
| `binance_eth_close` | float | ETH/USDT 5m close price (USD) |

---

### Binance Trading Pairs — USDT only

Source: Binance public API. USDT has the most liquid cross-pairs on Binance.
The USDCUSDT pair was listed mid-2019 so ~21% of rows are null at the head.

| Column | Type | Description |
|--------|------|-------------|
| `binance_usdcusdt_open/high/low/close` | float | USDC/USDT 5m OHLC (proxy for USDT vs USDC relative price) |
| `binance_usdcusdt_volume` | float | USDC volume traded (base asset) |
| `binance_usdcusdt_quote_volume` | float | USDT volume traded (quote asset) |
| `binance_usdcusdt_trades` | float | Number of trades in the 5m bar |
| `binance_usdcusdt_taker_buy_volume` | float | Volume where taker was the buyer (aggression signal) |
| `binance_usdcusdt_taker_buy_quote_volume` | float | Quote volume of taker buys |
| `binance_usdcusdt_buy_ratio` | float | `taker_buy_volume / volume` — 1.0 = all buys, 0.0 = all sells |
| `binance_usdcusdt_spread_proxy` | float | `(high - low) / close` — intra-bar volatility proxy |

**Availability:** USDT only.

---

### On-Chain Mint / Burn — Ethereum

Source: Etherscan V2 API. ERC-20 `Transfer` events from/to the zero address (`0x0`).

- **USDT**: Minting is infrequent (~230 events in 7 years, large batches). Burns only via
  `DestroyedBlackFunds` (OFAC seizures). `net_flow_usd` is sparse and not the primary flow
  signal for USDT — use `total_net_flow_usd` instead.
- **USDC**: Mint and Burn are called on every issuance/redemption — high frequency, clean signal.
- **DAI, USDe, RLUSD, BUSD**: Transfer from/to 0x0 captures minting and burning.

| Column | Type | Description |
|--------|------|-------------|
| `mint_count` | float | Number of mint events in this 5m window (zero-filled) |
| `mint_volume_usd` | float | USD value of tokens minted (zero-filled) |
| `burn_count` | float | Number of burn events (zero-filled) |
| `burn_volume_usd` | float | USD value of tokens burned (zero-filled) |
| `net_flow_usd` | float | `mint_volume − burn_volume` — net new supply on Ethereum (zero-filled) |

**Availability:** USDT, USDC, DAI, USDe, RLUSD, BUSD. Not available for UST (Terra-native).

---

### USDT ETH Treasury Flows — USDT only

Source: Etherscan V2 API. Tracks ERC-20 USDT `Transfer` events to/from the known Tether
treasury wallet on Ethereum (`0x5754284f...007b949`). Institutions send USDT to this address
for off-chain USD redemption; Tether re-issues from this address.

**Hypothesis:** Treasury inflow spikes (institutions redeeming at par) may precede open-market
depeg by minutes to hours.

| Column | Type | Description |
|--------|------|-------------|
| `treasury_inflow_count` | float | # transfers into Tether treasury (redemption pressure) |
| `treasury_inflow_volume_usd` | float | USD volume of USDT sent to treasury |
| `treasury_outflow_count` | float | # transfers out of Tether treasury (re-issuance) |
| `treasury_outflow_volume_usd` | float | USD volume of USDT re-issued from treasury |
| `treasury_net_flow_usd` | float | `outflow − inflow` (positive = net re-issuance; negative = net redemption pressure) |

**Availability:** USDT only.

---

### USDT TRON Treasury Flows — USDT only

Source: TronGrid API. Tracks TRC-20 USDT transfers to/from 3 known Tether treasury wallets
on TRON. TRON hosts >50% of USDT supply and is where Tether actively operates its mint/burn
cycle. Inter-treasury transfers are excluded to avoid double-counting.

| Column | Type | Description |
|--------|------|-------------|
| `tron_treasury_inflow_count` | float | # TRON transfers into treasury wallets (zero-filled) |
| `tron_treasury_inflow_volume_usd` | float | USD volume of USDT sent to TRON treasury |
| `tron_treasury_outflow_count` | float | # TRON transfers out of treasury wallets |
| `tron_treasury_outflow_volume_usd` | float | USD volume of USDT re-issued from TRON treasury |
| `tron_treasury_net_flow_usd` | float | `outflow − inflow` on TRON |

**Availability:** USDT only.

---

### Unified Institutional Flow Signal

Computed in `clean_data.py` to provide a single consistent flow signal across all coins.

| Column | Type | Description |
|--------|------|-------------|
| `total_net_flow_usd` | float | **USDT**: `treasury_net_flow_usd + tron_treasury_net_flow_usd` (ETH + TRON treasury). **All others**: same as `net_flow_usd` (on-chain mint − burn). |

**Availability:** All coins except UST.

---

### Curve Pool Swaps

Source: Etherscan V2 API (`TokenExchange` events). Each 5m bar counts swaps and volume for
each token in the relevant pool.

**Signal rationale:** Rising `{coin}_net_sell_volume_usd` means sophisticated on-chain traders
are exiting that stablecoin — a leading indicator of peg stress that may precede price movements.

#### Curve 3pool (DAI / USDC / USDT) — available for USDT, USDC, DAI

Pool address: `0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7` — deployed Sep 2020.

| Column | Type | Description |
|--------|------|-------------|
| `curve_3pool_{token}_sold_count` | float | # swaps selling `{token}` in this 5m window |
| `curve_3pool_{token}_sold_volume_usd` | float | USD volume of `{token}` sold |
| `curve_3pool_{token}_bought_count` | float | # swaps buying `{token}` |
| `curve_3pool_{token}_bought_volume_usd` | float | USD volume of `{token}` bought |
| `curve_3pool_{token}_net_sell_volume_usd` | float | `sold − bought` (positive = sell pressure on `{token}`) |

Where `{token}` ∈ `{dai, usdc, usdt}`. Each coin's file includes all three tokens' columns
(since a swap involving one token also involves the others).

#### Curve USDe/USDC pool — available for USDe

Pool address: `0x02950460E2b9529D0E00284A5fA2D7Bdf3Fa4d72` — deployed Nov 2023.

| Column | Type | Description |
|--------|------|-------------|
| `curve_usde_usdc_{token}_sold_count` | float | # swaps selling `{token}` |
| `curve_usde_usdc_{token}_sold_volume_usd` | float | USD volume sold |
| `curve_usde_usdc_{token}_bought_count` | float | # swaps buying `{token}` |
| `curve_usde_usdc_{token}_bought_volume_usd` | float | USD volume bought |
| `curve_usde_usdc_{token}_net_sell_volume_usd` | float | `sold − bought` |

Where `{token}` ∈ `{usde, usdc}`.

#### Curve RLUSD/USDC pool — available for RLUSD

Pool address: `0xd001ae433f254283fece51d4acce8c53263aa186` — deployed Dec 2024.

| Column | Type | Description |
|--------|------|-------------|
| `curve_rlusd_usdc_{token}_sold_count` | float | # swaps selling `{token}` |
| `curve_rlusd_usdc_{token}_sold_volume_usd` | float | USD volume sold |
| `curve_rlusd_usdc_{token}_bought_count` | float | # swaps buying `{token}` |
| `curve_rlusd_usdc_{token}_bought_volume_usd` | float | USD volume bought |
| `curve_rlusd_usdc_{token}_net_sell_volume_usd` | float | `sold − bought` |

Where `{token}` ∈ `{rlusd, usdc}`.

---

### Macro — FRED (daily, forward-filled to 5m)

Source: FRED API. Published at daily or lower frequency; each value is forward-filled into
all 5m bars until the next published value.

| Column | Type | Description |
|--------|------|-------------|
| `dxy` | float | US Dollar Index — broad dollar strength vs basket of currencies |
| `vix` | float | CBOE Volatility Index — equity market fear gauge |
| `t10y` | float | 10-year US Treasury yield (%) |
| `fedfunds` | float | Federal Funds effective rate (%) — monthly release, forward-filled |
| `fear_greed` | int | CNN Fear & Greed Index (0 = extreme fear, 100 = extreme greed); available from ~2018 |

---

### Metadata

| Column | Type | Description |
|--------|------|-------------|
| `date` | object | Calendar date of the bar (`YYYY-MM-DD`) |
| `time` | object | Time of day of the bar (`HH:MM:SS`) |
| `coin` | object | Coin key: `usdt`, `usdc`, `dai`, `busd`, `ust`, `usde`, `rlusd` |
| `peg` | float | Target peg price — `1.0` for all coins in this dataset |
| `coin_type` | object | `fiat-backed`, `crypto-collateralized`, `synthetic`, or `algorithmic` |
| `coin_status` | object | `active`, `discontinued` (BUSD), or `failed` (UST) |

---

### Labels — Depeg Prediction Targets

Computed by `label_data.py`. **Depeg** is defined as 3 or more consecutive 5-minute bars where
`|price_dev| > 0.005` (0.5% from peg). This 15-minute persistence requirement filters
transient tick noise.

| Column | Type | Description |
|--------|------|-------------|
| `price_dev` | float | `coinapi_close − peg` — signed deviation from peg |
| `depeg` | Int8 | `1` if this bar is **currently** part of a depeg episode; `0` otherwise |
| `depeg_next_5min` | Int8 | `1` if a depeg episode begins within the **next 1 bar** (5 min) |
| `depeg_next_30min` | Int8 | `1` if a depeg episode begins within the **next 6 bars** (30 min) |
| `depeg_next_1h` | Int8 | `1` if a depeg episode begins within the **next 12 bars** (1 hour) |
| `depeg_next_4h` | Int8 | `1` if a depeg episode begins within the **next 48 bars** (4 hours) |

`NaN` values appear only at the tail of each coin's time series (insufficient look-ahead window
to compute forward labels).

---

## Data Pipeline

```
src/data/merge_sources.py   →  {coin}_5m_raw.parquet   (pure join, no cleaning)
src/data/clean_data.py      →  {coin}_5m.parquet        (zero-fill, ffill, anomaly patch)
src/data/label_data.py      →  {coin}_5m.parquet        (adds depeg labels in-place)
```

### Cleaning rules (`clean_data.py`)
1. **Zero-fill** event columns — absence of an on-chain event means zero activity, not missing data
2. **Forward-fill** daily series — FRED and Fear & Greed propagate last known value to 5m bars
3. **Forward-fill** 5m market context — BTC/ETH closes have occasional gaps
4. **Null + forward-fill** CoinAPI price anomalies — bars with price outside [0.50, 2.00] are
   feed errors; UST is exempt
5. **Trim head rows** — rows before the first valid price across all key columns are dropped

---

## Raw Data

Raw files are in `data/raw/` organized by source:

| Directory | Contents |
|-----------|----------|
| `raw/binance/` | 5m OHLCV Parquet files per trading pair |
| `raw/coinapi/` | 5m VWAP OHLCV Parquet files per coin |
| `raw/onchain/` | Ethereum and TRON on-chain event files |
| `raw/curve/` | Curve pool TokenExchange events and 5m aggregations |
| `raw/fred/` | Daily FRED macro data |
| `raw/market/` | Daily BTC/ETH prices and Fear & Greed index |
| `raw/orderbook/` | 5m order book snapshots (Kraken) |
