# Data Directory — Stablecoin Depeg Prediction

## Overview

5-minute resolution dataset covering 7 stablecoins from their respective launch dates through
February 28, 2026. Processed files are split into two subfolders: `processed/merged/` (raw joins)
and `processed/cleansed/` (clean + labeled, modeling-ready).

## Files

### Processed (modeling-ready)

| File | Coin | Rows | Cols | Date Range | Notes |
|------|------|-----:|-----:|------------|-------|
| `usdt_5m.parquet` | Tether (USDT) | 897,936 | 74 | 2017-08-17 → 2026-02-28 | ETH + TRON treasury flows |
| `usdc_5m.parquet` | USD Coin (USDC) | 775,506 | 91 | 2018-10-16 → 2026-02-28 | ETH + Solana mint/burn |
| `dai_5m.parquet`  | Dai (DAI) | 828,963 | 53 | 2018-04-13 → 2026-02-28 | |
| `busd_5m.parquet` | Binance USD (BUSD) | 370,848 | 48 | 2019-09-20 → 2023-03-31 | Discontinued Mar 2023 |
| `ust_5m.parquet`  | TerraUSD (UST) | 153,961 | 53 | 2020-11-23 → 2022-05-12 | Failed May 2022; Curve only (no ETH on-chain) |
| `usde_5m.parquet` | Ethena USDe (USDe) | 200,928 | 48 | 2024-04-02 → 2026-02-28 | |
| `rlusd_5m.parquet`| Ripple USD (RLUSD) | 96,192 | 53 | 2025-04-01 → 2026-02-28 | ETH + XRPL mint/burn |

`processed/merged/{coin}_5m_raw.parquet` — all sources joined, no cleaning applied (NaNs intact).
`processed/cleansed/{coin}_5m.parquet` — zero-filled, forward-filled, anomaly-patched, and labeled. Raw columns only — derived features (e.g. `total_net_flow_usd`) are computed in the feature engineering stage.
`processed/features/{coin}_5m_features.parquet` — engineered features per coin, ready for modeling.
`processed/features/pooled_5m.parquet` — all 7 coins stacked (3.1M rows, targets: `depeg_next_1h`, `depeg_next_30min`).

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

### Binance Trading Pairs

Source: Binance public API. Coin-specific cross-pairs provide relative price and liquidity signals.
Column naming: `binance_{pair}_{field}` where field ∈ `{open, high, low, close, volume, quote_volume, trades, taker_buy_volume, taker_buy_quote_volume, buy_ratio, spread_proxy}`.

| Pair | Available for | Notes |
|------|---------------|-------|
| `binance_usdcusdt_*` | USDT, USDC | USDC/USDT relative price — proxy for USDT vs USDC stress; listed mid-2019 (~21% null at head for USDT) |
| `binance_btcusdc_*` | USDC | BTC quoted in USDC — measures USDC demand under crypto stress |
| `binance_ethusdc_*` | USDC | ETH quoted in USDC |

Each pair provides 11 columns: `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trades`,
`taker_buy_volume`, `taker_buy_quote_volume`, `buy_ratio` (`taker_buy_volume / volume`), `spread_proxy` (`(high−low)/close`).

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

### USDT Omni Layer Treasury Flows — USDT only

Source: OmniExplorer API (`collect_omni.py`). Tracks USDT Simple Send transactions to/from
Tether's known Omni treasury address on the Bitcoin blockchain
(`1NTMakcgVwQpMdGxRQnFKyb3G1FAJysSfz`, property ID 31).

**Signal rationale:** The Omni Layer (Bitcoin) was USDT's original issuance platform; institutional
redemption flows still pass through this address. Treasury inflow may precede open-market peg stress.

Raw events: `data/raw/omni/usdt_omni_treasury.parquet`. 5-min aggregates: `data/raw/omni/usdt_omni_5m.parquet`.
Patched into `usdt_5m.parquet` and feature files via `patch_usdt_omni.py`.

| Column | Type | Description |
|--------|------|-------------|
| `omni_treasury_inflow_count` | float | # transfers into Omni treasury (redemption pressure) |
| `omni_treasury_inflow_volume_usd` | float | USD volume of USDT sent to Omni treasury |
| `omni_treasury_outflow_count` | float | # transfers out of Omni treasury (re-issuance) |
| `omni_treasury_outflow_volume_usd` | float | USD volume of USDT re-issued from Omni treasury |
| `omni_treasury_net_flow_usd` | float | `inflow − outflow` (positive = net redemption pressure; note opposite sign convention from ETH/TRON treasury columns) |

**Availability:** USDT only. Added to cleansed and feature files via `patch_usdt_omni.py`.

---

### USDC Solana Mint / Burn — USDC only

Source: Dune Analytics (`tokens_solana.transfers`, query 6794492) for history (Oct 2020 → Nov 2025);
Helius enhanced API for incremental updates. USDC is natively issued by Circle on Solana
(mint: `EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`). 711k+ events collected.

| Column | Type | Description |
|--------|------|-------------|
| `sol_mint_count` | float | Number of USDC mint events on Solana in this 5m window (zero-filled) |
| `sol_mint_volume_usd` | float | USD value of USDC minted on Solana |
| `sol_burn_count` | float | Number of USDC burn events on Solana (zero-filled) |
| `sol_burn_volume_usd` | float | USD value of USDC burned on Solana |
| `sol_net_flow_usd` | float | `mint_volume − burn_volume` on Solana |

**Availability:** USDC only.

---

### RLUSD XRPL Mint / Burn + DEX — RLUSD only

Source: `collect_xrpl.py` (XRPL public JSON-RPC, no key required — primary collector, includes DEX
flows) and `collect_dune_xrpl.py` (Dune Analytics query 6811285 — historical bootstrap, mint/burn
only). RLUSD is issued by Ripple on the XRP Ledger
(currency hex: `524C555344000000000000000000000000000000`, issuer: `rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De`).

| Column | Type | Description |
|--------|------|-------------|
| `xrpl_mint_count` | float | Number of RLUSD mint payments on XRPL in this 5m window (zero-filled) |
| `xrpl_mint_volume_usd` | float | USD value of RLUSD minted on XRPL |
| `xrpl_burn_count` | float | Number of RLUSD burn payments on XRPL (zero-filled) |
| `xrpl_burn_volume_usd` | float | USD value of RLUSD burned on XRPL |
| `xrpl_net_flow_usd` | float | `mint_volume − burn_volume` on XRPL |
| `xrpl_dex_buy_count` | float | Number of RLUSD DEX buy offers (OfferCreate) in this 5m window |
| `xrpl_dex_buy_volume_usd` | float | USD value of RLUSD acquired via XRPL DEX |
| `xrpl_dex_sell_count` | float | Number of RLUSD DEX sell offers |
| `xrpl_dex_sell_volume_usd` | float | USD value of RLUSD sold via XRPL DEX |
| `xrpl_dex_net_volume_usd` | float | `buy_volume − sell_volume` (positive = net buying pressure on DEX) |

**Availability:** RLUSD only. DEX columns only present when collected via `collect_xrpl.py` (not the Dune bootstrap).

---

### Unified Institutional Flow Signal

Computed in `04_feature_engineering.ipynb` (not present in cleansed files — derived during feature engineering).

| Column | Type | Description |
|--------|------|-------------|
| `total_net_flow_usd` | float | **USDT**: `treasury_net_flow_usd + tron_treasury_net_flow_usd + omni_treasury_net_flow_usd` (ETH + TRON + Omni treasury). **All others**: `net_flow_usd` (on-chain mint − burn). Zero for UST (no ETH contract). |

**Availability:** All coins (zeros for UST). Present in feature files, not in cleansed files.

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

#### Curve BUSD/3CRV pool — available for BUSD

Pool address: `0x4807862AA8b2bF68830e4C8dc86D0e9A998e085a` — deployed Sep 2020, inactive after Mar 2023.

| Column | Type | Description |
|--------|------|-------------|
| `curve_busd_3crv_{token}_sold_count` | float | # swaps selling `{token}` |
| `curve_busd_3crv_{token}_sold_volume_usd` | float | USD volume sold |
| `curve_busd_3crv_{token}_bought_count` | float | # swaps buying `{token}` |
| `curve_busd_3crv_{token}_bought_volume_usd` | float | USD volume bought |
| `curve_busd_3crv_{token}_net_sell_volume_usd` | float | `sold − bought` |

Where `{token}` ∈ `{busd, 3crv}`.

#### Curve UST/3CRV pools — available for UST

Two pools tracked to capture UST's May 2022 collapse on-chain:

- **ust_3crv**: `0x890f4e345B1dAED0367A877a1612f86A1f86985f` — deployed Dec 2020 (`{token}` ∈ `{ust, 3crv}`)
- **ust_wormhole_3crv**: `0xCEAF7747579696A2F0bb206a14210e3c9e6fB269` — deployed Oct 2021, Wormhole-bridged UST (`{token}` ∈ `{wust, 3crv}`)

Columns follow the same pattern: `curve_{pool}_{token}_{sold_count, sold_volume_usd, bought_count, bought_volume_usd, net_sell_volume_usd}`.

**Note:** UST (TerraUSD) was Terra-native — no ERC-20 on-chain mint/burn data exists. Curve pool
swap data is the primary on-chain stress signal available for UST.

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

### Circulating Supply — DeFiLlama (daily, forward-filled to 5m)

Source: DeFiLlama stablecoin API (`collect_defillama.py`). Daily `peggedUSD` circulating supply
for all 7 stablecoins. Output: `data/raw/defillama/mcap_daily.parquet`. Forward-filled to 5m
in the merge step. All 7 columns are included in every coin's dataset as cross-coin supply signals.

| Column | Type | Description |
|--------|------|-------------|
| `usdt_mcap` | float | USDT circulating supply (USD, all chains) |
| `usdc_mcap` | float | USDC circulating supply (USD, all chains) |
| `dai_mcap` | float | DAI circulating supply (USD) |
| `busd_mcap` | float | BUSD circulating supply (USD) |
| `ust_mcap` | float | UST circulating supply (USD; collapses to ~zero in May 2022) |
| `usde_mcap` | float | USDe circulating supply (USD) |
| `rlusd_mcap` | float | RLUSD circulating supply (USD) |

---

### Order Book Snapshots — CoinAPI Market Data (5m aggregated)

Source: CoinAPI Market Data API (`collect_orderbook.py`). Requires `COINAPI_MARKETDATA_KEY`
(separate from the Indexes API key). Per-snapshot metrics aggregated to 5-min windows (mean + std).
Output: `data/raw/orderbook/{coin}_orderbook_5m.parquet`.

| Column | Type | Description |
|--------|------|-------------|
| `obi_1pct_mean` | float | Mean order book imbalance within 1% of mid: `(bid_vol − ask_vol) / (bid_vol + ask_vol)`; range [−1, +1], negative = selling pressure |
| `obi_1pct_std` | float | Std dev of OBI (1%) across snapshots in the 5m window |
| `obi_2pct_mean` | float | Mean OBI within 2% of mid price |
| `obi_2pct_std` | float | Std dev of OBI (2%) |
| `spread_bps_mean` | float | Mean best bid/ask spread in basis points |
| `spread_bps_std` | float | Std dev of spread (bps) |
| `bid_slope_mean` | float | Mean bid-side slope: cumulative volume regressed against price distance from mid; steeper = thinner book |
| `bid_slope_std` | float | Std dev of bid slope |
| `ask_slope_mean` | float | Mean ask-side slope |
| `ask_slope_std` | float | Std dev of ask slope |

**Availability:** Coins with `orderbook_symbol` configured in `config/settings.py`.

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

Computed in `02_clean_merged_data.ipynb`. **Depeg** is defined as 3 or more consecutive 5-minute bars where
`|price_dev| > 0.005` (0.5% from peg). This 15-minute persistence requirement filters
transient tick noise.

| Column | Type | Description |
|--------|------|-------------|
| `price_dev` | float | `coinapi_close − peg` — signed deviation from peg |
| `depeg` | Int8 | `1` if this bar is **currently** part of a depeg episode (either direction); `0` otherwise |
| `depeg_down` | Int8 | `1` if this bar is currently part of a **below-peg** depeg episode (`price_dev < −threshold`) |
| `depeg_next_5min` | Int8 | `1` if a depeg episode begins within the **next 1 bar** (5 min) |
| `depeg_next_5min_down` | Int8 | Same, scoped to below-peg events only |
| `depeg_next_30min` | Int8 | `1` if a depeg episode begins within the **next 6 bars** (30 min) |
| `depeg_next_30min_down` | Int8 | Same, scoped to below-peg events only |
| `depeg_next_1h` | Int8 | `1` if a depeg episode begins within the **next 12 bars** (1 hour) |
| `depeg_next_1h_down` | Int8 | Same, scoped to below-peg events only |
| `depeg_next_4h` | Int8 | `1` if a depeg episode begins within the **next 48 bars** (4 hours) |
| `depeg_next_4h_down` | Int8 | Same, scoped to below-peg events only |
| `depeg_next_12h` | Int8 | `1` if a depeg episode begins within the **next 144 bars** (12 hours) |
| `depeg_next_12h_down` | Int8 | Same, scoped to below-peg events only |
| `depeg_next_24h` | Int8 | `1` if a depeg episode begins within the **next 288 bars** (24 hours) |
| `depeg_next_24h_down` | Int8 | Same, scoped to below-peg events only |

14 label columns total: `depeg`, `depeg_down`, and 6 horizons × 2 directions (all-depeg + downside-only).

`NaN` values appear only at the tail of each coin's time series (insufficient look-ahead window
to compute forward labels).

---

## Data Pipeline

```
src/data_collection_scripts/collect_*.py  →  data/raw/{source}/                          (raw + 5m aggregates per source)
notebooks/01_merge_raw_data.ipynb         →  processed/merged/{coin}_5m_raw.parquet      (pure join, no cleaning)
notebooks/02_clean_merged_data.ipynb      →  processed/cleansed/{coin}_5m.parquet        (zero-fill, ffill, anomaly patch + depeg labels)
notebooks/03_eda_all_coins.ipynb          →  processed/cleansed/{coin}_5m_clean.parquet     (adds severity labels)
notebooks/04_feature_engineering.ipynb   →  processed/features/pooled_5m.parquet           (all 7 coins stacked, 3.1M rows)
notebooks/05_modeling.ipynb              →  data/models/ + processed/predictions/           (CatBoost models + test predictions)
```

### Stage 1 — Collection (`src/data_collection_scripts/collect_*.py`)

Each collector fetches from its API, applies only unavoidable transformations, and writes to
`data/raw/`. No statistical preprocessing, outlier removal, or filling at this stage.

**Transformations applied in all collectors:**
- **Unit conversion** — raw token amounts divided by decimals (÷10⁶ for USDT/USDC, ÷10¹⁸ for DAI/USDe/RLUSD/BUSD)
- **Timestamp normalisation** — source-specific epochs and formats converted to UTC datetime
- **Deduplication** — on `tx_hash` (+ `event_type` where one tx can have both mint and burn)
- **5-min aggregation** — event-level data (individual mint/burn/swap) binned to 5-min bars with count + sum; raw event files are saved alongside the aggregates

| Script | API / Source | Raw output | 5m output |
|--------|-------------|------------|-----------|
| `collect_binance.py` | Binance public REST | — | `raw/binance/{pair}_5m.parquet` |
| `collect_coinapi.py` | CoinAPI VWAP (paid) | — | `raw/coinapi/{coin}_5m.parquet` |
| `collect_onchain.py` | Etherscan V2 API | `raw/onchain/{coin}_eth_events.parquet` | `raw/onchain/{coin}_eth_5m.parquet` |
| `collect_tron.py` | TronGrid API | `raw/onchain/usdt_tron_events.parquet` | `raw/onchain/usdt_tron_5m.parquet` |
| `collect_omni.py` | OmniExplorer API | `raw/omni/usdt_omni_treasury.parquet` | `raw/omni/usdt_omni_5m.parquet` |
| `collect_curve.py` | Etherscan V2 API | `raw/curve/{pool}_events.parquet` | `raw/curve/{pool}_5m.parquet` |
| `collect_xrpl.py` | XRPL public JSON-RPC | `raw/onchain/rlusd_xrpl_events.parquet` | `raw/onchain/rlusd_xrpl_5m.parquet` |
| `collect_dune_xrpl.py` | Dune Analytics (XRPL historical) | same as above | same as above |
| `collect_dune.py` | Dune Analytics (Solana) | `raw/onchain/usdc_sol_events_dune.parquet` | `raw/onchain/usdc_sol_5m.parquet` |
| `collect_solana.py` | Helius API (Solana) | `raw/onchain/usdc_sol_events.parquet` | merged into `usdc_sol_5m.parquet` |
| `collect_orderbook.py` | CoinAPI Market Data API | — | `raw/orderbook/{coin}_orderbook_5m.parquet` |
| `collect_defillama.py` | DeFiLlama stablecoin API | `raw/defillama/mcap_daily.parquet` | — (daily, forward-filled in merge) |
| `collect_fred.py` | FRED API | `raw/fred/macro.parquet` | — (daily, forward-filled in merge) |
| `collect_market.py` | Alternative.me (Fear & Greed) | `raw/market/market_daily.parquet` | — (daily, forward-filled in merge) |

**Collector-specific notes:**

- **Etherscan** (`collect_onchain.py`, `collect_curve.py`): paginates in 100k-block chunks with
  adaptive bisection when the 1,000-result-per-page limit is hit. Checkpoints after each chunk.
- **XRPL RPC** (`collect_xrpl.py`): paginates the RLUSD issuer's `account_tx` in 20k-ledger chunks
  via XRPL public JSON-RPC (no key required). Collects mint, burn, and DEX trades (OfferCreate).
  `collect_dune_xrpl.py` provides a Dune Analytics historical bootstrap (mint/burn only, query 6811285,
  60-day chunks); both scripts write to the same output files.
- **Dune Solana** (`collect_dune.py`): executes in 3-week chunks (stay under Dune's 32k-row limit).
  Mints/burns from `tokens_solana.transfers` with `action IN ('mint','burn')`. Checkpoints after each chunk.
- **Helius** (`collect_solana.py`): incremental USDC Solana updates via enhanced transaction API.
  Deduped and merged with Dune output on `tx_hash + event_type`. Only indexes back to ~Aug 2024.
- **TronGrid** (`collect_tron.py`): inter-treasury transfers between known Tether wallets are
  excluded to avoid double-counting.
- **patch_usdt_omni.py**: post-processing utility (not a collector). After running `collect_omni.py`
  and the notebook pipeline, run this script to patch Omni treasury columns into `usdt_5m.parquet`,
  recompute affected features in `usdt_5m_features.parquet`, and rebuild `pooled_5m.parquet`.

---

### Stage 2 — Merge (`01_merge_raw_data.ipynb`)

Builds a continuous 5-min UTC index per coin from the coin's launch date to `GLOBAL_END_DATE`
(2026-02-28 23:55 UTC). All 5-min sources are joined directly onto this index. Daily sources
(FRED, Fear & Greed, BTC/ETH) are forward-filled into 5-min bars. Output is a single wide
Parquet with all columns present but no filling or cleaning applied — NaN means the source had
no data for that bar.

---

### Stage 3 — Cleaning & Labelling (`02_clean_merged_data.ipynb`)

1. **Zero-fill** event columns — absence of an on-chain event means zero activity, not missing data.
   Prefixes zero-filled: `mint_`, `burn_`, `net_flow_usd`, `treasury_*`, `tron_treasury_*`,
   `omni_treasury_*`, `curve_*`, `sol_*` (USDC Solana), `xrpl_*` (RLUSD XRPL)
2. **Forward-fill** daily series — FRED and Fear & Greed propagate last known value to 5m bars
3. **Forward-fill** 5m market context — BTC/ETH closes have occasional gaps
4. **Null + forward-fill** CoinAPI price anomalies — bars with price outside [0.50, 2.00] are
   feed errors; UST is exempt since its collapse legitimately breached those bounds
5. **Trim head rows** — rows before the first valid price across all key columns are dropped
6. **Depeg labels** — adds `depeg`, `depeg_down`, and 6 forward-looking horizons (5min, 30min, 1h, 4h, 12h, 24h) each in both all-depeg and downside-only variants (12 label columns total)

---

## Raw Data

Raw files are in `data/raw/` organized by source:

| Directory | Contents |
|-----------|----------|
| `raw/binance/` | 5m OHLCV Parquet files per trading pair |
| `raw/coinapi/` | 5m VWAP OHLCV Parquet files per coin |
| `raw/onchain/` | Ethereum mint/burn events + USDT treasury flows (ETH + TRON) + XRPL RLUSD events + Solana USDC events |
| `raw/curve/` | Curve pool TokenExchange events and 5m aggregations |
| `raw/orderbook/` | CoinAPI Market Data order book snapshots aggregated to 5m per coin |
| `raw/defillama/` | Daily stablecoin circulating supply |
| `raw/omni/` | USDT Omni Layer (Bitcoin) treasury flows |
| `raw/fred/` | Daily FRED macro data |
| `raw/market/` | Daily BTC/ETH prices and Fear & Greed index |
