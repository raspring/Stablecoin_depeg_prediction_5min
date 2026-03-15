# Feature Engineering

Scripts that transform cleansed 5-minute stablecoin data into model-ready features for depeg prediction.

## Scripts

| Script | Input | Output |
|--------|-------|--------|
| `feature_engineering.py [coin\|all]` | `data/processed/cleansed/{coin}_5m.parquet` | `data/processed/features/{coin}_5m_features.parquet` |
| `build_pooled_dataset.py` | Per-coin feature files above | `data/processed/features/pooled_5m.parquet` |

## Pooled Dataset

`pooled_5m.parquet` stacks all 7 coins into a single modeling-ready file using the 76 columns common across every coin. Rows where `depeg_next_1h` is NaN (13 trailing rows per coin) are dropped.

- **3,324,243 rows × 77 columns** (76 features + `coin_key`)
- **Target**: `depeg_next_1h` — will a depeg episode begin within the next 1 hour (12 bars)?
- **Class balance**: 9.68% positive (321,753 depeg rows)

| Coin | Rows | depeg_next_1h=1 | Rate |
|------|-----:|----------------:|-----:|
| USDT | 897,923 | 89,107 | 9.92% |
| USDC | 775,493 | 45,974 | 5.93% |
| DAI | 828,950 | 175,521 | 21.17% |
| BUSD | 370,835 | 1,414 | 0.38% |
| UST | 153,948 | 9,530 | 6.19% |
| USDe | 200,915 | 207 | 0.10% |
| RLUSD | 96,179 | 0 | 0.00% |

RLUSD has no historical depeg events — it learns entirely from other coins via the pooled model.

## Feature Reference (68 model inputs)

### Raw Price (5)
Directly from CoinAPI VWAP data.

| Column | Description |
|--------|-------------|
| `coinapi_open` | 5-min open price |
| `coinapi_high` | 5-min high price |
| `coinapi_low` | 5-min low price |
| `coinapi_close` | 5-min close price |
| `coinapi_tick_count` | Number of ticks in the bar |

### Market Context (7)
Macro and crypto market indicators, forward-filled from daily sources.

| Column | Description |
|--------|-------------|
| `binance_btc_close` | BTC/USDT close price |
| `binance_eth_close` | ETH/USDT close price |
| `dxy` | US Dollar Index (FRED) |
| `vix` | CBOE Volatility Index (FRED) |
| `t10y` | 10-Year Treasury yield (FRED) |
| `fedfunds` | Fed Funds effective rate (FRED) |
| `fear_greed` | Crypto Fear & Greed Index (0–100) |

### Price Deviation — Raw & Rolling Stats (10)
`price_dev` = `(coinapi_close - 1.0)` — signed deviation from the $1.00 peg.

| Column | Description |
|--------|-------------|
| `price_dev` | Raw peg deviation |
| `price_dev_mean_{15min,1h,4h}` | Rolling mean over 3/12/48 bars |
| `price_dev_std_{15min,1h,4h}` | Rolling std over 3/12/48 bars |
| `price_dev_absmax_{15min,1h,4h}` | Rolling max of abs(price_dev) over 3/12/48 bars |

### Price Momentum (7)

| Column | Description |
|--------|-------------|
| `price_dev_diff1` | 1-bar change in price_dev |
| `bars_above_01pct_{15min,1h,4h}` | Count of bars where \|price_dev\| > 0.1% in window |
| `bars_above_03pct_{15min,1h,4h}` | Count of bars where \|price_dev\| > 0.3% in window |

### Intrabar Volatility (1)

| Column | Description |
|--------|-------------|
| `intrabar_range` | `(high - low) / close` — bar-level price range |

### On-Chain Flows (9)
Coin-specific raw signals are aggregated into common column names in `feature_engineering.py`. Each coin's `net_flow`, `mint`, and `burn` incorporate all relevant chains and treasury sources.

| Coin | `net_flow` sources | `mint`/`burn` sources |
|------|-------------------|----------------------|
| USDT | ETH net_flow + ETH treasury + TRON treasury | ETH mint/burn |
| USDC | ETH net_flow + Solana net_flow | ETH mint/burn |
| DAI | ETH net_flow | ETH mint/burn |
| BUSD | ETH net_flow | ETH mint/burn |
| UST | — (no ETH contract) | — |
| USDe | ETH net_flow | ETH mint/burn |
| RLUSD | ETH net_flow + XRPL net_flow | ETH mint/burn |

| Column | Description |
|--------|-------------|
| `net_flow_sum_{1h,4h,1d}` | Rolling sum of total_net_flow_usd over 12/48/288 bars |
| `net_flow_zscore_30d` | Z-score of net_flow vs trailing 30-day window |
| `mint_sum_{1h,4h}` | Rolling sum of mint_volume_usd over 12/48 bars |
| `burn_sum_{1h,4h}` | Rolling sum of burn_volume_usd over 12/48 bars |
| `mint_burn_ratio_1h` | mint_sum_1h / (burn_sum_1h + 1) |

### Curve DEX Pressure (5)
Measures institutional selling pressure on-chain. Each coin maps to its primary Curve pool leg.

| Coin | Curve pool | Signal column |
|------|-----------|---------------|
| USDT | Curve 3pool | `curve_3pool_usdt_net_sell_volume_usd` |
| USDC | Curve 3pool | `curve_3pool_usdc_net_sell_volume_usd` |
| DAI | Curve 3pool | `curve_3pool_dai_net_sell_volume_usd` |
| BUSD | busd_3crv | `curve_busd_3crv_busd_net_sell_volume_usd` |
| UST | ust_3crv + ust_wormhole_3crv | summed across both pools |
| USDe | usde_usdc | `curve_usde_usdc_usde_net_sell_volume_usd` |
| RLUSD | rlusd_usdc | `curve_rlusd_usdc_rlusd_net_sell_volume_usd` |

| Column | Description |
|--------|-------------|
| `curve_net_sell_sum_{15min,1h,4h}` | Rolling sum of coin's net sell volume over 3/12/48 bars |
| `curve_net_sell_zscore_30d` | Z-score of net sell vs trailing 30-day window |
| `curve_sell_buy_ratio_1h` | sold_sum_1h / (bought_sum_1h + 1) |

### Market Returns & Volatility (8)

| Column | Description |
|--------|-------------|
| `btc_return_{1h,4h}` | BTC price pct change over 12/48 bars |
| `eth_return_{1h,4h}` | ETH price pct change over 12/48 bars |
| `btc_vol_{4h,1d}` | Rolling std of BTC 1-bar returns over 48/288 bars |
| `vix_diff_1d` | VIX change vs 288 bars ago (daily momentum) |
| `fear_greed_diff_1d` | Fear & Greed change vs 288 bars ago |

### Temporal (4)

| Column | Description |
|--------|-------------|
| `hour_of_day` | UTC hour (0–23) |
| `day_of_week` | Day of week (0=Mon, 6=Sun) |
| `is_weekend` | 1 if Saturday or Sunday |
| `is_us_market_hours` | 1 if Mon–Fri 13:30–20:00 UTC (NYSE hours) |

### Lag Features (12)
Autoregressive features using `shift(n).fillna(0.0)`.

| Column | Description |
|--------|-------------|
| `lag{1,3,6,12}_price_dev` | price_dev lagged 1/3/6/12 bars (5/15/30/60 min) |
| `lag{1,3,6,12}_net_flow_usd` | total_net_flow_usd lagged 1/3/6/12 bars |
| `lag{1,3,6,12}_curve_net_sell` | Curve net sell volume lagged 1/3/6/12 bars |

## Metadata & Labels (not model inputs)

| Column | Description |
|--------|-------------|
| `coin_key` | Coin identifier — used as categorical model input |
| `date`, `time` | Calendar date and time of bar |
| `coin`, `peg`, `coin_type`, `coin_status` | Coin metadata |
| `depeg` | Current depeg state (autoregressive feature — include in model) |
| `depeg_next_1h` | **Target label** |
