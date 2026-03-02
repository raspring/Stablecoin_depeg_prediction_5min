# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

CMU MSBA capstone project — **stablecoin depeg prediction at 5-minute resolution**.

Stablecoins: USDT, USDC, RLUSD, DAI, UST (failed 2022), USDe, BUSD (discontinued 2023).
Data goes back to 2015-01-01 (USDT); other coins use their own launch dates.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Collect data
python src/data/collect_all.py usdt
python src/data/collect_all.py all
python src/data/collect_all.py all --no-coinapi   # skip until key is ready
python src/data/collect_all.py all --no-daily     # skip FRED/market/defillama

# Collect individual sources
python src/data/collect_binance.py [coin|all]     # free, 5m OHLCV
python src/data/collect_coinapi.py [coin|all]     # paid, fiat pair OHLCV
python src/data/collect_fred.py                   # daily macro (FRED_API_KEY)
python src/data/collect_market.py                 # daily BTC/ETH/Fear&Greed
python src/data/collect_onchain.py [coin|all]     # Ethereum mint/burn + USDT treasury flows (ETHERSCAN_API_KEY)
python src/data/collect_tron.py                   # USDT TRON treasury flows (TRONGRID_API_KEY optional)
python src/data/collect_curve.py [pool|all]       # Curve pool swap events (ETHERSCAN_API_KEY)

# Merge to 5m Parquet
python src/data/merge_sources.py [coin|all]

# Run tests
pytest tests/
```

## Architecture

### Native 5-minute sources
- **BinanceCollector** (`collect_binance.py`) — free public API, BTCUSDT/ETHUSDT etc., full history from Aug 2017
- **CoinAPICollector** (`collect_coinapi.py`) — paid, fiat pairs (USDTUSD, USDCUSD, DAIUSD), full history
- **OnchainCollector** (`collect_onchain.py`) — Ethereum mint/burn events + USDT ETH treasury flows via Etherscan V2 API
- **TronCollector** (`collect_tron.py`) — USDT treasury inflow/outflow on TRON via TronGrid API
- **CurveCollector** (`collect_curve.py`) — TokenExchange events on Curve 3pool, USDe/USDC, and RLUSD/USDC pools via Etherscan V2 API

### Daily sources (forward-filled to 5m in merge)
- **FREDCollector** (`collect_fred.py`) — DXY, VIX, T10Y, Fed Funds
- **MarketCollector** (`collect_market.py`) — BTC/ETH prices, Fear & Greed

### Merge pipeline
`merge_sources.py` builds a 5m UTC index per coin, joins 5m sources directly, and
forward-fills daily sources. Output: `data/processed/{coin}_5m.parquet`.

## Storage

All data stored as **Parquet** (not CSV). Raw files in `data/raw/{source}/`.

## API Keys

Set in `.env` file:
- `COINAPI_KEY` — required for fiat pair data (coinapi.io)
- `FRED_API_KEY` — required for macro data
- `ETHERSCAN_API_KEY` — required for on-chain mint/burn events (Etherscan V2 API)
- `TRONGRID_API_KEY` — optional for TRON treasury flows; free tier allows ~10 QPS without key
- `COINGECKO_API_KEY` — optional (higher rate limits)

## Key Design Decisions

- **5m is the base frequency** — daily sources are forward-filled up, not 5m aggregated down
- **Per-coin start dates** — coins have different launch dates; see `config/settings.py`
- **Failed/discontinued coins included** — UST (failed May 2022) and BUSD (discontinued Mar 2023)
  provide real depeg examples; their `end_date` is set in config
- **Parquet throughout** — 1.6M+ rows per coin makes CSV impractical
- **Binance is free for 5m** — only CoinAPI is paid (needed for direct USD fiat pairs)
- **BTC/ETH from Binance, not CoinAPI** — for market indicator features, Binance is free and
  sufficiently accurate; CoinAPI VWAP precision only matters for stablecoin peg measurement

## On-chain Mint/Burn Mechanics

### USDT (Tether)
- **Minting**: Institutional clients wire USD to Tether off-chain; Tether calls `issue()` in
  large infrequent batches (~230 events total on Ethereum over 7 years, some up to $1B+)
- **Redemption**: Entirely off-chain — institutions send USDT to Tether's treasury wallet,
  receive a USD wire, and the tokens sit in treasury to be re-issued to the next customer.
  The `redeem()` function exists in the contract but Tether never calls it on Ethereum.
  **No Redeem events exist on-chain.**
- **Burns**: Only via `DestroyedBlackFunds` — tokens seized from OFAC-sanctioned wallets
  (1,175 events, more than mints). This is the only true on-chain supply destruction.
- **Treasury tracking (ETH)**: Institutions return USDT to `0x5754284f...007b949` (Tether Treasury
  on Ethereum) for off-chain USD redemption. These Transfer events are now collected as
  `treasury_inflow` (redemption pressure) and `treasury_outflow` (re-issuance) in the ETH collector.
- **Treasury tracking (TRON)**: TRON hosts >50% of USDT supply and is where Tether actively
  operates the mint/burn cycle. Three known treasury wallets are tracked via TronGrid API
  (`collect_tron.py`). Inter-treasury transfers are excluded to avoid double-counting.
- **Leading indicator hypothesis**: Sophisticated institutions redeem at par directly with Tether
  *before* the market depeg; treasury inflow spikes on both chains may precede open-market
  peg breaks by minutes to hours.

## Curve Pool Swap Mechanics

`collect_curve.py` tracks `TokenExchange` events on three pools:

| Pool         | Address                                      | Tokens                | Deployed |
|--------------|----------------------------------------------|-----------------------|----------|
| 3pool        | `0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7` | DAI / USDC / USDT     | Sep 2020 |
| usde_usdc    | `0x02950460E2b9529D0E00284A5fA2D7Bdf3Fa4d72` | USDe / USDC           | Nov 2023 |
| rlusd_usdc   | `0xd001ae433f254283fece51d4acce8c53263aa186` | RLUSD / USDC          | Dec 2024 |

**Signal rationale**: When `usdt_net_sell_volume_usd > 0` in the 3pool (USDT is being sold for
DAI or USDC), sophisticated traders are exiting USDT on-chain — a leading indicator of depeg
stress that may precede open-market price movements.

**topic0** (keccak256 of `TokenExchange(address,int128,uint256,int128,uint256)`):
`0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140`
(All Curve pool types — classic and NG — emit the same signature; verified on-chain.)

**Output per pool**: `data/raw/curve/{pool}_5m.parquet` with columns
`{token}_sold_volume_usd`, `{token}_bought_volume_usd`, `{token}_net_sell_volume_usd` for each token.

### USDC (Circle)
- **Minting**: Circle calls `Mint()` on-chain for every issuance — high frequency, many events
- **Redemption**: Circle calls `Burn()` on-chain for every redemption — clean net flow signal
- **Net flow**: `mint_volume - burn_volume` is a reliable indicator of institutional demand/stress
- **Implication**: USDC provides a much cleaner on-chain supply signal than USDT
