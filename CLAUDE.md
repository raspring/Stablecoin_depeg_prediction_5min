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
python src/data/collect_defillama.py [coin|all]   # daily supply
python src/data/collect_fred.py                   # daily macro (FRED_API_KEY)
python src/data/collect_market.py                 # daily BTC/ETH/Fear&Greed

# Merge to 5m Parquet
python src/data/merge_sources.py [coin|all]

# Run tests
pytest tests/
```

## Architecture

### Native 5-minute sources
- **BinanceCollector** (`collect_binance.py`) — free public API, BTCUSDT etc., full history from 2017
- **CoinAPICollector** (`collect_coinapi.py`) — paid, fiat pairs (USDTUSD, USDCUSD, DAIUSD), full history

### Daily sources (forward-filled to 5m in merge)
- **DefiLlamaCollector** (`collect_defillama.py`) — circulating supply
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
- `COINGECKO_API_KEY` — optional (higher rate limits)

## Key Design Decisions

- **5m is the base frequency** — daily sources are forward-filled up, not 5m aggregated down
- **Per-coin start dates** — coins have different launch dates; see `config/settings.py`
- **Failed/discontinued coins included** — UST (failed May 2022) and BUSD (discontinued Mar 2023)
  provide real depeg examples; their `end_date` is set in config
- **Parquet throughout** — 1.6M+ rows per coin makes CSV impractical
- **Binance is free for 5m** — only CoinAPI is paid (needed for direct USD fiat pairs)
