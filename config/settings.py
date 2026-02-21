from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# Native resolution
INTERVAL = "5m"
INTERVAL_MINUTES = 5

# Depeg threshold
DEPEG_THRESHOLD = 0.005  # 0.5% from peg

# Global start date (per-coin dates override this)
GLOBAL_START_DATE = "2015-01-01"

# Stablecoin configuration
STABLECOINS = {
    "usdt": {
        "name": "Tether",
        "peg": 1.0,
        "type": "fiat-backed",
        "status": "active",
        "start_date": "2015-01-01",   # USDT launched Oct 2014
        "end_date": None,
        "coingecko_id": "tether",
        "ethereum_contract": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "binance_pairs": ["BTCUSDT", "ETHUSDT", "USDCUSDT"],
        "coinapi_symbols": ["IDX_REFRATE_VWAP_USDT"],
        "orderbook_symbol": "KRAKEN_SPOT_USDT_USD",   # Most liquid direct fiat pair
    },
    "usdc": {
        "name": "USD Coin",
        "peg": 1.0,
        "type": "fiat-backed",
        "status": "active",
        "start_date": "2018-09-01",   # USDC launched Sep 2018
        "end_date": None,
        "coingecko_id": "usd-coin",
        "ethereum_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "binance_pairs": ["BTCUSDC", "ETHUSDC", "USDCUSDT"],
        "coinapi_symbols": ["IDX_REFRATE_VWAP_USDC"],
        "orderbook_symbol": "COINBASE_SPOT_USDC_USD",  # Coinbase issues USDC — deepest book
    },
    "dai": {
        "name": "Dai",
        "peg": 1.0,
        "type": "crypto-collateralized",
        "status": "active",
        "start_date": "2017-12-01",   # DAI launched Dec 2017
        "end_date": None,
        "coingecko_id": "dai",
        "ethereum_contract": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "binance_pairs": ["DAIUSDT"],
        "coinapi_symbols": ["IDX_REFRATE_VWAP_DAI"],
        "orderbook_symbol": "KRAKEN_SPOT_DAI_USD",
    },
    "busd": {
        "name": "Binance USD",
        "peg": 1.0,
        "type": "fiat-backed",
        "status": "discontinued",     # Paxos stopped minting Mar 2023
        "start_date": "2019-09-01",   # BUSD launched Sep 2019
        "end_date": "2023-03-31",
        "coingecko_id": "binance-usd",
        "ethereum_contract": "0x4Fabb145d64652a948d72533023f6E7A623C7C53",
        "binance_pairs": ["BTCBUSD", "ETHBUSD"],
        "coinapi_symbols": ["IDX_REFRATE_VWAP_BUSD"],
        "orderbook_symbol": "BINANCE_SPOT_BUSD_USDT",  # Binance was primary venue
    },
    "ust": {
        "name": "TerraUSD",
        "peg": 1.0,
        "type": "algorithmic",
        "status": "failed",           # Collapsed May 2022
        "start_date": "2020-09-01",   # UST launched Sep 2020
        "end_date": "2022-05-31",
        "coingecko_id": "terrausd",
        "ethereum_contract": None,    # Terra-native; bridged via Wormhole but not native ERC-20
        "binance_pairs": ["USTUSDT", "USTBUSD"],
        "coinapi_symbols": ["IDX_REFRATE_VWAP_UST"],
        "orderbook_symbol": "BINANCE_SPOT_UST_USDT",   # Binance was primary venue
    },
    "usde": {
        "name": "Ethena USDe",
        "peg": 1.0,
        "type": "synthetic",
        "status": "active",
        "start_date": "2024-02-01",   # USDe launched Feb 2024
        "end_date": None,
        "coingecko_id": "ethena-usde",
        "ethereum_contract": "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
        "binance_pairs": ["USDEUSDT"],
        "coinapi_symbols": ["IDX_REFRATE_VWAP_USDE"],
        "orderbook_symbol": "BINANCE_SPOT_USDE_USDT",
    },
    "rlusd": {
        "name": "Ripple USD",
        "peg": 1.0,
        "type": "fiat-backed",
        "status": "active",
        "start_date": "2024-12-01",   # RLUSD launched Dec 2024
        "end_date": None,
        "coingecko_id": "ripple-usd",
        "ethereum_contract": "0x8292Bb45bf1Ee4d14D77B5Ea4e2C33f63b0f33b7",  # Verify
        "binance_pairs": [],          # Limited exchange listings as of early 2025
        "coinapi_symbols": ["IDX_REFRATE_VWAP_RLUSD"],
        "orderbook_symbol": None,     # Verify once listings mature
    },
}
