# src/

Source code for the stablecoin depeg prediction project.

## Structure

| Folder | Purpose |
|--------|---------|
| `data_collection_scripts/` | Scripts to collect raw data from all external sources (Binance, CoinAPI, Etherscan, FRED, etc.) |

## Data Collection

All raw data is written to `data/raw/{source}/` as Parquet files. Run scripts individually or via the orchestrator:

```bash
# Run all collectors for a single coin
python src/data_collection_scripts/collect_all.py usdt

# Run all collectors for all coins
python src/data_collection_scripts/collect_all.py all

# Common flags
python src/data_collection_scripts/collect_all.py all --no-coinapi      # skip CoinAPI (paid)
python src/data_collection_scripts/collect_all.py all --no-orderbook    # skip order book (paid)
python src/data_collection_scripts/collect_all.py all --no-daily        # skip FRED/market
```

See `src/data_collection_scripts/README.md` for the full list of collectors and API key requirements.

## Post-Collection Processing

All feature engineering, EDA, and modeling live in `notebooks/`. The `src/` folder is purely for data ingestion — no model training or feature logic belongs here.
