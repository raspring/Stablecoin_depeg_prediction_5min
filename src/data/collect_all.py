"""
Orchestrator: run all collectors for one or all stablecoins.

Usage:
    python src/data/collect_all.py usdt
    python src/data/collect_all.py all
    python src/data/collect_all.py usdt --start 2020-01-01
    python src/data/collect_all.py all --no-coinapi   # skip CoinAPI if key not ready
    python src/data/collect_all.py all --no-daily     # skip FRED/market
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import STABLECOINS

import src.data.collect_binance as binance
import src.data.collect_coinapi as coinapi
import src.data.collect_fred as fred
import src.data.collect_market as market


def run(
    coin_keys: list[str],
    start_date: datetime = None,
    end_date: datetime = None,
    skip_coinapi: bool = False,
    skip_daily: bool = False,
) -> None:

    # --- Per-coin 5m sources ---
    for coin in coin_keys:
        cfg = STABLECOINS[coin]
        coin_start = start_date or datetime.fromisoformat(cfg["start_date"]).replace(tzinfo=timezone.utc)

        print(f"\n{'='*50}")
        print(f"  {cfg['name']} ({coin.upper()})")
        print(f"{'='*50}")

        # Binance (free, always run)
        print("\n[Binance]")
        data = binance.collect_coin(coin, start_date=coin_start, end_date=end_date)
        binance.save(data, coin)

        # CoinAPI (fiat pairs, requires key)
        if not skip_coinapi:
            print("\n[CoinAPI]")
            try:
                data = coinapi.collect_coin(coin, start_date=coin_start, end_date=end_date)
                coinapi.save(data, coin)
            except EnvironmentError as e:
                print(f"  Skipping CoinAPI: {e}")

    # --- Daily sources (market-wide, collected once) ---
    if not skip_daily:
        print("\n[FRED — macro]")
        try:
            df = fred.collect_all()
            fred.save(df)
        except EnvironmentError as e:
            print(f"  Skipping FRED: {e}")

        print("\n[Market — BTC/ETH/Fear&Greed]")
        df = market.collect_all()
        market.save(df)

    print("\n\nDone. Run merge_sources.py to build 5m Parquet files.")


def main():
    parser = argparse.ArgumentParser(description="Collect stablecoin data")
    parser.add_argument("coin", help="Coin key (usdt, usdc, ...) or 'all'")
    parser.add_argument("--start", default=None, help="Override start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="Override end date YYYY-MM-DD")
    parser.add_argument("--no-coinapi", action="store_true", help="Skip CoinAPI collection")
    parser.add_argument("--no-daily", action="store_true", help="Skip FRED/market")
    args = parser.parse_args()

    coin_keys = list(STABLECOINS.keys()) if args.coin == "all" else [args.coin]

    invalid = [c for c in coin_keys if c not in STABLECOINS]
    if invalid:
        print(f"Unknown coins: {invalid}. Valid: {list(STABLECOINS.keys())}")
        sys.exit(1)

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc) if args.start else None
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc) if args.end else None

    run(coin_keys, start_date=start, end_date=end,
        skip_coinapi=args.no_coinapi, skip_daily=args.no_daily)


if __name__ == "__main__":
    main()
