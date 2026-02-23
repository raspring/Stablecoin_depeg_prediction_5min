"""
Verify order book symbol availability and snapshot frequency.

Run this after getting COINAPI_MARKETDATA_KEY to confirm:
  - Each symbol exists and returns data
  - Snapshot frequency (how many snapshots per hour)
  - How far back history goes

Usage:
    python scripts/explore_orderbook.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests

API_KEY = os.getenv("COINAPI_MARKETDATA_KEY")
if not API_KEY:
    print("Error: COINAPI_MARKETDATA_KEY not set.")
    sys.exit(1)

BASE_URL = "https://rest.coinapi.io/v1"
HEADERS = {"X-CoinAPI-Key": API_KEY}

from config.settings import STABLECOINS


def probe(symbol_id: str, time_start: str, time_end: str, limit: int = 100, timeout: int = 60):
    url = f"{BASE_URL}/orderbooks/{symbol_id}/history"
    try:
        r = requests.get(url, headers=HEADERS, params={
            "time_start": time_start,
            "time_end": time_end,
            "limit": limit,
        }, timeout=timeout)
        return r.status_code, r.json() if r.status_code == 200 else r.text[:300]
    except requests.exceptions.Timeout:
        return "TIMEOUT", []
    except Exception as e:
        return "ERROR", str(e)


for coin, cfg in STABLECOINS.items():
    symbol = cfg.get("orderbook_symbol")
    if not symbol:
        print(f"\n{coin.upper()}: no symbol configured, skipping")
        continue

    print(f"\n{'='*55}")
    print(f"  {coin.upper()} — {symbol}")
    print(f"{'='*55}")

    # Recent 1-hour window — check frequency and structure
    status, data = probe(symbol, "2024-01-01T00:00:00", "2024-01-01T01:00:00", limit=100)
    print(f"  2024 sample (1hr): HTTP {status}")
    if status == 200 and data:
        print(f"  Snapshots in 1hr: {len(data)}  (~{len(data)} per hour, 1 per {60//max(len(data),1)} min)")
        snap = data[0]
        bids = snap.get("bids", [])
        asks = snap.get("asks", [])
        print(f"  Bid levels: {len(bids)}, Ask levels: {len(asks)}")
        if bids and asks:
            print(f"  Best bid: {bids[0]['price']:.6f}, Best ask: {asks[0]['price']:.6f}")
            mid = (bids[0]['price'] + asks[0]['price']) / 2
            spread_bps = ((asks[0]['price'] - bids[0]['price']) / mid) * 10_000
            print(f"  Spread: {spread_bps:.2f} bps")
        print(f"  First ts: {snap.get('time_exchange', '?')}")

        # Check 5m coverage: how many 5m windows have at least 1 snapshot?
        import pandas as pd
        timestamps = pd.to_datetime([s["time_exchange"] for s in data])
        windows = timestamps.floor("5min").nunique()
        print(f"  5m windows covered (of 12 possible): {windows}/12")
    elif status != 200:
        print(f"  Error: {data}")
        continue

    # Binary search for earliest available data (with higher timeout)
    print(f"  Checking history depth...")
    for year in ["2020", "2019", "2018", "2017"]:
        s2, d2 = probe(symbol, f"{year}-01-01T00:00:00", f"{year}-03-01T00:00:00", limit=1, timeout=60)
        if s2 == 200 and d2:
            print(f"  Data available from: {year} (first: {d2[0].get('time_exchange', '?')[:19]})")
            break
        elif s2 == "TIMEOUT":
            print(f"  {year}: timed out (data likely exists but slow to retrieve)")
            break
    else:
        print(f"  History only goes back to ~2020 or later")
