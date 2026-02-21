"""
Verify order book symbol availability and snapshot frequency.

Run this after getting COINAPI_MARKETDATA_KEY to confirm:
  - Each symbol exists and returns data
  - Snapshot frequency (how many snapshots per hour/day)
  - How far back history goes

Usage:
    python scripts/explore_orderbook.py
"""

import os
import sys
from datetime import datetime, timezone
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


def probe(symbol_id: str, time_start: str, time_end: str, limit: int = 10):
    url = f"{BASE_URL}/orderbooks/{symbol_id}/history"
    r = requests.get(url, headers=HEADERS, params={
        "time_start": time_start,
        "time_end": time_end,
        "limit": limit,
    }, timeout=30)
    return r.status_code, r.json() if r.status_code == 200 else r.text[:200]


for coin, cfg in STABLECOINS.items():
    symbol = cfg.get("orderbook_symbol")
    if not symbol:
        print(f"\n{coin.upper()}: no symbol configured, skipping")
        continue

    print(f"\n{'='*55}")
    print(f"  {coin.upper()} — {symbol}")
    print(f"{'='*55}")

    # Recent data (2024)
    status, data = probe(symbol, "2024-01-01T00:00:00", "2024-01-01T01:00:00", limit=5)
    print(f"  2024 sample: HTTP {status}")
    if status == 200 and data:
        print(f"  Snapshots in 1hr window: {len(data)}")
        print(f"  First snapshot: {data[0].get('time_exchange', data[0])}")
        snap = data[0]
        bids = snap.get("bids", [])
        asks = snap.get("asks", [])
        print(f"  Bid levels: {len(bids)}, Ask levels: {len(asks)}")
        if bids and asks:
            print(f"  Best bid: {bids[0]['price']}, Best ask: {asks[0]['price']}")

    # Check earliest available
    status2, data2 = probe(symbol, "2018-01-01T00:00:00", "2018-06-01T00:00:00", limit=1)
    if status2 == 200 and data2:
        print(f"  Earliest available: {data2[0].get('time_exchange', '?')}")
    else:
        status3, data3 = probe(symbol, "2020-01-01T00:00:00", "2020-06-01T00:00:00", limit=1)
        if status3 == 200 and data3:
            print(f"  Earliest available: {data3[0].get('time_exchange', '?')} (no data before 2020)")
        else:
            print(f"  Could not determine earliest data (HTTP {status3})")
