"""
Explore available CoinAPI symbols and indexes for stablecoins.

Usage:
    python scripts/explore_coinapi_symbols.py

Tries both the Market Data API (/symbols) and the Indexes API (/indexes)
to find what's available for each stablecoin.
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

API_KEY = os.getenv("COINAPI_KEY")
if not API_KEY:
    print("Error: COINAPI_KEY not set in environment or .env file.")
    sys.exit(1)

BASE_URL = "https://rest.coinapi.io/v1"
HEADERS = {"X-CoinAPI-Key": API_KEY}

STABLECOINS = ["USDT", "USDC", "DAI", "BUSD", "UST", "USDE", "RLUSD"]


def get(path: str, params: dict = None) -> dict | list | None:
    url = f"{BASE_URL}{path}"
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        print(f"  HTTP {e.response.status_code}: {url}")
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ── 1. List all available indexes ────────────────────────────
section("All available indexes (searching for stablecoin-related)")
indexes = get("/indexes")
if indexes:
    stable_terms = {"USDT", "USDC", "DAI", "BUSD", "UST", "USDE", "RLUSD", "STABLE", "USD"}
    matched = [i for i in indexes if any(t in str(i).upper() for t in stable_terms)]
    if matched:
        for idx in matched:
            print(f"  {idx}")
    else:
        print(f"  No stablecoin indexes found. Total indexes available: {len(indexes)}")
        print("  First 20 index IDs:")
        for idx in indexes[:20]:
            print(f"    {idx}")
else:
    print("  Could not fetch index list.")

# ── 2. Market Data API symbols (SPOT + INDEX types) ──────────
section("Market Data API — symbols by stablecoin")
for coin in STABLECOINS:
    print(f"\n  {coin}/USD:")
    data = get("/symbols", params={"filter_symbol_id": f"{coin}_USD"})
    if data:
        by_type: dict[str, list[str]] = {}
        for s in data:
            t = s.get("symbol_type", "UNKNOWN")
            sid = s.get("symbol_id", "")
            by_type.setdefault(t, []).append(sid)
        for stype, ids in sorted(by_type.items()):
            print(f"    [{stype}]")
            for sid in sorted(ids):
                print(f"      {sid}")
    # (errors already printed inside get())

# ── 3. Probe known Index API timeseries endpoint ──────────────
section("Index API — probing timeseries endpoint")
candidate_ids = [
    f"COINAPI_VWAP_{coin}_USD" for coin in STABLECOINS
] + [
    f"COINAPI_PRIMKT_{coin}_USD" for coin in STABLECOINS
] + [
    f"IDX_{coin}_USD" for coin in STABLECOINS
]

for idx_id in candidate_ids:
    result = get(f"/indexes/{idx_id}/timeseries", params={
        "period_id": "5MIN",
        "time_start": "2024-01-01T00:00:00",
        "limit": 1,
    })
    if result:
        print(f"  FOUND: {idx_id} → {result}")
    # 404s are expected — only print successes
