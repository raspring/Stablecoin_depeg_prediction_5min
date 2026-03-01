"""
Collect USDT treasury flows on TRON via TronGrid API.

Tracks TRC20 USDT transfers to and from known Tether treasury wallets on TRON.
Inflows  (→ treasury) — institutions returning USDT for redemption (leading indicator)
Outflows (← treasury) — Tether re-issuing USDT from existing treasury stock

USDT TRC20 contract : TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t  (6 decimals)
TRON USDT launched  : ~2019-04-01

Treasury wallets (confirmed via Whale Alert / Arkham labels):
  THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC — Tether Treasury (active ~2019–2021)
  TKHuVq1oKVruCGLvqVexFs6dawKv6fQgFs — Tether Treasury (active ~2020–2022)
  TBPxhVAsuzoFnKyXtc1o2UySEydPHgATto — Tether Treasury (active 2024+)

Inter-treasury transfers are excluded to avoid double-counting.

Output:
  data/raw/onchain/usdt_tron_treasury.parquet  — raw transfers (deduped)
  data/raw/onchain/usdt_tron_5m.parquet        — 5-min aggregated flows

5-min columns:
  treasury_inflow_count, treasury_inflow_volume_usd   — redemptions
  treasury_outflow_count, treasury_outflow_volume_usd — re-issuances
  treasury_net_flow_usd  (outflow − inflow; positive = net re-issuance)

API key:
  Set TRONGRID_API_KEY in .env for higher rate limits (20 QPS).
  Without a key the public tier allows ~10 QPS.
"""

import os
import time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TRONGRID_URL  = "https://api.trongrid.io"
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
USDT_DECIMALS = 6

# Known Tether treasury wallets on TRON (labeled by Whale Alert / Arkham).
# Add new addresses here as Tether rotates wallets.
TREASURY_WALLETS = [
    "THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC",   # ~2019–2021
    "TKHuVq1oKVruCGLvqVexFs6dawKv6fQgFs",   # ~2020–2022
    "TBPxhVAsuzoFnKyXtc1o2UySEydPHgATto",   # 2024+
]

# TRC20 USDT launched on TRON ~2019-04-01
START_MS   = 1_554_076_800_000   # 2019-04-01 00:00:00 UTC in milliseconds
CHUNK_MS   = 30 * 24 * 3600 * 1000   # 30-day windows per API call sequence
PAGE_LIMIT = 200                      # TronGrid max per page
RATE_DELAY = 0.1                      # seconds between calls (~10 req/s)

# ---------------------------------------------------------------------------
# TronGrid API
# ---------------------------------------------------------------------------

def _headers(api_key: str | None) -> dict:
    return {"TRON-PRO-API-KEY": api_key} if api_key else {}


def fetch_page(address: str, min_ms: int, max_ms: int,
               api_key: str | None, fingerprint: str | None = None,
               retries: int = 5) -> dict:
    params = {
        "only_confirmed":   "true",
        "limit":            PAGE_LIMIT,
        "contract_address": USDT_CONTRACT,
        "min_timestamp":    min_ms,
        "max_timestamp":    max_ms,
        "order_by":         "block_timestamp,asc",
    }
    if fingerprint:
        params["fingerprint"] = fingerprint

    url = f"{TRONGRID_URL}/v1/accounts/{address}/transactions/trc20"

    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=_headers(api_key),
                             timeout=30)
            r.raise_for_status()
            time.sleep(RATE_DELAY)
            return r.json()
        except requests.RequestException as e:
            wait = 2 ** attempt
            print(f"    Retrying in {wait}s (attempt {attempt+1}/{retries}): {e}")
            time.sleep(wait)

    raise RuntimeError(f"Failed after {retries} retries for {address} "
                       f"ms={min_ms}–{max_ms}")


def collect_wallet(address: str, api_key: str | None,
                   start_ms: int, end_ms: int) -> list[dict]:
    """Fetch all USDT TRC20 transfers for one treasury wallet."""
    records    = []
    chunk_start = start_ms

    while chunk_start < end_ms:
        chunk_end   = min(chunk_start + CHUNK_MS - 1, end_ms)
        fingerprint = None

        while True:
            resp = fetch_page(address, chunk_start, chunk_end, api_key, fingerprint)
            data = resp.get("data", [])
            records.extend(data)

            fingerprint = resp.get("meta", {}).get("fingerprint")
            if not data or not fingerprint:
                break

        chunk_start = chunk_end + 1

    return records


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_records(raw: list[dict], wallet_address: str) -> pd.DataFrame:
    """Convert TronGrid transfer records to a clean DataFrame."""
    treasury_set = set(TREASURY_WALLETS)
    rows = []

    for tx in raw:
        from_addr = tx.get("from", "")
        to_addr   = tx.get("to",   "")

        # Skip inter-treasury transfers to avoid double-counting
        if from_addr in treasury_set and to_addr in treasury_set:
            continue

        is_inflow = (to_addr == wallet_address)
        rows.append({
            "timestamp":         datetime.fromtimestamp(
                                     tx["block_timestamp"] / 1000, tz=timezone.utc),
            "block_timestamp_ms": tx["block_timestamp"],
            "tx_hash":            tx["transaction_id"],
            "from_addr":          from_addr,
            "to_addr":            to_addr,
            "event_type":         "treasury_inflow" if is_inflow else "treasury_outflow",
            "amount_usd":         int(tx["value"]) / (10 ** USDT_DECIMALS),
            "wallet":             wallet_address,
        })

    if not rows:
        return pd.DataFrame(columns=[
            "timestamp", "block_timestamp_ms", "tx_hash",
            "from_addr", "to_addr", "event_type", "amount_usd", "wallet",
        ])
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Checkpoint + collection
# ---------------------------------------------------------------------------

def collect_all_wallets(api_key: str | None) -> pd.DataFrame:
    out_dir         = RAW_DIR / "onchain"
    checkpoint_path = out_dir / "usdt_tron_treasury_checkpoint.parquet"

    existing = pd.DataFrame()
    start_ms = START_MS

    if checkpoint_path.exists():
        existing = pd.read_parquet(checkpoint_path)
        if not existing.empty:
            start_ms = int(existing["block_timestamp_ms"].max()) + 1
            ts_str   = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat()
            print(f"  Resuming from {ts_str} ({len(existing):,} events in checkpoint)")

    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    if start_ms >= now_ms:
        print("  Already up to date.")
        return existing

    new_frames = []
    for wallet in TREASURY_WALLETS:
        print(f"  Collecting {wallet}...")
        raw = collect_wallet(wallet, api_key, start_ms, now_ms)
        df  = parse_records(raw, wallet)
        print(f"    {len(df):,} transfers")
        new_frames.append(df)

    if new_frames:
        new_df   = pd.concat(new_frames, ignore_index=True)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = (combined
                    .drop_duplicates(subset=["tx_hash", "wallet"])
                    .sort_values("timestamp")
                    .reset_index(drop=True))
    else:
        combined = existing

    out_dir.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(checkpoint_path, index=False)
    print(f"  Checkpoint saved: {checkpoint_path.name} ({len(combined):,} events)")
    return combined


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_5m(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate treasury transfers into 5-minute bins."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["ts_5m"] = df["timestamp"].dt.floor("5min")

    inflows  = df[df["event_type"] == "treasury_inflow"].groupby("ts_5m").agg(
        treasury_inflow_count=("amount_usd", "count"),
        treasury_inflow_volume_usd=("amount_usd", "sum"),
    )
    outflows = df[df["event_type"] == "treasury_outflow"].groupby("ts_5m").agg(
        treasury_outflow_count=("amount_usd", "count"),
        treasury_outflow_volume_usd=("amount_usd", "sum"),
    )

    agg = inflows.join(outflows, how="outer").fillna(0)
    agg["treasury_net_flow_usd"] = (
        agg["treasury_outflow_volume_usd"] - agg["treasury_inflow_volume_usd"]
    )
    agg.index.name = "timestamp"
    return agg.reset_index().sort_values("timestamp")


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def save(raw: pd.DataFrame, bins: pd.DataFrame) -> None:
    out_dir = RAW_DIR / "onchain"
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_path  = out_dir / "usdt_tron_treasury.parquet"
    bins_path = out_dir / "usdt_tron_5m.parquet"

    raw.to_parquet(raw_path,  index=False)
    bins.to_parquet(bins_path, index=False)

    print(f"  Saved {raw_path}  ({len(raw):,} transfers)")
    print(f"  Saved {bins_path} ({len(bins):,} 5-min rows)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    api_key = os.getenv("TRONGRID_API_KEY")
    if not api_key:
        print("Warning: TRONGRID_API_KEY not set — using public rate limits (~10 QPS)")

    print("=== USDT TRON Treasury Flows ===")
    raw  = collect_all_wallets(api_key)
    bins = aggregate_5m(raw)
    save(raw, bins)

    print("\nDone.")
