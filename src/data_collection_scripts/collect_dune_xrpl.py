"""
Fetch RLUSD mint + burn events on XRPL from Dune Analytics via API.

Dune query ID: 6811285
  SELECT _ledger_close_time_human AS timestamp, ledger_index, hash AS tx_hash,
         CAST(amount.value AS DOUBLE) AS amount_rlusd,
         CASE WHEN account = issuer THEN 'mint' WHEN destination = issuer THEN 'burn' END AS event_type
  FROM xrpl.transactions
  WHERE amount.currency = '524C555344000000000000000000000000000000'
    AND amount.issuer   = 'rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De'
    AND transaction_type = 'Payment'
    AND (account = issuer OR destination = issuer)
    AND ledger_close_date >= DATE '{{start_date}}'
    AND ledger_close_date <  DATE '{{end_date}}'

Timestamp format from Dune: '2024-Nov-15 20:23:20.000000000 UTC'

Output:
  data/raw/onchain/rlusd_xrpl_events.parquet  — raw mint/burn events
  data/raw/onchain/rlusd_xrpl_5m.parquet      — 5-min aggregated flows

5-min columns:
  xrpl_mint_count, xrpl_mint_volume_usd
  xrpl_burn_count, xrpl_burn_volume_usd
  xrpl_net_flow_usd

Usage:
  python src/data_collection_scripts/collect_dune_xrpl.py
"""

import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR

load_dotenv()

DUNE_API_BASE = "https://api.dune.com/api/v1"
QUERY_ID      = 6811285
POLL_INTERVAL = 5
MAX_WAIT      = 1800

START_DATE = date(2024, 11, 1)   # RLUSD first appeared on XRPL
END_DATE   = date(2026, 3, 1)
CHUNK_DAYS = 60                  # 5,559 total rows — 2-month chunks well under limits


def _headers(api_key: str) -> dict:
    return {"X-Dune-API-Key": api_key}


def execute_chunk(api_key: str, start: date, end: date) -> str:
    r = requests.post(
        f"{DUNE_API_BASE}/query/{QUERY_ID}/execute",
        headers=_headers(api_key),
        json={"query_parameters": {
            "start_date": start.isoformat(),
            "end_date":   end.isoformat(),
        }},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["execution_id"]


def poll_until_done(execution_id: str, api_key: str) -> None:
    start = time.time()
    while True:
        r = requests.get(
            f"{DUNE_API_BASE}/execution/{execution_id}/status",
            headers=_headers(api_key),
            timeout=30,
        )
        r.raise_for_status()
        state   = r.json()["state"]
        elapsed = int(time.time() - start)
        if state == "QUERY_STATE_COMPLETED":
            print(f"done ({elapsed}s)")
            return
        if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
            raise RuntimeError(f"Query ended with state: {state}")
        if elapsed > MAX_WAIT:
            raise TimeoutError(f"Still running after {MAX_WAIT}s")
        time.sleep(POLL_INTERVAL)


def fetch_results(execution_id: str, api_key: str) -> pd.DataFrame:
    rows, offset, limit = [], 0, 50_000
    while True:
        r = requests.get(
            f"{DUNE_API_BASE}/execution/{execution_id}/results",
            headers=_headers(api_key),
            params={"limit": limit, "offset": offset},
            timeout=60,
        )
        r.raise_for_status()
        batch = r.json().get("result", {}).get("rows", [])
        rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return pd.DataFrame(rows)


def parse_timestamp(s: str) -> pd.Timestamp:
    """Parse Dune XRPL timestamp: '2024-Nov-15 20:23:20.000000000 UTC'"""
    return pd.to_datetime(s, format="%Y-%b-%d %H:%M:%S.%f UTC", utc=True)


def collect_all(api_key: str) -> pd.DataFrame:
    out_dir         = RAW_DIR / "onchain"
    checkpoint_path = out_dir / "rlusd_xrpl_dune_checkpoint.parquet"
    out_dir.mkdir(parents=True, exist_ok=True)

    existing = pd.DataFrame()
    done_end = START_DATE

    if checkpoint_path.exists():
        existing = pd.read_parquet(checkpoint_path)
        if not existing.empty:
            done_end = existing["timestamp"].max().date()
            print(f"  Resuming from {done_end} ({len(existing):,} events in checkpoint)")

    # Build pending chunks
    chunks = []
    cur = START_DATE
    while cur < END_DATE:
        nxt = min(cur + timedelta(days=CHUNK_DAYS), END_DATE)
        chunks.append((cur, nxt))
        cur = nxt
    pending = [(s, e) for s, e in chunks if e > done_end]

    if not pending:
        print("  Already up to date.")
        return existing

    combined = existing.copy()

    for chunk_start, chunk_end in pending:
        print(f"  Chunk {chunk_start} → {chunk_end} ... ", end="", flush=True)
        try:
            eid      = execute_chunk(api_key, chunk_start, chunk_end)
            poll_until_done(eid, api_key)
            chunk_df = fetch_results(eid, api_key)
        except Exception as e:
            print(f"\n  ERROR on chunk {chunk_start}–{chunk_end}: {e}")
            break

        if not chunk_df.empty:
            chunk_df["timestamp"]    = chunk_df["timestamp"].apply(parse_timestamp)
            chunk_df["amount_rlusd"] = chunk_df["amount_rlusd"].astype(float)
            chunk_df["ledger_index"] = chunk_df["ledger_index"].astype(int)
            combined = (pd.concat([combined, chunk_df], ignore_index=True)
                        .drop_duplicates(subset=["tx_hash", "event_type"])
                        .sort_values("timestamp")
                        .reset_index(drop=True))
            combined.to_parquet(checkpoint_path, index=False)
            mints = (chunk_df["event_type"] == "mint").sum()
            burns = (chunk_df["event_type"] == "burn").sum()
            print(f"  {mints} mints, {burns} burns  (total {len(combined):,})")
        else:
            print("  0 events")

    return combined


def aggregate_5m(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["ts_5m"] = df["timestamp"].dt.floor("5min")

    frames = []
    for event_type, prefix in [("mint", "xrpl_mint"), ("burn", "xrpl_burn")]:
        subset = df[df["event_type"] == event_type]
        if subset.empty:
            continue
        agg = subset.groupby("ts_5m").agg(
            **{f"{prefix}_count":      ("amount_rlusd", "count"),
               f"{prefix}_volume_usd": ("amount_rlusd", "sum")}
        )
        frames.append(agg)

    if not frames:
        return pd.DataFrame()

    result = frames[0]
    for f in frames[1:]:
        result = result.join(f, how="outer")

    result = result.fillna(0)
    result["xrpl_net_flow_usd"] = (
        result.get("xrpl_mint_volume_usd", 0) - result.get("xrpl_burn_volume_usd", 0)
    )
    result.index.name = "timestamp"
    return result.reset_index().sort_values("timestamp")


def save(raw: pd.DataFrame, bins: pd.DataFrame) -> None:
    out_dir = RAW_DIR / "onchain"
    raw_path  = out_dir / "rlusd_xrpl_events.parquet"
    bins_path = out_dir / "rlusd_xrpl_5m.parquet"

    raw.to_parquet(raw_path, index=False)
    bins.to_parquet(bins_path, index=False)

    mints = (raw["event_type"] == "mint").sum()
    burns = (raw["event_type"] == "burn").sum()
    print(f"  Saved {raw_path.name}  ({mints:,} mints, {burns:,} burns)")
    print(f"  Saved {bins_path.name} ({len(bins):,} 5-min rows)")
    print(f"  Date range: {raw['timestamp'].min().date()} → {raw['timestamp'].max().date()}")


if __name__ == "__main__":
    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        raise SystemExit("DUNE_API_KEY not set in .env")

    print(f"=== Dune RLUSD XRPL Mint+Burn Collector (query {QUERY_ID}) ===")
    raw = collect_all(api_key)

    if not raw.empty:
        bins = aggregate_5m(raw)
        save(raw, bins)

    print("\nDone.")
