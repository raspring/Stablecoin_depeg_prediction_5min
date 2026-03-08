"""
Fetch Solana USDC mint + burn events from Dune Analytics via API.

Dune is the authoritative source for complete USDC Solana history (Oct 2020 →).
Helius only indexes back to ~Aug 2024, so Dune is used for the historical baseline.
collect_solana.py handles incremental updates for events after the Dune cutoff.

Dune query (use {{start_date}} / {{end_date}} parameters):

    SELECT
        block_time   AS timestamp,
        tx_id        AS tx_hash,
        action       AS event_type,
        amount / 1e6 AS amount_usdc
    FROM tokens_solana.transfers
    WHERE token_mint_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
      AND action             IN ('mint', 'burn')
      AND amount             > 0
      AND block_date >= DATE '{{start_date}}'
      AND block_date <  DATE '{{end_date}}'
    ORDER BY block_time

Setup:
  1. Create/update the query in Dune UI with the SQL above.
  2. Save it. Note the query ID from the URL (dune.com/queries/{id}).
  3. Add to .env:
       DUNE_API_KEY=your_key
       DUNE_USDC_SOL_QUERY_ID=123456

Output:
  data/raw/onchain/usdc_sol_events_dune.parquet  — raw mint/burn events
  data/raw/onchain/usdc_sol_5m.parquet           — 5-min aggregated flows
    sol_mint_count, sol_mint_volume_usd
    sol_burn_count, sol_burn_volume_usd
    sol_net_flow_usd

Usage:
  python src/data/collect_dune.py --query-id 123456
"""

import os
import time
import argparse
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
POLL_INTERVAL = 5
MAX_WAIT      = 1800   # 30 min per chunk
CHUNK_DAYS    = 21     # 3-week chunks — keeps results well under Dune's 32k row limit

START_DATE = date(2020, 10, 1)   # USDC launched on Solana
END_DATE   = date(2026, 3, 1)


def _headers(api_key: str) -> dict:
    return {"X-Dune-API-Key": api_key}


def execute_chunk(query_id: int, api_key: str, start: date, end: date) -> str:
    r = requests.post(
        f"{DUNE_API_BASE}/query/{query_id}/execute",
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
            raise TimeoutError(f"Chunk still running after {MAX_WAIT}s")
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


def _chunks(start: date, end: date, days: int):
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        yield cur, nxt
        cur = nxt


def collect_all(query_id: int, api_key: str) -> pd.DataFrame:
    out_dir         = RAW_DIR / "onchain"
    checkpoint_path = out_dir / "usdc_sol_dune_checkpoint.parquet"
    out_dir.mkdir(parents=True, exist_ok=True)

    existing = pd.DataFrame()
    done_end = START_DATE

    if checkpoint_path.exists():
        existing = pd.read_parquet(checkpoint_path)
        if not existing.empty:
            done_end = existing["timestamp"].max().date()
            mint_n = (existing["event_type"] == "mint").sum()
            burn_n = (existing["event_type"] == "burn").sum()
            print(f"  Resuming from {done_end} "
                  f"({mint_n:,} mints, {burn_n:,} burns in checkpoint)")

    pending = [(s, e) for s, e in _chunks(START_DATE, END_DATE, CHUNK_DAYS)
               if e > done_end]

    if not pending:
        print("  Already up to date.")
        return existing

    combined = existing.copy()

    for chunk_start, chunk_end in pending:
        print(f"  Chunk {chunk_start} → {chunk_end} ... ", end="", flush=True)
        try:
            eid      = execute_chunk(query_id, api_key, chunk_start, chunk_end)
            poll_until_done(eid, api_key)
            chunk_df = fetch_results(eid, api_key)
        except Exception as e:
            print(f"\n  ERROR on chunk {chunk_start}–{chunk_end}: {e}")
            break

        if not chunk_df.empty:
            chunk_df["timestamp"]   = pd.to_datetime(chunk_df["timestamp"], utc=True)
            chunk_df["amount_usdc"] = chunk_df["amount_usdc"].astype(float)
            combined = (pd.concat([combined, chunk_df], ignore_index=True)
                        .drop_duplicates(subset=["tx_hash", "event_type"])
                        .sort_values("timestamp")
                        .reset_index(drop=True))
            combined.to_parquet(checkpoint_path, index=False)
            mints = (chunk_df["event_type"] == "mint").sum()
            burns = (chunk_df["event_type"] == "burn").sum()
            print(f"    {mints} mints, {burns} burns  (total {len(combined):,})")
        else:
            print(f"    0 events")

    return combined


def aggregate_5m(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["ts_5m"] = df["timestamp"].dt.floor("5min")

    frames = []
    for event_type, prefix in [("mint", "sol_mint"), ("burn", "sol_burn")]:
        subset = df[df["event_type"] == event_type]
        if subset.empty:
            continue
        agg = subset.groupby("ts_5m").agg(
            **{f"{prefix}_count":      ("amount_usdc", "count"),
               f"{prefix}_volume_usd": ("amount_usdc", "sum")}
        )
        frames.append(agg)

    if not frames:
        return pd.DataFrame()

    result = frames[0]
    for f in frames[1:]:
        result = result.join(f, how="outer")

    result = result.fillna(0)
    result["sol_net_flow_usd"] = (
        result.get("sol_mint_volume_usd", 0) - result.get("sol_burn_volume_usd", 0)
    )
    result.index.name = "timestamp"
    return result.reset_index().sort_values("timestamp")


def save(raw: pd.DataFrame, bins: pd.DataFrame) -> None:
    out_dir = RAW_DIR / "onchain"
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_path  = out_dir / "usdc_sol_events_dune.parquet"
    bins_path = out_dir / "usdc_sol_5m.parquet"

    raw.to_parquet(raw_path, index=False)
    bins.to_parquet(bins_path, index=False)

    mint_n = (raw["event_type"] == "mint").sum()
    burn_n = (raw["event_type"] == "burn").sum()
    print(f"  Saved {raw_path.name}  ({mint_n:,} mints, {burn_n:,} burns)")
    print(f"  Saved {bins_path.name} ({len(bins):,} 5-min rows)")
    print(f"  Date range: {raw['timestamp'].min().date()} → {raw['timestamp'].max().date()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-id", type=int,
                        default=int(os.getenv("DUNE_USDC_SOL_QUERY_ID", 0)))
    args = parser.parse_args()

    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        raise SystemExit("DUNE_API_KEY not set in .env")
    if not args.query_id:
        raise SystemExit("Provide --query-id or set DUNE_USDC_SOL_QUERY_ID in .env")

    print(f"=== Dune USDC Solana Mint+Burn Collector (query {args.query_id}) ===")
    raw = collect_all(args.query_id, api_key)

    if not raw.empty:
        bins = aggregate_5m(raw)
        save(raw, bins)

    print("\nDone.")


if __name__ == "__main__":
    main()
