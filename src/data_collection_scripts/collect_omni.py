"""
collect_omni.py — USDT Omni Layer treasury inflow/outflow collector

Pulls all Simple Send transactions to/from Tether's known Omni treasury
address on the Bitcoin blockchain via OmniExplorer API.

Treasury address: 1NTMakcgVwQpMdGxRQnFKyb3G1FAJysSfz

Inflow  (redemption pressure): someone sends USDT *to* the treasury
Outflow (re-issuance):         treasury sends USDT *out* to institutions

Output files:
  data/raw/omni/usdt_omni_treasury.parquet   — raw events
  data/raw/omni/usdt_omni_5m.parquet         — 5-minute aggregates

Usage:
  python src/data_collection_scripts/collect_omni.py
  python src/data_collection_scripts/collect_omni.py --refresh   # ignore checkpoint, re-collect all
"""

import argparse
import time
from datetime import timezone
from pathlib import Path

import pandas as pd
import requests

# ── config ───────────────────────────────────────────────────────────────────
TREASURY_ADDR = "1NTMakcgVwQpMdGxRQnFKyb3G1FAJysSfz"
PROPERTY_ID   = 31          # USDT on Omni
API_BASE      = "https://api.omniexplorer.info/v1/transaction/address"
REQUEST_DELAY = 2.0         # seconds between pages (polite rate)
MAX_RETRIES   = 8

RAW_DIR    = Path(__file__).parents[2] / "data" / "raw" / "omni"
EVENTS_F   = RAW_DIR / "usdt_omni_treasury.parquet"
AGG_5M_F   = RAW_DIR / "usdt_omni_5m.parquet"
PROGRESS_F = RAW_DIR / "usdt_omni_treasury_progress.parquet"  # saved mid-run


# ── helpers ───────────────────────────────────────────────────────────────────

def fetch_page(page: int) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(
                API_BASE,
                data={"addr": TREASURY_ADDR, "page": page},
                timeout=30,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            wait = min(2 ** attempt, 60)
            print(f"  [retry {attempt+1}/{MAX_RETRIES}] page {page}: {e} — waiting {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch page {page} after {MAX_RETRIES} attempts")


def parse_tx(tx: dict) -> dict | None:
    """Return a cleaned row dict or None if the tx should be skipped."""
    if not tx.get("valid"):
        return None
    if tx.get("propertyid") != PROPERTY_ID:
        return None

    ts = pd.Timestamp(tx["blocktime"], unit="s", tz=timezone.utc)
    amount = float(tx["amount"])
    is_inflow = tx["referenceaddress"] == TREASURY_ADDR

    return {
        "timestamp":       ts,
        "block":           tx["block"],
        "txid":            tx["txid"],
        "event_type":      "treasury_inflow" if is_inflow else "treasury_outflow",
        "amount_usd":      amount,
        "counterparty":    tx["sendingaddress"] if is_inflow else tx["referenceaddress"],
        "tx_type":         tx["type"],
    }


def collect_all(min_block: int = 0, start_page: int = 0,
                existing_rows: list | None = None) -> pd.DataFrame:
    """Fetch all pages, stopping once we hit transactions older than min_block.

    Saves progress to PROGRESS_F every 10 pages so collection can be resumed
    if interrupted.
    """
    data = fetch_page(0)
    total_pages = data["pages"]
    print(f"Total pages: {total_pages}  (starting at page {start_page})")

    rows = existing_rows or []

    for page in range(start_page, total_pages):
        data = fetch_page(page)
        txs  = data.get("transactions", [])

        page_rows = []
        stop = False
        for tx in txs:
            if tx["block"] <= min_block:
                stop = True
                break
            parsed = parse_tx(tx)
            if parsed:
                page_rows.append(parsed)

        rows.extend(page_rows)

        oldest = txs[-1]["block"] if txs else "?"
        newest = txs[0]["block"]  if txs else "?"
        n_in  = sum(1 for r in page_rows if r["event_type"] == "treasury_inflow")
        n_out = sum(1 for r in page_rows if r["event_type"] == "treasury_outflow")
        print(f"  page {page:3d}/{total_pages-1}  blocks {oldest}–{newest}"
              f"  +{n_in} in / {n_out} out  (total so far: {len(rows)})")

        if stop:
            print(f"  Reached checkpoint block {min_block} — stopping early")
            break

        # Save progress every 10 pages
        if (page + 1) % 10 == 0 and rows:
            _save_progress(rows, page + 1)

        time.sleep(REQUEST_DELAY)

    # Remove progress file on clean completion
    if PROGRESS_F.exists():
        PROGRESS_F.unlink()

    return pd.DataFrame(rows)


def _save_progress(rows: list, next_page: int) -> None:
    """Persist mid-run rows to disk so a failed run can be resumed."""
    df = pd.DataFrame(rows)
    df.attrs["next_page"] = next_page
    PROGRESS_F.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PROGRESS_F, index=False)
    print(f"  [checkpoint] saved {len(rows)} rows, next page = {next_page}")


def aggregate_5m(events: pd.DataFrame) -> pd.DataFrame:
    """Aggregate raw events to 5-minute bars."""
    if events.empty:
        return pd.DataFrame()

    events = events.set_index("timestamp").sort_index()

    inflows  = events[events["event_type"] == "treasury_inflow"]["amount_usd"]
    outflows = events[events["event_type"] == "treasury_outflow"]["amount_usd"]

    freq = "5min"
    agg = pd.DataFrame(index=pd.date_range(
        events.index.min().floor(freq),
        events.index.max().ceil(freq),
        freq=freq, tz="UTC",
    ))
    agg.index.name = "timestamp"

    agg["omni_treasury_inflow_count"]      = inflows.resample(freq).count()
    agg["omni_treasury_inflow_volume_usd"] = inflows.resample(freq).sum()
    agg["omni_treasury_outflow_count"]     = outflows.resample(freq).count()
    agg["omni_treasury_outflow_volume_usd"]= outflows.resample(freq).sum()

    agg = agg.fillna(0)
    agg["omni_treasury_net_flow_usd"] = (
        agg["omni_treasury_inflow_volume_usd"]
        - agg["omni_treasury_outflow_volume_usd"]
    )

    # Drop all-zero rows
    nonzero_mask = (agg != 0).any(axis=1)
    return agg[nonzero_mask]


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Collect USDT Omni treasury data")
    parser.add_argument("--refresh", action="store_true",
                        help="Ignore existing checkpoint and re-collect everything")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing events for checkpoint
    min_block  = 0
    start_page = 0
    existing   = pd.DataFrame()
    seed_rows  = []

    if args.refresh:
        print("--refresh: collecting full history from scratch")
        if PROGRESS_F.exists():
            PROGRESS_F.unlink()
    elif PROGRESS_F.exists():
        # Resume an interrupted run
        progress = pd.read_parquet(PROGRESS_F)
        start_page = progress.attrs.get("next_page", 0)
        seed_rows  = progress.to_dict("records")
        print(f"Resuming interrupted run from page {start_page} "
              f"({len(seed_rows)} rows already collected)")
        if EVENTS_F.exists():
            existing = pd.read_parquet(EVENTS_F)
    elif EVENTS_F.exists():
        existing = pd.read_parquet(EVENTS_F)
        if not existing.empty and "block" in existing.columns:
            min_block = int(existing["block"].max())
            print(f"Checkpoint: will collect blocks > {min_block}")
        else:
            print("Existing file found but empty — collecting all")
    else:
        print("No checkpoint — collecting full history")

    # Fetch new events
    new_events = collect_all(min_block=min_block, start_page=start_page,
                             existing_rows=seed_rows)
    print(f"\nNew events fetched: {len(new_events)}")

    if new_events.empty and existing.empty:
        print("No data collected.")
        return

    # Merge with existing
    if not existing.empty and not new_events.empty:
        combined = (
            pd.concat([existing, new_events], ignore_index=True)
              .drop_duplicates(subset=["txid"])
              .sort_values("timestamp")
              .reset_index(drop=True)
        )
    elif not new_events.empty:
        combined = new_events.sort_values("timestamp").reset_index(drop=True)
    else:
        combined = existing

    print(f"Total events after merge: {len(combined)}")
    print(f"  Inflows:  {(combined['event_type']=='treasury_inflow').sum()}")
    print(f"  Outflows: {(combined['event_type']=='treasury_outflow').sum()}")
    print(f"  Date range: {combined['timestamp'].min()} → {combined['timestamp'].max()}")

    # Save raw events
    combined.to_parquet(EVENTS_F, index=False)
    print(f"Saved raw events → {EVENTS_F}")

    # Aggregate to 5m
    agg = aggregate_5m(combined)
    agg.to_parquet(AGG_5M_F)
    print(f"Saved 5m aggregates ({len(agg)} rows) → {AGG_5M_F}")

    # Summary stats
    print("\nVolume summary by year:")
    combined["year"] = pd.to_datetime(combined["timestamp"]).dt.year
    summary = combined.groupby(["year", "event_type"])["amount_usd"].agg(["count", "sum"])
    summary["sum"] = summary["sum"].map(lambda x: f"${x/1e9:.2f}B")
    print(summary.to_string())


if __name__ == "__main__":
    main()
