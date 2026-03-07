"""
Collect USDC mint/burn events on Solana via Helius enhanced API.

USDC is Circle's primary stablecoin on Solana with $9.3B supply (17% of total USDC).
USDT on Solana (Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB) is Wormhole-bridged
from Ethereum/TRON, not natively issued — supply changes appear as bridge operations,
not TOKEN_MINT events, so USDT Solana is not collected here.

USDC Solana mechanics:
  Mints: Circle's mint authority (BJE5MMbqXjVwjAF7oxwPYXnTXDyspzZyt4vwenNw5ruG)
         issues large batches via TOKEN_MINT. ~969 events since Aug 2024.
  Burns: Circle uses short-lived rotating addresses that receive USDC and burn it
         (type="BURN" in Helius). The active burn addresses are listed in
         USDC_BURN_ADDRESSES below. Burns are sparse (~18/day when active).
         Historical burn addresses before Aug 2024 are not captured.

USDC Solana mint : EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v  (6 decimals)
Mint authority   : BJE5MMbqXjVwjAF7oxwPYXnTXDyspzZyt4vwenNw5ruG
USDC launched    : ~October 2020 on Solana

API: Helius enhanced transaction API (api.helius.xyz)
     Set HELIUS_API_KEY in .env — free tier: 1M credits/month

Output:
  data/raw/onchain/usdc_sol_events.parquet  — raw mint/burn events
  data/raw/onchain/usdc_sol_5m.parquet      — 5-min aggregated flows

5-min columns (prefixed sol_):
  sol_mint_count, sol_mint_volume_usd
  sol_burn_count, sol_burn_volume_usd
  sol_net_flow_usd                          (mint − burn)

Usage:
  python src/data/collect_solana.py
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

# ── Config ────────────────────────────────────────────────────────────────────

HELIUS_BASE    = "https://api.helius.xyz/v0"
USDC_MINT      = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDC_AUTHORITY = "BJE5MMbqXjVwjAF7oxwPYXnTXDyspzZyt4vwenNw5ruG"

# Known Circle burn addresses on Solana. Circle uses rotating short-lived addresses
# that receive USDC from users and burn it (Helius type="BURN"). Add new addresses
# here as they are discovered. Historical addresses before Aug 2024 are unknown.
USDC_BURN_ADDRESSES = [
    "41zCUJsKk6cMB94DDtm99qWmyMZfp4GkAhhuz4xTwePu",   # active Mar 2026
]

# USDC launched on Solana ~Oct 2020; but Helius only indexes back to ~Aug 2024
USDC_SOL_START = datetime(2024, 8, 1, tzinfo=timezone.utc)

PAGE_LIMIT  = 100    # Helius max per page
RATE_DELAY  = 0.2   # seconds between calls

# ── Helius API ─────────────────────────────────────────────────────────────────

def _get(path: str, params: dict, api_key: str, retries: int = 5) -> list | dict:
    params = {**params, "api-key": api_key}
    url = f"{HELIUS_BASE}/{path}"
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 404:
                # Helius returns 404 when no transactions of that type exist
                return []
            r.raise_for_status()
            time.sleep(RATE_DELAY)
            return r.json()
        except requests.RequestException as e:
            wait = 2 ** attempt
            print(f"    Retry {attempt+1}/{retries} in {wait}s: {e}")
            time.sleep(wait)
    raise RuntimeError(f"Helius API failed after {retries} retries: {url}")


def fetch_page(address: str, api_key: str, tx_type: str | None = None,
               before: str | None = None) -> list:
    """Fetch one page of transactions for an address."""
    params = {"limit": PAGE_LIMIT}
    if tx_type:
        params["type"] = tx_type
    if before:
        params["before"] = before

    result = _get(f"addresses/{address}/transactions", params, api_key)

    # Helius returns a list on success, or a dict with 'error' key
    if isinstance(result, dict) and "error" in result:
        err = result["error"]
        # "Failed to find events" with a next cursor — extract it and return empty
        if "before-signature" in err:
            import re
            match = re.search(r'`before-signature` parameter set to (\S+)\.', err)
            if match:
                return []   # no results in this window; caller can continue with next cursor
        print(f"    Helius error: {err}")
        return []

    return result if isinstance(result, list) else []


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_tx(tx: dict, event_type: str) -> list[dict]:
    """
    Extract mint or burn amount from a Helius enhanced transaction.
    tokenAmount in Helius is already in UI (human-readable) form — no decimal conversion.
    """
    ts_unix = tx.get("timestamp")
    if not ts_unix:
        return []
    timestamp = datetime.fromtimestamp(ts_unix, tz=timezone.utc)
    tx_hash = tx.get("signature", "")

    records = []
    for tt in tx.get("tokenTransfers", []):
        if not tt.get("mint", "").startswith(USDC_MINT[:10]):
            continue
        amount = float(tt.get("tokenAmount", 0))
        if amount <= 0:
            continue

        # Mint: from empty (created from nothing)
        # Burn: to empty (destroyed)
        frm = tt.get("fromUserAccount", "")
        to  = tt.get("toUserAccount", "")

        if event_type == "mint" and not frm:
            records.append({"timestamp": timestamp, "tx_hash": tx_hash,
                            "event_type": "mint", "amount_usd": amount})
        elif event_type == "burn" and not to:
            records.append({"timestamp": timestamp, "tx_hash": tx_hash,
                            "event_type": "burn", "amount_usd": amount})

    return records


# ── Collection ────────────────────────────────────────────────────────────────

def collect_source(address: str, api_key: str, tx_type: str,
                   event_type: str, start_ts: datetime,
                   last_sig: str | None = None) -> list[dict]:
    """
    Paginate backward through an address's transaction history,
    collecting events until we reach start_ts or run out of pages.
    """
    events   = []
    before   = last_sig
    n_pages  = 0
    done     = False

    while not done:
        txs = fetch_page(address, api_key, tx_type=tx_type, before=before)

        if not txs:
            break

        for tx in txs:
            ts = tx.get("timestamp", 0)
            if ts and datetime.fromtimestamp(ts, tz=timezone.utc) < start_ts:
                done = True
                break
            events.extend(parse_tx(tx, event_type))

        n_pages += 1
        before = txs[-1].get("signature")
        if not before:
            break

    print(f"    {event_type}: {len(events)} events ({n_pages} pages)")
    return events


def collect_all(api_key: str) -> pd.DataFrame:
    out_dir         = RAW_DIR / "onchain"
    checkpoint_path = out_dir / "usdc_sol_checkpoint.parquet"

    existing     = pd.DataFrame()
    last_sig     = None
    start_ts     = USDC_SOL_START

    if checkpoint_path.exists():
        existing = pd.read_parquet(checkpoint_path)
        if not existing.empty:
            # Resume from most recent event — find last processed signature
            last_ts = existing["timestamp"].max()
            print(f"  Resuming from {last_ts} ({len(existing):,} events in checkpoint)")
            # We paginate backwards, so use a cutoff timestamp instead
            start_ts = last_ts

    print(f"  Collecting USDC Solana events from {start_ts.date()} → now")

    # Collect mints from authority
    print("  Fetching mints (authority)...")
    mint_events = collect_source(
        USDC_AUTHORITY, api_key, tx_type="TOKEN_MINT",
        event_type="mint", start_ts=start_ts
    )

    # Collect burns from known Circle burn addresses (type="BURN" in Helius)
    # Burns are indexed by burner address, not by the USDC mint address.
    print("  Fetching burns (Circle burn addresses)...")
    burn_events = []
    for burn_addr in USDC_BURN_ADDRESSES:
        burn_events.extend(collect_source(
            burn_addr, api_key, tx_type="BURN",
            event_type="burn", start_ts=start_ts
        ))

    all_new = mint_events + burn_events

    if not all_new:
        print("  No new events found.")
        return existing

    new_df = pd.DataFrame(all_new)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = (combined
                .drop_duplicates(subset=["tx_hash", "event_type"])
                .sort_values("timestamp")
                .reset_index(drop=True))

    out_dir.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(checkpoint_path, index=False)
    print(f"  Checkpoint: {len(combined):,} events → {checkpoint_path.name}")
    return combined


# ── Aggregation ───────────────────────────────────────────────────────────────

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
            **{f"{prefix}_count":      ("amount_usd", "count"),
               f"{prefix}_volume_usd": ("amount_usd", "sum")}
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


# ── Save ──────────────────────────────────────────────────────────────────────

def save(raw: pd.DataFrame, bins: pd.DataFrame) -> None:
    out_dir = RAW_DIR / "onchain"
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_path  = out_dir / "usdc_sol_events.parquet"
    bins_path = out_dir / "usdc_sol_5m.parquet"

    raw.to_parquet(raw_path, index=False)
    bins.to_parquet(bins_path, index=False)

    print(f"  Saved {raw_path.name}  ({len(raw):,} events)")
    print(f"  Saved {bins_path.name} ({len(bins):,} 5-min rows)")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    api_key = os.getenv("HELIUS_API_KEY")
    if not api_key:
        raise SystemExit("HELIUS_API_KEY not set in .env")

    print("=== USDC Solana Collector ===")
    raw  = collect_all(api_key)
    bins = aggregate_5m(raw)
    if not bins.empty:
        save(raw, bins)
    print("\nDone.")
