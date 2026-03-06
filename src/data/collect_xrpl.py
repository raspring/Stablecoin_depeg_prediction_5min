"""
Collect RLUSD mint/burn and DEX trading flows on XRPL (XRP Ledger).

RLUSD is Ripple's native stablecoin, primarily issued on XRPL. Querying the
RLUSD issuer's account_tx captures all RLUSD activity because every RLUSD
transfer or DEX trade modifies a trust line with the issuer.

RLUSD issuer  : rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De
RLUSD currency: 524C555344000000000000000000000000000000 (hex-encoded "RLUSD")
RLUSD launched: December 2024

Event types collected:
  mint     — Payment FROM issuer to holder (supply increases)
  burn     — Payment FROM holder TO issuer (supply decreases)
  dex_sell — OfferCreate where holder's RLUSD balance decreases (selling)
  dex_buy  — OfferCreate where holder's RLUSD balance increases (buying)

API: XRPL public JSON-RPC at https://xrplcluster.com (no key required)

Output:
  data/raw/onchain/rlusd_xrpl_events.parquet  — raw events
  data/raw/onchain/rlusd_xrpl_5m.parquet      — 5-min aggregated flows

5-min columns:
  xrpl_mint_count, xrpl_mint_volume_usd
  xrpl_burn_count, xrpl_burn_volume_usd
  xrpl_net_flow_usd                          (mint − burn)
  xrpl_dex_buy_count,  xrpl_dex_buy_volume_usd
  xrpl_dex_sell_count, xrpl_dex_sell_volume_usd
  xrpl_dex_net_volume_usd                    (buy − sell)

Usage:
  python src/data/collect_xrpl.py
"""

import time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR

# ── Config ────────────────────────────────────────────────────────────────────

XRPL_URL            = "https://xrplcluster.com"
RIPPLE_EPOCH_OFFSET = 946_684_800          # Unix − Ripple epoch (seconds)

RLUSD_ISSUER   = "rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De"
RLUSD_CURRENCY = "524C555344000000000000000000000000000000"

# Approximate ledger at RLUSD launch (Dec 2024). Safe lower bound.
RLUSD_LAUNCH_LEDGER = 92_000_000

# Ledgers per chunk (~4 days at 0.26 ledgers/sec)
CHUNK_LEDGERS = 100_000

PAGE_LIMIT  = 1000   # max transactions per API call (XRPL maximum)
RATE_DELAY  = 0.15   # seconds between calls

# ── XRPL API ──────────────────────────────────────────────────────────────────

def _post(method: str, params: dict, retries: int = 5) -> dict:
    payload = {"method": method, "params": [params]}
    for attempt in range(retries):
        try:
            r = requests.post(XRPL_URL, json=payload, timeout=30)
            r.raise_for_status()
            return r.json().get("result", {})
        except requests.RequestException as e:
            wait = 2 ** attempt
            print(f"    Retry {attempt+1}/{retries} in {wait}s: {e}")
            time.sleep(wait)
    raise RuntimeError(f"XRPL API failed after {retries} retries")


def get_current_ledger() -> int:
    result = _post("ledger", {"ledger_index": "validated"})
    return result["ledger_index"]


def fetch_page(ledger_min: int, ledger_max: int, marker=None) -> dict:
    params = {
        "account":          RLUSD_ISSUER,
        "ledger_index_min": ledger_min,
        "ledger_index_max": ledger_max,
        "limit":            PAGE_LIMIT,
        "forward":          True,
    }
    if marker:
        params["marker"] = marker
    time.sleep(RATE_DELAY)
    return _post("account_tx", params)


# ── Parsing ───────────────────────────────────────────────────────────────────

def _is_rlusd(amount) -> bool:
    return (isinstance(amount, dict)
            and amount.get("currency") == RLUSD_CURRENCY
            and amount.get("issuer") == RLUSD_ISSUER)


def _get_account_rlusd_delta(account: str, meta: dict) -> float:
    """
    Return the net RLUSD balance change for `account` in this transaction.
    Positive = account gained RLUSD (bought/received).
    Negative = account lost RLUSD (sold/sent).

    In XRPL RippleState nodes, RLUSD_ISSUER is always on the HighLimit side
    (limit = 0). The holder is on the LowLimit side. Balance is stored from
    the LowLimit (holder) perspective: positive = holder holds RLUSD.
    """
    total_delta = 0.0
    for node in meta.get("AffectedNodes", []):
        for node_type in ("ModifiedNode", "DeletedNode", "CreatedNode"):
            n = node.get(node_type)
            if not n or n.get("LedgerEntryType") != "RippleState":
                continue

            fields = n.get("FinalFields") or n.get("NewFields") or {}
            prev   = n.get("PreviousFields") or {}

            hi = fields.get("HighLimit", {})
            lo = fields.get("LowLimit",  {})

            # Only RLUSD trust lines where issuer is on the HighLimit side
            if hi.get("issuer") != RLUSD_ISSUER:
                continue
            if hi.get("currency") != RLUSD_CURRENCY:
                continue

            # This node's holder (LowLimit side) must be our account
            if lo.get("issuer") != account:
                continue

            final_bal = float((fields.get("Balance") or {}).get("value", 0))
            prev_bal  = float((prev.get("Balance")  or {}).get("value", 0))

            total_delta += final_bal - prev_bal

    return total_delta


def parse_tx(tx_entry: dict) -> list[dict]:
    """Extract RLUSD events from a single transaction entry."""
    tx   = tx_entry.get("tx",   {})
    meta = tx_entry.get("meta", {})

    if meta.get("TransactionResult") != "tesSUCCESS":
        return []

    tx_type   = tx.get("TransactionType", "")
    timestamp = datetime.fromtimestamp(
        tx["date"] + RIPPLE_EPOCH_OFFSET, tz=timezone.utc
    )
    tx_hash = tx.get("hash", "")
    account = tx.get("Account", "")

    # ── Mint / Burn (Payment transactions) ────────────────────────────────
    if tx_type == "Payment":
        # Prefer delivered_amount (handles partial payments correctly)
        amount = meta.get("delivered_amount") or tx.get("Amount") or {}
        if not _is_rlusd(amount):
            return []

        value    = float(amount.get("value", 0))
        receiver = tx.get("Destination", "")

        if account == RLUSD_ISSUER:
            return [{"timestamp": timestamp, "tx_hash": tx_hash,
                     "event_type": "mint", "amount_usd": value}]
        elif receiver == RLUSD_ISSUER:
            return [{"timestamp": timestamp, "tx_hash": tx_hash,
                     "event_type": "burn", "amount_usd": value}]
        return []

    # ── DEX trades (OfferCreate) ───────────────────────────────────────────
    if tx_type == "OfferCreate":
        taker_gets = tx.get("TakerGets", {})
        taker_pays = tx.get("TakerPays", {})

        if not (_is_rlusd(taker_gets) or _is_rlusd(taker_pays)):
            return []

        delta = _get_account_rlusd_delta(account, meta)
        if abs(delta) < 0.001:
            return []

        event_type = "dex_sell" if delta < 0 else "dex_buy"
        return [{"timestamp": timestamp, "tx_hash": tx_hash,
                 "event_type": event_type, "amount_usd": abs(delta)}]

    return []


# ── Collection ────────────────────────────────────────────────────────────────

def collect_chunk(ledger_min: int, ledger_max: int) -> list[dict]:
    """Fetch and parse all transactions in a ledger range."""
    events  = []
    marker  = None
    n_pages = 0

    while True:
        result = fetch_page(ledger_min, ledger_max, marker)
        txs    = result.get("transactions", [])
        n_pages += 1
        for tx_entry in txs:
            events.extend(parse_tx(tx_entry))
        marker = result.get("marker")
        if not marker or not txs:
            break

    return events


def collect_all(api_key=None) -> pd.DataFrame:
    out_dir         = RAW_DIR / "onchain"
    checkpoint_path = out_dir / "rlusd_xrpl_checkpoint.parquet"

    existing     = pd.DataFrame()
    start_ledger = RLUSD_LAUNCH_LEDGER

    if checkpoint_path.exists():
        existing = pd.read_parquet(checkpoint_path)
        if not existing.empty and "ledger_index" in existing.columns:
            start_ledger = int(existing["ledger_index"].max()) + 1
            print(f"  Resuming from ledger {start_ledger:,} ({len(existing):,} events in checkpoint)")

    end_ledger = get_current_ledger()
    print(f"  Collecting ledgers {start_ledger:,} → {end_ledger:,}")

    if start_ledger >= end_ledger:
        print("  Already up to date.")
        return existing

    chunk_start = start_ledger
    combined    = existing.copy()
    out_dir.mkdir(parents=True, exist_ok=True)

    while chunk_start < end_ledger:
        chunk_end = min(chunk_start + CHUNK_LEDGERS - 1, end_ledger)
        print(f"  Chunk {chunk_start:,}–{chunk_end:,} ...", end=" ", flush=True)

        events = collect_chunk(chunk_start, chunk_end)
        print(f"{len(events)} events")

        if events:
            chunk_df = pd.DataFrame(events)
            chunk_df["ledger_index"] = chunk_end
            combined = pd.concat([combined, chunk_df], ignore_index=True)
            combined = (combined
                        .drop_duplicates(subset=["tx_hash", "event_type"])
                        .sort_values("timestamp")
                        .reset_index(drop=True))
            combined.to_parquet(checkpoint_path, index=False)
            print(f"    Checkpoint: {len(combined):,} events total")

        chunk_start = chunk_end + 1

    if combined.empty:
        print("  No events found.")
    return combined


# ── Aggregation ───────────────────────────────────────────────────────────────

def aggregate_5m(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["ts_5m"] = df["timestamp"].dt.floor("5min")

    frames = []
    for event_type, prefix in [
        ("mint",     "xrpl_mint"),
        ("burn",     "xrpl_burn"),
        ("dex_sell", "xrpl_dex_sell"),
        ("dex_buy",  "xrpl_dex_buy"),
    ]:
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
    result["xrpl_net_flow_usd"]     = (result.get("xrpl_mint_volume_usd",     0)
                                        - result.get("xrpl_burn_volume_usd",     0))
    result["xrpl_dex_net_volume_usd"] = (result.get("xrpl_dex_buy_volume_usd",  0)
                                          - result.get("xrpl_dex_sell_volume_usd", 0))
    result.index.name = "timestamp"
    return result.reset_index().sort_values("timestamp")


# ── Save ──────────────────────────────────────────────────────────────────────

def save(raw: pd.DataFrame, bins: pd.DataFrame) -> None:
    out_dir = RAW_DIR / "onchain"
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_path  = out_dir / "rlusd_xrpl_events.parquet"
    bins_path = out_dir / "rlusd_xrpl_5m.parquet"

    raw.to_parquet(raw_path,  index=False)
    bins.to_parquet(bins_path, index=False)

    print(f"  Saved {raw_path.name}  ({len(raw):,} events)")
    print(f"  Saved {bins_path.name} ({len(bins):,} 5-min rows)")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== RLUSD XRPL Collector ===")
    raw  = collect_all()
    bins = aggregate_5m(raw)
    save(raw, bins)
    print("\nDone.")
