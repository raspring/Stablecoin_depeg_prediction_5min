"""
Collect Curve Finance pool swap events as stablecoin stress indicators.

Source: Etherscan Logs API (V2)

Pools tracked:
  3pool   — DAI/USDC/USDT (0xbEbc44...)  classic StableSwap, Sep 2020
  usde    — USDe/USDC    (0x029504...) StableSwap-NG, Nov 2023
  rlusd   — RLUSD/USDC   (0xd001ae...) StableSwap-NG, Dec 2024

Signal rationale:
  In each pool a token being *sold* means the trader wants OUT of that token.
  For the 3pool, rising usdt_net_sell_volume_usd (USDT sold > USDT bought)
  is a leading indicator of USDT depeg stress — traders are fleeing USDT
  for DAI or USDC before the price moves on centralised exchanges.

Event signature hash (keccak256):
  All Curve pools (classic and NG) emit:
    TokenExchange(address,int128,uint256,int128,uint256)
    topic0 = 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140
  (NG factory pools use the same int128 event signature as classic pools;
   verified against real transaction logs on both 3pool and USDe/USDC pool)

Data field layout (both pool types):
  [32B: sold_id][32B: tokens_sold][32B: bought_id][32B: tokens_bought]
  (int128 values 0-2 are sign-extended to 256 bits; high bytes = 0)

Output per pool:
  data/raw/curve/{pool}_events.parquet  — one row per swap event
  data/raw/curve/{pool}_5m.parquet     — 5-min aggregated bins

5-min bin columns (per-token):
  {token}_sold_count, {token}_sold_volume_usd
  {token}_bought_count, {token}_bought_volume_usd
  {token}_net_sell_volume_usd   = sold_volume - bought_volume
                                  (positive = sell pressure on that token)
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
# Pool configs
# ---------------------------------------------------------------------------

# keccak256("TokenExchange(address,int128,uint256,int128,uint256)")
# Verified against real on-chain logs (3pool and USDe/USDC NG pool both use this signature).
_TOPIC_TOKEN_EXCHANGE = "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"

POOL_CONFIGS = {
    "3pool": {
        "contract":     "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
        "deploy_block": 10_809_473,   # Sep 2020
        "topic0":       _TOPIC_TOKEN_EXCHANGE,
        # tokens indexed 0..N-1; decimals used to convert raw uint256 → USD
        "tokens": {
            0: ("dai",  18),
            1: ("usdc",  6),
            2: ("usdt",  6),
        },
    },
    "usde_usdc": {
        "contract":     "0x02950460E2b9529D0E00284A5fA2D7Bdf3Fa4d72",
        "deploy_block": 18_580_701,   # Nov 2023
        "topic0":       _TOPIC_TOKEN_EXCHANGE,
        "tokens": {
            0: ("usde", 18),
            1: ("usdc",  6),
        },
    },
    "rlusd_usdc": {
        "contract":     "0xd001ae433f254283fece51d4acce8c53263aa186",
        "deploy_block": 21_436_847,   # Dec 2024
        "topic0":       _TOPIC_TOKEN_EXCHANGE,
        "tokens": {
            0: ("rlusd", 18),
            1: ("usdc",   6),
        },
    },
}

# ---------------------------------------------------------------------------
# Etherscan API (mirrors collect_onchain.py settings)
# ---------------------------------------------------------------------------

ETHERSCAN_URL   = "https://api.etherscan.io/v2/api"
ETHERSCAN_CHAIN = 1
CHUNK_BLOCKS    = 100_000
MAX_RESULTS     = 1_000
RATE_DELAY      = 0.25


def get_current_block(api_key: str) -> int:
    params = {"chainid": ETHERSCAN_CHAIN, "module": "proxy",
              "action": "eth_blockNumber", "apikey": api_key}
    r = requests.get(ETHERSCAN_URL, params=params, timeout=30)
    r.raise_for_status()
    time.sleep(RATE_DELAY)
    return int(r.json()["result"], 16)


def fetch_logs(contract: str, topic0: str, from_block: int,
               to_block: int, api_key: str, retries: int = 5) -> list:
    params = {
        "chainid":   ETHERSCAN_CHAIN,
        "module":    "logs",
        "action":    "getLogs",
        "address":   contract,
        "topic0":    topic0,
        "fromBlock": from_block,
        "toBlock":   to_block,
        "offset":    MAX_RESULTS,
        "page":      1,
        "apikey":    api_key,
    }
    for attempt in range(retries):
        try:
            r = requests.get(ETHERSCAN_URL, params=params, timeout=30)
            r.raise_for_status()
            time.sleep(RATE_DELAY)
            result = r.json()
            if result["status"] == "0":
                if "No records found" in result.get("message", ""):
                    return []
                if "timeout" in result.get("message", "").lower() or \
                   "too busy" in result.get("message", "").lower():
                    raise IOError(f"Etherscan server busy: {result['message']}")
                raise RuntimeError(f"Etherscan error: {result}")
            return result["result"]
        except (IOError, requests.RequestException) as e:
            wait = 2 ** attempt
            print(f"    Retrying in {wait}s (attempt {attempt+1}/{retries}): {e}")
            time.sleep(wait)
    raise RuntimeError(f"Failed after {retries} retries for blocks {from_block}-{to_block}")


def collect_logs(contract: str, topic0: str, label: str,
                 api_key: str, start_block: int, end_block: int) -> list:
    """Paginate with adaptive chunk sizing."""
    all_logs = []
    current  = start_block
    print(f"  Collecting {label} events (blocks {start_block:,} → {end_block:,})...")

    while current <= end_block:
        chunk_end = min(current + CHUNK_BLOCKS - 1, end_block)
        while True:
            logs = fetch_logs(contract, topic0, current, chunk_end, api_key)
            if len(logs) < MAX_RESULTS:
                break
            chunk_end = current + (chunk_end - current) // 2

        all_logs.extend(logs)
        current = chunk_end + 1

        if len(all_logs) > 0 and len(all_logs) % 10_000 < len(logs):
            print(f"    {len(all_logs):,} {label} events...")

    print(f"  {label}: {len(all_logs):,} total events")
    return all_logs


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_logs(logs: list, tokens: dict) -> pd.DataFrame:
    """
    Decode raw TokenExchange log entries.

    Data field layout (128 bytes = 256 hex chars after 0x):
      [0:64]   sold_id      — uint256 (low byte is the token index)
      [64:128] tokens_sold  — uint256 (raw amount in token's smallest unit)
      [128:192] bought_id   — uint256
      [192:256] tokens_bought — uint256
    """
    if not logs:
        return pd.DataFrame(columns=[
            "timestamp", "block_number", "tx_hash", "log_index",
            "sold_token", "sold_volume_usd", "bought_token", "bought_volume_usd",
        ])

    records = []
    for log in logs:
        raw = log["data"][2:]  # strip 0x
        if len(raw) < 256:
            continue           # malformed; skip

        sold_id      = int(raw[0:64],   16)
        tokens_sold  = int(raw[64:128], 16)
        bought_id    = int(raw[128:192], 16)
        tokens_bought = int(raw[192:256], 16)

        sold_name,   sold_dec   = tokens.get(sold_id,   ("unknown", 18))
        bought_name, bought_dec = tokens.get(bought_id, ("unknown", 18))

        records.append({
            "timestamp":        datetime.fromtimestamp(int(log["timeStamp"], 16), tz=timezone.utc),
            "block_number":     int(log["blockNumber"], 16),
            "tx_hash":          log["transactionHash"],
            "log_index":        int(log["logIndex"], 16) if len(log["logIndex"]) > 2 else 0,
            "sold_token":       sold_name,
            "sold_volume_usd":  tokens_sold  / (10 ** sold_dec),
            "bought_token":     bought_name,
            "bought_volume_usd": tokens_bought / (10 ** bought_dec),
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Checkpoint + collection
# ---------------------------------------------------------------------------

def collect_pool_with_checkpoint(pool: str, api_key: str,
                                  end_block: int) -> pd.DataFrame:
    config          = POOL_CONFIGS[pool]
    out_dir         = RAW_DIR / "curve"
    checkpoint_path = out_dir / f"{pool}_checkpoint.parquet"

    existing   = pd.DataFrame()
    start_block = config["deploy_block"]

    if checkpoint_path.exists():
        existing = pd.read_parquet(checkpoint_path)
        if not existing.empty:
            resume_block = int(existing["block_number"].max()) + 1
            print(f"  Resuming {pool} from block {resume_block:,} "
                  f"({len(existing):,} events in checkpoint)")
            start_block = resume_block

    if start_block > end_block:
        print(f"  {pool}: already complete ({len(existing):,} events)")
        return existing

    raw_logs = collect_logs(
        contract=config["contract"],
        topic0=config["topic0"],
        label=pool,
        api_key=api_key,
        start_block=start_block,
        end_block=end_block,
    )
    new_df = parse_logs(raw_logs, config["tokens"])

    result = (pd.concat([existing, new_df], ignore_index=True)
              .sort_values("timestamp")
              .reset_index(drop=True))

    out_dir.mkdir(parents=True, exist_ok=True)
    result.to_parquet(checkpoint_path, index=False)
    print(f"  Checkpoint saved: {checkpoint_path.name} ({len(result):,} events)")
    return result


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_5m(df: pd.DataFrame, tokens: dict) -> pd.DataFrame:
    """
    Aggregate raw swap events into 5-minute bins.

    For each token in the pool, produce:
      {token}_sold_count, {token}_sold_volume_usd
      {token}_bought_count, {token}_bought_volume_usd
      {token}_net_sell_volume_usd  (sold - bought; positive = sell pressure)
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["ts_5m"] = df["timestamp"].dt.floor("5min")

    token_names = [name for name, _ in tokens.values()]
    frames = []

    for token in token_names:
        sold   = df[df["sold_token"]   == token].groupby("ts_5m").agg(
            **{f"{token}_sold_count":      ("sold_volume_usd",   "count"),
               f"{token}_sold_volume_usd": ("sold_volume_usd",   "sum")})
        bought = df[df["bought_token"] == token].groupby("ts_5m").agg(
            **{f"{token}_bought_count":      ("bought_volume_usd", "count"),
               f"{token}_bought_volume_usd": ("bought_volume_usd", "sum")})
        combined = sold.join(bought, how="outer").fillna(0)
        combined[f"{token}_net_sell_volume_usd"] = (
            combined[f"{token}_sold_volume_usd"]
            - combined[f"{token}_bought_volume_usd"]
        )
        frames.append(combined)

    agg = pd.concat(frames, axis=1).fillna(0)
    agg.index.name = "timestamp"
    return agg.reset_index().sort_values("timestamp")


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def save(pool: str, raw: pd.DataFrame, bins: pd.DataFrame) -> None:
    out_dir = RAW_DIR / "curve"
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_path  = out_dir / f"{pool}_events.parquet"
    bins_path = out_dir / f"{pool}_5m.parquet"

    raw.to_parquet(raw_path,  index=False)
    bins.to_parquet(bins_path, index=False)

    print(f"  Saved {raw_path}  ({len(raw):,} events)")
    print(f"  Saved {bins_path} ({len(bins):,} 5-min rows)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def collect_pool(pool: str, api_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    end_block = get_current_block(api_key)
    print(f"Current Ethereum block: {end_block:,}")
    raw  = collect_pool_with_checkpoint(pool, api_key, end_block)
    bins = aggregate_5m(raw, POOL_CONFIGS[pool]["tokens"])
    return raw, bins


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("pool", choices=list(POOL_CONFIGS.keys()) + ["all"],
                        help="Pool to collect, or 'all'")
    args = parser.parse_args()

    api_key = os.getenv("ETHERSCAN_API_KEY")
    if not api_key:
        raise EnvironmentError("ETHERSCAN_API_KEY not set in .env")

    pools = list(POOL_CONFIGS.keys()) if args.pool == "all" else [args.pool]

    for pool in pools:
        print(f"\n=== Curve {pool} TokenExchange Events ===")
        raw, bins = collect_pool(pool, api_key)
        save(pool, raw, bins)

    print("\nDone.")
