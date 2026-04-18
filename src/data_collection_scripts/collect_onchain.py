"""
Collect on-chain mint and burn events for stablecoins from Ethereum.

Source: Etherscan Logs API (V2)

Supported coins:
  usdt  — Issue(uint256) mints + DestroyedBlackFunds burns
            Note: Tether never calls Redeem() on-chain; redemptions are off-chain.
  usdc  — Mint(address,address,uint256) + Burn(address,uint256)
  busd  — SupplyIncreased(address,uint256) mints + SupplyDecreased(address,uint256) burns
            Paxos-issued; discontinued March 2023. Dedicated supply events (not Transfer).
  dai   — ERC20 Transfer(from=0x0,...) mints + Transfer(...,to=0x0) burns
            MakerDAO DAI has no dedicated Mint/Burn events; all supply changes
            are represented as Transfer events from/to the zero address.
  usde  — ERC20 Transfer(from=0x0,...) mints + Transfer(...,to=0x0) burns
            Ethena synthetic dollar; same Transfer-from/to-zero pattern as DAI.
  rlusd — ERC20 Transfer(from=0x0,...) mints + Transfer(...,to=0x0) burns
            Ripple USD (launched Dec 2024) uses the same Transfer pattern.

Output per coin:
  data/raw/onchain/{coin}_eth_events.parquet  — one row per event
  data/raw/onchain/{coin}_eth_5m.parquet      — 5-min aggregated bins

5-min bin columns:
  mint_count, mint_volume_usd           — new supply minted
  burn_count, burn_volume_usd           — supply destroyed
  net_flow_usd                          — mint_volume - burn_volume
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
# Coin configs
# ---------------------------------------------------------------------------
# data_slice: how to extract the uint256 amount from the log's data field
#   "full"  — data IS the uint256 (32 bytes, 66 chars with 0x prefix)
#   "last"  — data contains [other_fields | uint256]; take last 64 hex chars

_ZERO         = "0x0000000000000000000000000000000000000000000000000000000000000000"
_TRANSFER     = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# Tether treasury on Ethereum (confirmed via Etherscan label, $4.4B USDT held)
_ETH_TREASURY = "0x0000000000000000000000005754284f345afc66a98fbb0a0afe71e0f007b949"

COIN_CONFIGS = {
    "usdt": {
        "contract":     "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "decimals":     6,
        "deploy_block": 4_634_748,   # Nov 2017
        "events": [
            {
                "topic":      "0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a",
                "event_type": "mint",              # Issue(uint256 amount)
                "data_slice": "full",
            },
            {
                "topic":      "0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6",
                "event_type": "burn",              # DestroyedBlackFunds(address,uint256)
                "data_slice": "last",              # data = [address (32B) | amount (32B)]
            },
            # Treasury flows: Tether never calls Redeem() on Ethereum; institutions
            # return USDT by sending it to the treasury wallet.  Tracking these
            # Transfer events gives a direct redemption-pressure signal that
            # Issue() events alone cannot provide.
            {
                "topic":        _TRANSFER,
                "event_type":   "treasury_inflow",   # institutions → treasury (redemptions)
                "data_slice":   "full",
                "extra_topics": {"topic2": _ETH_TREASURY},
            },
            {
                "topic":        _TRANSFER,
                "event_type":   "treasury_outflow",  # treasury → institutions (re-issuance)
                "data_slice":   "full",
                "extra_topics": {"topic1": _ETH_TREASURY, "topic0_1_opr": "and"},
            },
        ],
    },
    "usdc": {
        "contract":     "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "decimals":     6,
        "deploy_block": 6_082_465,   # Sep 2018
        "events": [
            {
                "topic":      "0xab8530f87dc9b59234c4623bf917212bb2536d647574c8e7e5da92c2ede0c9f8",
                "event_type": "mint",         # Mint(address indexed, address indexed, uint256)
                "data_slice": "full",
            },
            {
                "topic":      "0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5",
                "event_type": "burn",         # Burn(address indexed, uint256)
                "data_slice": "full",
            },
        ],
    },
    "busd": {
        "contract":     "0x4Fabb145d64652a948d72533023f6E7A623C7C53",
        "decimals":     18,
        "deploy_block": 8_493_105,   # Sep 2019 (Paxos; discontinued Mar 2023)
        # Paxos uses dedicated supply events, not Transfer from/to zero.
        # Both amounts are the sole non-indexed word in data ("full" slice).
        "events": [
            {
                "topic":      "0xf5c174d57843e57fea3c649fdde37f015ef08750759cbee88060390566a98797",
                "event_type": "mint",         # SupplyIncreased(address indexed to, uint256)
                "data_slice": "full",
            },
            {
                "topic":      "0x1b7e18241beced0d7f41fbab1ea8ed468732edbcb74ec4420151654ca71c8a63",
                "event_type": "burn",         # SupplyDecreased(address indexed from, uint256)
                "data_slice": "full",
            },
        ],
    },
    "dai": {
        "contract":     "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "decimals":     18,
        "deploy_block": 8_928_158,   # Nov 2019 (Multi-Collateral DAI)
        # DAI has no dedicated Mint/Burn events.  Supply changes are
        # Transfer events from/to address(0).
        # data field = uint256 amount (single 32-byte word, no extra topics).
        "events": [
            {
                "topic":        _TRANSFER,
                "event_type":   "mint",       # Transfer(from=0x0, to=recipient, amount)
                "data_slice":   "full",
                "extra_topics": {"topic1": _ZERO, "topic0_1_opr": "and"},
            },
            {
                "topic":        _TRANSFER,
                "event_type":   "burn",       # Transfer(from=holder, to=0x0, amount)
                "data_slice":   "full",
                "extra_topics": {"topic2": _ZERO},
            },
        ],
    },
    "usde": {
        "contract":     "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
        "decimals":     18,
        "deploy_block": 18_571_358,  # Nov 2023
        # Ethena USDe has no dedicated Mint/Burn events; same Transfer pattern as DAI.
        "events": [
            {
                "topic":        _TRANSFER,
                "event_type":   "mint",       # Transfer(from=0x0, to=recipient, amount)
                "data_slice":   "full",
                "extra_topics": {"topic1": _ZERO, "topic0_1_opr": "and"},
            },
            {
                "topic":        _TRANSFER,
                "event_type":   "burn",       # Transfer(from=holder, to=0x0, amount)
                "data_slice":   "full",
                "extra_topics": {"topic2": _ZERO},
            },
        ],
    },
    "rlusd": {
        "contract":     "0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD",
        "decimals":     18,
        "deploy_block": 20_492_031,  # Aug 2024
        # Same Transfer-from/to-zero pattern as DAI.
        "events": [
            {
                "topic":        _TRANSFER,
                "event_type":   "mint",       # Transfer(from=0x0, to=recipient, amount)
                "data_slice":   "full",
                "extra_topics": {"topic1": _ZERO, "topic0_1_opr": "and"},
            },
            {
                "topic":        _TRANSFER,
                "event_type":   "burn",       # Transfer(from=holder, to=0x0, amount)
                "data_slice":   "full",
                "extra_topics": {"topic2": _ZERO},
            },
        ],
    },
}

# ---------------------------------------------------------------------------
# Etherscan API
# ---------------------------------------------------------------------------
ETHERSCAN_URL   = "https://api.etherscan.io/v2/api"
ETHERSCAN_CHAIN = 1        # Ethereum mainnet
CHUNK_BLOCKS    = 100_000  # ~2 weeks of blocks per request
MAX_RESULTS     = 1_000    # Etherscan hard limit per getLogs call
RATE_DELAY           = 0.25      # seconds between calls
CHECKPOINT_INTERVAL  = 50_000   # save checkpoint every N new events


def get_current_block(api_key: str) -> int:
    params = {"chainid": ETHERSCAN_CHAIN, "module": "proxy",
              "action": "eth_blockNumber", "apikey": api_key}
    r = requests.get(ETHERSCAN_URL, params=params, timeout=30)
    r.raise_for_status()
    time.sleep(RATE_DELAY)
    return int(r.json()["result"], 16)


def fetch_logs(contract: str, topic0: str, from_block: int,
               to_block: int, api_key: str, retries: int = 5,
               extra_topics: dict | None = None) -> list:
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
    if extra_topics:
        params.update(extra_topics)
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


def collect_event_type(contract: str, topic0: str, label: str,
                       api_key: str, start_block: int, end_block: int,
                       extra_topics: dict | None = None) -> list:
    """Paginate through all blocks with adaptive chunk sizing and retry."""
    all_logs = []
    current  = start_block
    print(f"  Collecting {label} events (blocks {start_block:,} → {end_block:,})...")

    while current <= end_block:
        chunk_end = min(current + CHUNK_BLOCKS - 1, end_block)

        while True:
            logs = fetch_logs(contract, topic0, current, chunk_end, api_key,
                              extra_topics=extra_topics)
            if len(logs) < MAX_RESULTS:
                break
            chunk_end = current + (chunk_end - current) // 2

        all_logs.extend(logs)
        current = chunk_end + 1

        if len(all_logs) > 0 and len(all_logs) % 5_000 < len(logs):
            print(f"    {len(all_logs):,} {label} events...")

    print(f"  {label}: {len(all_logs):,} total events")
    return all_logs


def collect_event_type_with_checkpoint(
    contract: str, topic0: str, label: str, api_key: str,
    start_block: int, end_block: int,
    decimals: int, data_slice: str, event_type: str,
    checkpoint_path: Path,
    extra_topics: dict | None = None,
) -> pd.DataFrame:
    """
    Fetch, parse, and checkpoint a single event type.

    Checkpoint stores parsed DataFrames. On resume, loads existing parsed rows,
    gets the last block_number, fetches only remaining raw logs, parses them,
    and concats with the checkpoint.
    """
    existing_df = pd.DataFrame()

    if checkpoint_path.exists():
        existing_df = pd.read_parquet(checkpoint_path)
        if not existing_df.empty:
            resume_block = int(existing_df["block_number"].max()) + 1
            print(f"  Resuming {label} from block {resume_block:,} ({len(existing_df):,} events in checkpoint)")
            start_block = resume_block

    if start_block > end_block:
        print(f"  {label}: already complete ({len(existing_df):,} events)")
        return existing_df

    # Fetch remaining raw logs
    raw_logs = collect_event_type(contract, topic0, label, api_key, start_block, end_block,
                                  extra_topics=extra_topics)
    new_df   = parse_logs(raw_logs, event_type, decimals, data_slice)

    result = pd.concat([existing_df, new_df], ignore_index=True).sort_values("timestamp")

    # Save checkpoint
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(checkpoint_path, index=False)
    print(f"  Checkpoint saved: {checkpoint_path.name} ({len(result):,} events)")

    return result


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_logs(logs: list, event_type: str, decimals: int, data_slice: str) -> pd.DataFrame:
    """Decode raw Etherscan log entries into a clean DataFrame."""
    if not logs:
        return pd.DataFrame(columns=["timestamp", "block_number", "tx_hash",
                                     "log_index", "event_type", "amount_usd"])
    records = []
    for log in logs:
        raw = log["data"]
        amount_raw = int(raw[-64:], 16) if data_slice == "last" else int(raw, 16)
        records.append({
            "timestamp":    datetime.fromtimestamp(int(log["timeStamp"], 16), tz=timezone.utc),
            "block_number": int(log["blockNumber"], 16),
            "tx_hash":      log["transactionHash"],
            "log_index":    int(log["logIndex"], 16) if len(log["logIndex"]) > 2 else 0,
            "event_type":   event_type,
            "amount_usd":   amount_raw / (10 ** decimals),
        })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_5m(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate raw events into 5-minute bins.

    Columns are generated dynamically from whatever event_type values are
    present: {etype}_count and {etype}_volume_usd for each type.

    Derived columns added when both halves exist:
      net_flow_usd          = mint_volume_usd - burn_volume_usd
      treasury_net_flow_usd = treasury_outflow_volume_usd - treasury_inflow_volume_usd
                              (positive = net re-issuance, negative = net redemption pressure)
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["ts_5m"] = df["timestamp"].dt.floor("5min")

    frames = []
    for etype in df["event_type"].unique():
        sub = df[df["event_type"] == etype]
        grp = sub.groupby("ts_5m").agg(
            **{
                f"{etype}_count":      ("amount_usd", "count"),
                f"{etype}_volume_usd": ("amount_usd", "sum"),
            }
        )
        frames.append(grp)

    agg = pd.concat(frames, axis=1).fillna(0)

    if {"mint_volume_usd", "burn_volume_usd"}.issubset(agg.columns):
        agg["net_flow_usd"] = agg["mint_volume_usd"] - agg["burn_volume_usd"]
    if {"treasury_inflow_volume_usd", "treasury_outflow_volume_usd"}.issubset(agg.columns):
        agg["treasury_net_flow_usd"] = (
            agg["treasury_outflow_volume_usd"] - agg["treasury_inflow_volume_usd"]
        )

    agg.index.name = "timestamp"
    return agg.reset_index().sort_values("timestamp")


# ---------------------------------------------------------------------------
# Main collection
# ---------------------------------------------------------------------------

def collect_coin(coin: str, api_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    config    = COIN_CONFIGS[coin]
    out_dir   = RAW_DIR / "onchain"
    end_block = get_current_block(api_key)
    print(f"Current Ethereum block: {end_block:,}")

    all_frames = []
    for event_cfg in config["events"]:
        checkpoint_path = out_dir / f"{coin}_{event_cfg['event_type']}_checkpoint.parquet"
        df = collect_event_type_with_checkpoint(
            contract=config["contract"],
            topic0=event_cfg["topic"],
            label=f"{coin.upper()} {event_cfg['event_type']}",
            api_key=api_key,
            start_block=config["deploy_block"],
            end_block=end_block,
            decimals=config["decimals"],
            data_slice=event_cfg["data_slice"],
            event_type=event_cfg["event_type"],
            checkpoint_path=checkpoint_path,
            extra_topics=event_cfg.get("extra_topics"),
        )
        all_frames.append(df)

    raw = pd.concat(all_frames, ignore_index=True).sort_values("timestamp").reset_index(drop=True)
    bins = aggregate_5m(raw)
    return raw, bins


def save(coin: str, raw: pd.DataFrame, bins: pd.DataFrame) -> None:
    out_dir = RAW_DIR / "onchain"
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_path  = out_dir / f"{coin}_eth_events.parquet"
    bins_path = out_dir / f"{coin}_eth_5m.parquet"

    raw.to_parquet(raw_path,  index=False)
    bins.to_parquet(bins_path, index=False)

    print(f"  Saved {raw_path}  ({len(raw):,} events)")
    print(f"  Saved {bins_path} ({len(bins):,} 5-min rows)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("coin", choices=list(COIN_CONFIGS.keys()) + ["all"],
                        help="Coin to collect, or 'all'")
    args = parser.parse_args()

    api_key = os.getenv("ETHERSCAN_API_KEY")
    if not api_key:
        raise EnvironmentError("ETHERSCAN_API_KEY not set in .env")

    coins = list(COIN_CONFIGS.keys()) if args.coin == "all" else [args.coin]

    for coin in coins:
        print(f"\n=== {coin.upper()} On-chain Events (Ethereum) ===")
        raw, bins = collect_coin(coin, api_key)
        save(coin, raw, bins)

    print("\nDone.")
