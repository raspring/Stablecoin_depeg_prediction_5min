"""
Collect macro data from FRED (Federal Reserve Economic Data).

Resolution: varies (daily/weekly/monthly). Forward-filled to 5m in merge.
Requires FRED_API_KEY environment variable.

Series collected:
    DTWEXBGS - USD index (DXY proxy, daily)
    VIXCLS    - CBOE VIX (daily)
    DGS10     - 10Y Treasury yield (daily)
    FEDFUNDS  - Fed Funds rate (monthly)
"""

import os
import time
from pathlib import Path

import pandas as pd
import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RAW_DIR, GLOBAL_START_DATE

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
RATE_LIMIT_DELAY = 0.5

SERIES = {
    "dxy": "DTWEXBGS",
    "vix": "VIXCLS",
    "t10y": "DGS10",
    "fedfunds": "FEDFUNDS",
}


def get_series(series_id: str, api_key: str, start: str = GLOBAL_START_DATE) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
    }
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    time.sleep(RATE_LIMIT_DELAY)

    data = response.json()
    obs = data.get("observations", [])

    df = pd.DataFrame(obs)[["date", "value"]]
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"]).reset_index(drop=True)
    return df


def collect_all(start: str = GLOBAL_START_DATE) -> pd.DataFrame:
    """
    Collect all FRED series and merge on date.

    Returns daily DataFrame with columns: date, dxy, vix, t10y, fedfunds
    """
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise EnvironmentError("FRED_API_KEY not set.")

    dfs = []
    for name, series_id in SERIES.items():
        print(f"  Collecting FRED {series_id} ({name})...")
        df = get_series(series_id, api_key, start=start)
        df = df.rename(columns={"value": name})
        dfs.append(df.set_index("date"))

    merged = pd.concat(dfs, axis=1).reset_index()
    merged = merged.sort_values("date").reset_index(drop=True)
    print(f"  FRED: {len(merged)} daily records ({merged['date'].min().date()} → {merged['date'].max().date()})")
    return merged


def save(df: pd.DataFrame) -> Path:
    out_dir = RAW_DIR / "fred"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "macro.parquet"
    df.to_parquet(path, index=False)
    print(f"  Saved {path}")
    return path


if __name__ == "__main__":
    df = collect_all()
    save(df)
