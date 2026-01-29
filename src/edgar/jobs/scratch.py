import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests 
from tqdm import tqdm

# Relative path of file that maps CIK to Ticker
CIK_SNAPSHOT_RELATIVE = Path("data/bronze/sec_company_tickers")

COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}"

USER_AGENT = 'edgar-ingestion/0.1 (contact: mike.lake23@yahoo.com)'

# Polite client defaults
REQUEST_TIMEOUT_SECS = 60
MAX_RETRIES = 5
BASE_BACKOFF_SECS = 1.0

# Requests per second 
RPS = 2.0

@dataclass(frozen=True) #@dataclass creates boiler plate for _init_, etc
class RunConfig: 
    limit: Optional[int] = None
    sleep_secs: float = 1.0 / RPS
    overwrite: bool = False 

def utc_now_iso() -> str: 
    return datetime.now(timezone.utc).isoformat()

def zero_pad_cik(cik: int) -> str:
    return str(cik).zfill(10)

def latest_snapshot_file(snapshot_root: Path) -> Path: 

    if not snapshot_root.exists():
        raise FileNotFoundError(f"Snapshot root not found: {snapshot_root}")
    
    dt_dirs = [p for p in snapshot_root.iterdir() if p.is_dir() and p.name.startswith("dt=")]

    if not dt_dirs: 
        raise FileNotFoundError(f"No dt=* snapshots found under: {snapshot_root}")
    
    latest = sorted(dt_dirs, key = lambda p: p.name)[-1]
    f = latest / "company_tickers.json"
    if not f.exists(): 
        raise FileNotFoundError(f"Snapshot file missing: {f}")
    
    return f

def fetch_companyfacts(run: RunConfig) -> None: 
    project_root = Path(__file__).resolve().parents[3]
    snapshot_root = project_root / CIK_SNAPSHOT_RELATIVE
    snapshot_file = latest_snapshot_file(snapshot_root)

    output_root = project_root / "data" / "bronze" / "sec" / "companyfacts"
    state_path = project_root / "data" / "bronze" / "companyfacts_state.json"

    state = load_state(state_path)
