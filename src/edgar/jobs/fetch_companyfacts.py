import json
import sys
import time
import tempfile
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from tqdm import tqdm

# Official SEC file that maps CIK<->ticker/name
CIK_SNAPSHOT_RELATIVE = Path("data/bronze/sec/company_tickers")

# Official SEC companyfacts endpoint (extracted XBRL)
# CIK must be 10-digit, zero-padded.
COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# REQUIRED by SEC: identify your app + contact.
USER_AGENT = "edgar-ingestion/0.1 (contact: mike.lake23@yahoo.com)"

# Polite client defaults (SEC guidance: don't be aggressive)
REQUEST_TIMEOUT_SECS = 60
MAX_RETRIES = 5
BASE_BACKOFF_SECS = 1.0

# Requests-per-second (keep <= 10; lower is safer for hobby runs)
RPS = 2.0  # 2 req/sec => 0.5s between requests


@dataclass(frozen=True)
class RunConfig:
    limit: Optional[int] = None          # max CIKs this run
    sleep_secs: float = 1.0 / RPS        # throttle
    overwrite: bool = False              # refetch even if file exists


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def zero_pad_cik(cik: int) -> str:
    return str(cik).zfill(10)


def latest_snapshot_file(snapshot_root: Path) -> Path:
    """
    Find latest dt=YYYY-MM-DD folder and return company_tickers.json inside it.
    """
    if not snapshot_root.exists():
        raise FileNotFoundError(f"Snapshot root not found: {snapshot_root}")

    dt_dirs = [p for p in snapshot_root.iterdir() if p.is_dir() and p.name.startswith("dt=")]
    if not dt_dirs:
        raise FileNotFoundError(f"No dt=* snapshots found under: {snapshot_root}")

    # ISO date sorts lexicographically
    latest = sorted(dt_dirs, key=lambda p: p.name)[-1]
    f = latest / "company_tickers.json"
    if not f.exists():
        raise FileNotFoundError(f"Snapshot file missing: {f}")
    return f


def load_ciks(snapshot_file: Path) -> List[int]:
    """
    snapshot JSON is keyed by numeric strings; values contain 'cik_str'
    """
    with open(snapshot_file, "r", encoding="utf-8") as fp:
        data = json.load(fp)

    ciks: List[int] = []
    for _, row in data.items():
        cik = row["cik_str"]
        if isinstance(cik, int):
            ciks.append(cik)
    # de-dup + stable ordering
    return sorted(set(ciks))


def load_state(state_path: Path) -> Dict[str, dict]:
    if state_path.exists():
        with open(state_path, "r", encoding="utf-8") as fp:
            return json.load(fp)
    return {
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
        "items": {}  # cik10 -> {"status": "...", "updated_at": "...", "bytes": int, "error": str}
    }


def save_state(state_path: Path, state: Dict[str, dict]) -> None:
    state["updated_at"] = utc_now_iso()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as fp:
        json.dump(state, fp, indent=2)


def request_with_retries(url: str, headers: Dict[str, str]) -> Tuple[int, bytes]:
    """
    Returns (status_code, content_bytes).
    Raises after MAX_RETRIES.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECS)
            # If 429/503 etc., treat as retryable
            if resp.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Retryable HTTP {resp.status_code}")
            return resp.status_code, resp.content
        except Exception as exc:
            last_exc = exc
            backoff = BASE_BACKOFF_SECS * (2 ** (attempt - 1))
            time.sleep(backoff)

    raise RuntimeError(f"Failed after {MAX_RETRIES} retries: {last_exc}")


def write_bytes_atomic(path: Path, content: bytes) -> int:
    """
    Write to temp then rename to avoid partial files.
    Returns bytes written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=path.name + ".",
        suffix=".tmp"
    )
    tmp = Path(tmp_name)

    try: 
        with os.fdopen(fd,"wb") as f: 
            f.write(content)
            f.flush
            os.fsync(f.fileno())
        
        tmp.replace(path)
        return len(content)
    finally: 

        if tmp.exists() and tmp!= path: 
            try: 
                tmp.unlink()
            except OSError:
                pass



def iter_pending_ciks(
    ciks: Iterable[int],
    output_root: Path,
    state: Dict[str, dict],
    overwrite: bool
) -> Iterable[str]:
    """
    Yields cik10 for CIKs that should be fetched this run.
    """
    items = state.get("items", {})
    for cik in ciks:
        cik10 = zero_pad_cik(cik)
        out_path = output_root / f"cik={cik10}" / "companyfacts.json"

        if overwrite:
            yield cik10
            continue

        if out_path.exists():
            # Mark as success if file exists and state doesn't say otherwise
            if cik10 not in items or items[cik10].get("status") != "success":
                items[cik10] = {"status": "success", "updated_at": utc_now_iso(), "bytes": out_path.stat().st_size}
            continue

        # If previously failed, we still want to retry (you can change policy later)
        yield cik10

    state["items"] = items


def fetch_companyfacts(run: RunConfig) -> None:
    project_root = Path(__file__).resolve().parents[3]
    snapshot_root = project_root / CIK_SNAPSHOT_RELATIVE
    snapshot_file = latest_snapshot_file(snapshot_root)

    output_root = project_root / "data" / "bronze" / "sec" / "companyfacts"
    state_path = project_root / "data" / "state" / "companyfacts_state.json"

    state = load_state(state_path)

    ciks = load_ciks(snapshot_file)
    if run.limit is not None:
        ciks = ciks[: run.limit]

    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
    }

    pending = list(iter_pending_ciks(ciks, output_root, state, run.overwrite))
    save_state(state_path, state)

    print(f"Using CIK snapshot: {snapshot_file}")
    print(f"Total CIKs in scope: {len(ciks)}")
    print(f"Pending to fetch: {len(pending)}")
    print(f"Output root: {output_root}")
    print(f"State file: {state_path}")

    items = state["items"]

    for cik10 in tqdm(pending, desc="Fetching companyfacts"):
        url = COMPANYFACTS_URL.format(cik=cik10)
        out_path = output_root / f"cik={cik10}" / "companyfacts.json"

        # throttle between requests
        time.sleep(run.sleep_secs)

        try:
            status, content = request_with_retries(url, headers=headers)
            if status != 200:
                items[cik10] = {"status": f"http_{status}", "updated_at": utc_now_iso(), "bytes": 0, "error": f"HTTP {status}"}
                save_state(state_path, state)
                continue

            nbytes = write_bytes_atomic(out_path, content)
            items[cik10] = {"status": "success", "updated_at": utc_now_iso(), "bytes": nbytes}

        except Exception as exc:
            items[cik10] = {"status": "error", "updated_at": utc_now_iso(), "bytes": 0, "error": str(exc)}
        finally:
            # persist progress frequently so resume is painless
            save_state(state_path, state)


def parse_args(argv: List[str]) -> RunConfig:
    """
    Very small CLI parser:
      --limit 200
      --overwrite
      --rps 2
    """
    limit = None
    overwrite = False
    rps = RPS

    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--limit":
            i += 1
            limit = int(argv[i])
        elif a == "--overwrite":
            overwrite = True
        elif a == "--rps":
            i += 1
            rps = float(argv[i])
        else:
            raise ValueError(f"Unknown arg: {a}")
        i += 1

    sleep_secs = 1.0 / max(rps, 0.1)
    return RunConfig(limit=limit, sleep_secs=sleep_secs, overwrite=overwrite)


def main():
    cfg = parse_args(sys.argv[1:])
    fetch_companyfacts(cfg)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
