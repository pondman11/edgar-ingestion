import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

SEC_CIK_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

USER_AGENT = (
    "edgar-ingestion/0.1 "
    "(contact: mike.lake23@yahoo.com)"
)


def snapshot_ciks(output_root: Path) -> Path:
    """
    Download the SEC company_tickers.json file and store
    a raw snapshot locally, versioned by date.

    Returns the path to the saved file.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    }

    response = requests.get(SEC_CIK_TICKERS_URL, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()

    snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = output_root / f"dt={snapshot_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "company_tickers.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return output_path


def main():
    project_root = Path(__file__).resolve().parents[3]
    output_root = project_root / "data" / "bronze" / "sec" / "company_tickers"

    output_root.mkdir(parents=True, exist_ok=True)

    print("Downloading SEC company_tickers.json...")
    output_path = snapshot_ciks(output_root)

    with open(output_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Snapshot saved to: {output_path}")
    print(f"Number of CIK entries: {len(data)}")

    # Show a small sample
    sample_keys = list(data.keys())[:5]
    print("\nSample entries:")
    for k in sample_keys:
        print(data[k])


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
