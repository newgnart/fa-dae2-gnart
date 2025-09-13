#!/usr/bin/env python3
"""
ghrsst_harmony.py
Query & download GHRSST around Vietnam via NASA Harmony using the harmony Python client.

- Auth: EDL bearer token from $EDL_TOKEN
- Levels supported: L2P (swath, fastest), L3S (daily gridded), L4 (gap-free daily)
- Region default: Vietnam-ish bbox (lat 8–22N, lon 102–112E)
- Time:
    * L2P -> rolling last 6 hours (UTC)
    * L3S/L4 -> full UTC "today" (00:00–23:59)
- Output: downloads each data link to ./outputs/<job_id>/

Usage:
  EDL_TOKEN=... python ghrsst_harmony.py --level L3S
  EDL_TOKEN=... python ghrsst_harmony.py --level L2P --bbox 7 23 100 114 --max-results 8
  EDL_TOKEN=... python ghrsst_harmony.py --level L4 --date 2025-09-12
"""

from __future__ import annotations
import argparse
import datetime as dt
import os
from pathlib import Path
import time
import typing as T
import requests

from harmony import BBox, Client, Collection, Request  # pip install harmony-py

from dotenv import load_dotenv

load_dotenv()

# ---------- Config ----------

# Known GHRSST collections (concept IDs). Replace if you have a different target.
# - L2P (Himawari AHI GHRSST swath; example concept ID; update if needed)
# - L3S (NOAA/STAR ACSPO L3S daily 0.02°)
# - L4  (JPL MUR L4 daily 0.01°)
DEFAULT_COLLECTIONS = {
    "L2P": "C2208421671-POCLOUD",  # Example; near-hourly swaths (update if needed)
    "L3S": "C2805339147-POCLOUD",  # NOAA/STAR ACSPO L3S Daily 0.02°
    "L4": "C1996881146-POCLOUD",  # JPL MUR L4 Global 0.01° Daily
}

HARMONY_BASE = "https://harmony.earthdata.nasa.gov"
DEFAULT_OUTPUT = Path("./outputs")

# ---------- Helpers ----------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch GHRSST via Harmony with level toggle."
    )
    p.add_argument(
        "--level",
        choices=["L2P", "L3S", "L4"],
        default="L3S",
        help="GHRSST processing level",
    )
    p.add_argument(
        "--collection-id",
        help="Override CMR concept ID (else uses a known default per level)",
    )
    p.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("S", "N", "W", "E"),
        default=[8.0, 22.0, 102.0, 112.0],
        help="lat/lat/lon/lon bbox",
    )
    p.add_argument(
        "--date", help="UTC date for daily products (YYYY-MM-DD). Ignored for L2P."
    )
    p.add_argument(
        "--hours", type=int, default=6, help="Rolling window hours for L2P (default 6)"
    )
    p.add_argument(
        "--format",
        default="application/x-netcdf4",
        help="Output MIME type (e.g., application/x-netcdf4 or image/tiff)",
    )
    p.add_argument(
        "--max-results", type=int, default=5, help="Limit preview result count"
    )
    p.add_argument("--outdir", default=str(DEFAULT_OUTPUT), help="Download directory")
    p.add_argument(
        "--poll-seconds", type=int, default=8, help="Polling interval for job status"
    )
    return p.parse_args()


def utc_day_bounds(date_str: str | None) -> tuple[dt.datetime, dt.datetime]:
    if date_str:
        day = dt.date.fromisoformat(date_str)
    else:
        day = dt.datetime.utcnow().date()
    start = dt.datetime.combine(day, dt.time(0, 0))
    end = dt.datetime.combine(day, dt.time(23, 59, 59))
    return start, end


def l2p_window(hours: int) -> tuple[dt.datetime, dt.datetime]:
    end = dt.datetime.utcnow()
    start = end - dt.timedelta(hours=hours)
    return start, end


def iso_z(ts: dt.datetime) -> str:
    return ts.replace(microsecond=0).isoformat() + "Z"


def ensure_token() -> str:
    token = os.getenv("EDL_TOKEN")
    if not token:
        raise SystemExit(
            "Missing EDL_TOKEN env var. Get an Earthdata Login token and set EDL_TOKEN=..."
        )
    return token


def submit_request(
    client: Client,
    collection_id: str,
    bbox: list[float],
    start: dt.datetime,
    stop: dt.datetime,
    fmt: str,
    max_results: int,
) -> str:
    req = Request(
        collection=Collection(id=collection_id),
        spatial=BBox(
            bbox[2], bbox[0], bbox[3], bbox[1]
        ),  # BBox(lonW, latS, lonE, latN)
        temporal={"start": start, "stop": stop},
        format=fmt,
        max_results=max_results,
    )
    if not req.is_valid():
        msgs = "\n".join(f"  - {m}" for m in req.error_messages())
        raise ValueError(f"Harmony Request invalid:\n{msgs}")
    return client.submit(req)


def poll_job(job_id: str, token: str, poll_seconds: int = 8) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    job_url = f"{HARMONY_BASE}/jobs/{job_id}"
    while True:
        r = requests.get(job_url, headers=headers, timeout=60)
        r.raise_for_status()
        j = r.json()
        status = j.get("status")
        progress = j.get("progress")
        print(f"[{job_id}] status={status} progress={progress}%")
        if status in {"successful", "failed", "canceled"}:
            return j
        time.sleep(poll_seconds)


def download_outputs(job_json: dict, outdir: Path, token: str) -> list[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    headers = {"Authorization": f"Bearer {token}"}
    files: list[Path] = []
    for link in job_json.get("links", []):
        if link.get("rel") == "data" and "href" in link:
            url = link["href"]
            name = link.get("title") or url.split("/")[-1]
            # ensure simple/safe filename
            fname = "".join(
                c for c in name if c.isalnum() or c in ("-", "_", ".", " ")
            ).strip()
            dest = outdir / fname
            print(f"→ downloading {url} -> {dest}")
            with requests.get(url, headers=headers, stream=True, timeout=600) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 512):
                        if chunk:
                            f.write(chunk)
            files.append(dest)
    return files


# ---------- Main ----------


def main():
    args = parse_args()
    token = ensure_token()

    collection_id = args.collection_id or DEFAULT_COLLECTIONS.get(args.level)
    if not collection_id:
        raise SystemExit(
            "No collection ID found. Use --collection-id to specify a CMR concept ID for your level."
        )

    if args.level == "L2P":
        start, stop = l2p_window(args.hours)
    else:
        start, stop = utc_day_bounds(args.date)

    print(f"Level: {args.level}")
    print(f"Collection: {collection_id}")
    print(f"BBox (S,N,W,E): {tuple(args.bbox)}")
    print(f"Time window UTC: {iso_z(start)} → {iso_z(stop)}")
    print(f"Format: {args.format} | max_results: {args.max_results}")

    client = Client(token=token)

    try:
        job_id = submit_request(
            client=client,
            collection_id=collection_id,
            bbox=args.bbox,
            start=start,
            stop=stop,
            fmt=args.format,
            max_results=args.max_results,
        )
    except Exception as e:
        raise SystemExit(f"Submit failed: {e}")

    print(f"Submitted job: {job_id}")

    job_json = poll_job(job_id, token, args.poll_seconds)

    status = job_json.get("status")
    if status != "successful":
        raise SystemExit(f"Job finished with status: {status}")

    outdir = Path(args.outdir) / job_id
    files = download_outputs(job_json, outdir, token)

    if files:
        print("\nDownloaded files:")
        for p in files:
            print(f"  - {p}")
    else:
        print("No data links found in job response.")


if __name__ == "__main__":
    main()
