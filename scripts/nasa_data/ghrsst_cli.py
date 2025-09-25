#!/usr/bin/env python3
"""
ghrsst_cli.py
CLI wrapper for GHRSST data download using the modular HarmonyDownloader.
Maintains backward compatibility with the original script.
"""

import argparse
import datetime as dt
from pathlib import Path

from harmony_downloader import GHRSSTDownloader


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
        "--bbox",
        nargs=4,
        type=float,
        metavar=("S", "N", "W", "E"),
        help="lat/lat/lon/lon bbox (default: Vietnam region)",
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
    p.add_argument(
        "--download-dir", default="data/harmony", help="Download directory"
    )
    p.add_argument(
        "--poll-seconds", type=int, default=8, help="Polling interval for job status"
    )
    return p.parse_args()


def main():
    args = parse_args()

    try:
        downloader = GHRSSTDownloader(download_dir=args.download_dir)

        # Parse date if provided
        date = None
        if args.date:
            date = dt.date.fromisoformat(args.date)

        # Download based on level
        if args.level == "L2P":
            files = downloader.download_l2p(
                bbox=args.bbox,
                hours=args.hours,
                format_type=args.format,
                max_results=args.max_results,
                poll_seconds=args.poll_seconds,
            )
        elif args.level == "L3S":
            files = downloader.download_l3s(
                bbox=args.bbox,
                date=date,
                format_type=args.format,
                max_results=args.max_results,
                poll_seconds=args.poll_seconds,
            )
        elif args.level == "L4":
            files = downloader.download_l4(
                bbox=args.bbox,
                date=date,
                format_type=args.format,
                max_results=args.max_results,
                poll_seconds=args.poll_seconds,
            )

        print(f"\n✓ Successfully downloaded {len(files)} files")

    except Exception as e:
        raise SystemExit(f"Error: {e}")


if __name__ == "__main__":
    main()
