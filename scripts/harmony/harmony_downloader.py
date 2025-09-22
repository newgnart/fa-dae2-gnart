#!/usr/bin/env python3
"""
harmony_downloader.py
NASA Harmony API downloader supporting multiple data collections.
"""

from __future__ import annotations
import datetime as dt
import os
from pathlib import Path
import time
import typing as T
from abc import ABC, abstractmethod
import requests

from harmony import BBox, Client, Collection, Request
from dotenv import load_dotenv

load_dotenv()


class HarmonyDownloader(ABC):
    """Base class for NASA Harmony API data downloaders."""

    def __init__(
        self, token: str | None = None, download_dir: Path | str = "data/harmony"
    ):
        self.token = token or self._get_token()
        self.download_dir = Path(download_dir)
        self.client = Client(token=self.token)
        self.harmony_base = "https://harmony.earthdata.nasa.gov"

    def _get_token(self) -> str:
        token = os.getenv("EDL_TOKEN")
        if not token:
            raise ValueError("Missing EDL_TOKEN env var. Get an Earthdata Login token.")
        return token

    @abstractmethod
    def get_collections(self) -> dict[str, str]:
        """Return mapping of collection names to CMR concept IDs."""
        pass

    @abstractmethod
    def get_default_bbox(self) -> list[float]:
        """Return default bounding box [south, north, west, east]."""
        pass

    def submit_request(
        self,
        collection_id: str,
        bbox: list[float],
        start: dt.datetime,
        stop: dt.datetime,
        format_type: str = "application/x-netcdf4",
        max_results: int = 5,
    ) -> str:
        """Submit a Harmony request and return job ID."""
        req = Request(
            collection=Collection(id=collection_id),
            spatial=BBox(
                bbox[2], bbox[0], bbox[3], bbox[1]
            ),  # BBox(lonW, latS, lonE, latN)
            temporal={"start": start, "stop": stop},
            format=format_type,
            max_results=max_results,
        )
        if not req.is_valid():
            msgs = "\n".join(f"  - {m}" for m in req.error_messages())
            raise ValueError(f"Harmony Request invalid:\n{msgs}")
        return self.client.submit(req)

    def poll_job(self, job_id: str, poll_seconds: int = 8) -> dict:
        """Poll job status until completion."""
        headers = {"Authorization": f"Bearer {self.token}"}
        job_url = f"{self.harmony_base}/jobs/{job_id}"

        while True:
            r = requests.get(job_url, headers=headers, timeout=60)
            r.raise_for_status()
            job_data = r.json()
            status = job_data.get("status")
            progress = job_data.get("progress", 0)

            print(f"[{job_id}] status={status} progress={progress}%")

            if status in {"successful", "failed", "canceled"}:
                return job_data

            time.sleep(poll_seconds)

    def download_files(self, job_data: dict, job_id: str) -> list[Path]:
        """Download all data files from completed job."""
        _download_dir = self.download_dir / job_id
        _download_dir.mkdir(parents=True, exist_ok=True)

        headers = {"Authorization": f"Bearer {self.token}"}
        files: list[Path] = []

        for link in job_data.get("links", []):
            if link.get("rel") == "data" and "href" in link:
                url = link["href"]
                name = link.get("title") or url.split("/")[-1]

                # Sanitize filename
                filename = "".join(
                    c for c in name if c.isalnum() or c in ("-", "_", ".", " ")
                ).strip()
                dest = _download_dir / filename

                print(f"→ downloading {url} -> {dest}")

                with requests.get(url, headers=headers, stream=True, timeout=600) as r:
                    r.raise_for_status()
                    with open(dest, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 512):
                            if chunk:
                                f.write(chunk)

                files.append(dest)

        return files

    def download(
        self,
        collection_name: str,
        bbox: list[float] | None = None,
        start_time: dt.datetime | None = None,
        stop_time: dt.datetime | None = None,
        format_type: str = "application/x-netcdf4",
        max_results: int = 5,
        poll_seconds: int = 8,
    ) -> list[Path]:
        """Download data for specified collection and parameters."""
        collections = self.get_collections()
        if collection_name not in collections:
            raise ValueError(
                f"Unknown collection: {collection_name}. Available: {list(collections.keys())}"
            )

        collection_id = collections[collection_name]
        bbox = bbox or self.get_default_bbox()

        if not start_time or not stop_time:
            start_time, stop_time = self._get_default_time_range(collection_name)

        print(f"Collection: {collection_name} ({collection_id})")
        print(f"BBox (S,N,W,E): {tuple(bbox)}")
        print(f"Time window UTC: {self._iso_z(start_time)} → {self._iso_z(stop_time)}")
        print(f"Format: {format_type} | max_results: {max_results}")

        try:
            job_id = self.submit_request(
                collection_id=collection_id,
                bbox=bbox,
                start=start_time,
                stop=stop_time,
                format_type=format_type,
                max_results=max_results,
            )
            print(f"Submitted job: {job_id}")

            job_data = self.poll_job(job_id, poll_seconds)

            if job_data.get("status") != "successful":
                raise RuntimeError(f"Job failed with status: {job_data.get('status')}")

            files = self.download_files(job_data, job_id)

            if files:
                print("\nDownloaded files:")
                for file_path in files:
                    print(f"  - {file_path}")
            else:
                print("No data files found in job response.")

            return files

        except Exception as e:
            raise RuntimeError(f"Download failed: {e}")

    @abstractmethod
    def _get_default_time_range(
        self, collection_name: str
    ) -> tuple[dt.datetime, dt.datetime]:
        """Get default time range for collection."""
        pass

    def _iso_z(self, timestamp: dt.datetime) -> str:
        """Format datetime as ISO string with Z suffix."""
        return timestamp.replace(microsecond=0).isoformat() + "Z"


class GHRSSTDownloader(HarmonyDownloader):
    """GHRSST sea surface temperature data downloader."""

    def get_collections(self) -> dict[str, str]:
        return {
            "L2P": "C2208421671-POCLOUD",  # Himawari AHI GHRSST swath
            "L3S": "C2805339147-POCLOUD",  # NOAA/STAR ACSPO L3S Daily 0.02°
            "L4": "C1996881146-POCLOUD",  # JPL MUR L4 Global 0.01° Daily
        }

    def get_default_bbox(self) -> list[float]:
        """Vietnam region: lat 8–22N, lon 102–112E"""
        return [8.0, 22.0, 102.0, 112.0]

    def _get_default_time_range(
        self, collection_name: str
    ) -> tuple[dt.datetime, dt.datetime]:
        """Get appropriate time range based on collection type."""
        if collection_name == "L2P":
            return self._l2p_window(hours=6)
        else:
            return self._daily_bounds()

    def _l2p_window(self, hours: int = 6) -> tuple[dt.datetime, dt.datetime]:
        """Rolling time window for L2P swath data."""
        end = dt.datetime.utcnow()
        start = end - dt.timedelta(hours=hours)
        return start, end

    def _daily_bounds(
        self, date: dt.date | None = None
    ) -> tuple[dt.datetime, dt.datetime]:
        """Full UTC day bounds for daily products."""
        day = date or (dt.datetime.utcnow() - dt.timedelta(days=1)).date()
        start = dt.datetime.combine(day, dt.time(0, 0))
        end = dt.datetime.combine(day, dt.time(23, 59, 59))
        return start, end

    def download_l2p(
        self, bbox: list[float] | None = None, hours: int = 6, **kwargs
    ) -> list[Path]:
        """Download L2P swath data for recent hours."""
        start, stop = self._l2p_window(hours)
        return self.download(
            "L2P", bbox=bbox, start_time=start, stop_time=stop, **kwargs
        )

    def download_l3s(
        self, bbox: list[float] | None = None, date: dt.date | None = None, **kwargs
    ) -> list[Path]:
        """Download L3S daily gridded data."""
        start, stop = self._daily_bounds(date)
        return self.download(
            "L3S", bbox=bbox, start_time=start, stop_time=stop, **kwargs
        )

    def download_l4(
        self, bbox: list[float] | None = None, date: dt.date | None = None, **kwargs
    ) -> list[Path]:
        """Download L4 gap-free daily data."""
        start, stop = self._daily_bounds(date)
        return self.download(
            "L4", bbox=bbox, start_time=start, stop_time=stop, **kwargs
        )
