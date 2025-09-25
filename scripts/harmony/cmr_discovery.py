#!/usr/bin/env python3
"""
cmr_discovery.py
NASA CMR (Common Metadata Repository) discovery for available GHRSST data.
"""

import datetime as dt
import requests
from typing import List, Dict, Optional
from pathlib import Path
import json

from harmony_downloader import GHRSSTDownloader


class CMRDiscovery:
    """NASA CMR metadata discovery for GHRSST collections."""

    def __init__(self):
        self.cmr_base_url = "https://cmr.earthdata.nasa.gov/search"

    def discover_available_granules(
        self,
        collection_id: str,
        days_back: int = 7,
        bbox: Optional[List[float]] = None
    ) -> List[Dict]:
        """Query CMR to discover available granules for a collection."""

        end_time = dt.datetime.now(dt.timezone.utc)
        start_time = end_time - dt.timedelta(days=days_back)

        params = {
            'concept_id': collection_id,
            'temporal': f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')},{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}",
            'page_size': 100,
            'sort_key': '-start_date'  # Most recent first
        }

        # Add spatial filter if provided
        if bbox:
            # CMR expects: west,south,east,north
            params['bounding_box'] = f"{bbox[2]},{bbox[0]},{bbox[3]},{bbox[1]}"

        try:
            response = requests.get(
                f"{self.cmr_base_url}/granules.json",
                params=params,
                timeout=30
            )
            response.raise_for_status()

            granules_data = response.json()
            granules = granules_data.get('feed', {}).get('entry', [])

            return self._parse_granules(granules)

        except requests.RequestException as e:
            raise RuntimeError(f"CMR discovery failed: {e}")

    def _parse_granules(self, granules: List[Dict]) -> List[Dict]:
        """Parse CMR granule response into useful metadata."""
        parsed_granules = []

        for granule in granules:
            try:
                parsed = {
                    'granule_id': granule.get('id'),
                    'title': granule.get('title'),
                    'start_time': granule.get('time_start'),
                    'end_time': granule.get('time_end'),
                    'update_time': granule.get('updated'),
                    'size_mb': self._extract_size(granule),
                    'download_url': self._extract_download_url(granule)
                }
                parsed_granules.append(parsed)
            except Exception as e:
                print(f"Warning: Failed to parse granule {granule.get('id', 'unknown')}: {e}")
                continue

        return parsed_granules

    def _extract_size(self, granule: Dict) -> Optional[float]:
        """Extract file size in MB from granule metadata."""
        try:
            # CMR stores size in different places depending on provider
            size_bytes = None

            # Try granule level
            if 'granule_size' in granule:
                size_bytes = float(granule['granule_size'])

            # Try in links
            for link in granule.get('links', []):
                if 'length' in link:
                    size_bytes = float(link['length'])
                    break

            return size_bytes / (1024 * 1024) if size_bytes else None

        except (ValueError, KeyError):
            return None

    def _extract_download_url(self, granule: Dict) -> Optional[str]:
        """Extract direct download URL if available."""
        for link in granule.get('links', []):
            if link.get('rel') == 'http://esipfed.org/ns/fedsearch/1.1/data#':
                return link.get('href')
        return None


class StreamingGHRSSTDownloader(GHRSSTDownloader):
    """GHRSST downloader with CMR discovery capabilities."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cmr = CMRDiscovery()
        self.state_file = Path("cmr_download_state.json")

    def discover_and_download(
        self,
        collection_name: str,
        days_back: int = 7,
        max_granules: int = 5
    ) -> List[Path]:
        """Discover available data via CMR and download the most recent."""

        collection_id = self.get_collections()[collection_name]
        bbox = self.get_default_bbox()

        print(f"Discovering available {collection_name} data from last {days_back} days...")

        # Discover available granules
        granules = self.cmr.discover_available_granules(
            collection_id=collection_id,
            days_back=days_back,
            bbox=bbox
        )

        if not granules:
            print("No recent data found")
            return []

        print(f"Found {len(granules)} available granules")

        # Show discovered data
        for i, granule in enumerate(granules[:5]):
            print(f"  {i+1}. {granule['title'][:60]}...")
            print(f"     Time: {granule['start_time']} -> {granule['end_time']}")
            if granule['size_mb']:
                print(f"     Size: {granule['size_mb']:.1f} MB")

        # Download most recent granules
        downloaded_files = []
        processed_granules = granules[:max_granules]

        for granule in processed_granules:
            try:
                start_time = dt.datetime.fromisoformat(granule['start_time'].replace('Z', '+00:00'))
                end_time = dt.datetime.fromisoformat(granule['end_time'].replace('Z', '+00:00'))

                files = self.download(
                    collection_name=collection_name,
                    bbox=bbox,
                    start_time=start_time,
                    stop_time=end_time,
                    max_results=1
                )

                downloaded_files.extend(files)

            except Exception as e:
                print(f"Failed to download granule {granule['granule_id']}: {e}")
                continue

        # Save state for incremental processing
        self._save_download_state(collection_name, granules[0] if granules else None)

        return downloaded_files

    def download_since_last_run(self, collection_name: str) -> List[Path]:
        """Download only new data since last run using CMR discovery."""

        last_state = self._load_download_state(collection_name)
        last_update_time = last_state.get('last_update_time') if last_state else None

        collection_id = self.get_collections()[collection_name]
        bbox = self.get_default_bbox()

        # Discover recent granules
        granules = self.cmr.discover_available_granules(
            collection_id=collection_id,
            days_back=3,  # Check last 3 days
            bbox=bbox
        )

        # Filter to only new granules
        if last_update_time:
            last_dt = dt.datetime.fromisoformat(last_update_time.replace('Z', '+00:00'))
            new_granules = [
                g for g in granules
                if dt.datetime.fromisoformat(g['update_time'].replace('Z', '+00:00')) > last_dt
            ]
        else:
            new_granules = granules[:1]  # First run, get just the latest

        if not new_granules:
            print("No new data since last run")
            return []

        print(f"Found {len(new_granules)} new granules since last run")

        # Download new granules
        downloaded_files = []
        for granule in new_granules:
            try:
                start_time = dt.datetime.fromisoformat(granule['start_time'].replace('Z', '+00:00'))
                end_time = dt.datetime.fromisoformat(granule['end_time'].replace('Z', '+00:00'))

                files = self.download(
                    collection_name=collection_name,
                    bbox=bbox,
                    start_time=start_time,
                    stop_time=end_time,
                    max_results=1
                )

                downloaded_files.extend(files)

            except Exception as e:
                print(f"Failed to download granule {granule['granule_id']}: {e}")
                continue

        # Update state
        if new_granules:
            self._save_download_state(collection_name, new_granules[0])

        return downloaded_files

    def _load_download_state(self, collection_name: str) -> Optional[Dict]:
        """Load last download state for incremental processing."""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    return state.get(collection_name)
        except Exception:
            pass
        return None

    def _save_download_state(self, collection_name: str, latest_granule: Optional[Dict]):
        """Save download state for incremental processing."""
        try:
            # Load existing state
            state = {}
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)

            # Update state for this collection
            if latest_granule:
                state[collection_name] = {
                    'last_granule_id': latest_granule['granule_id'],
                    'last_update_time': latest_granule['update_time'],
                    'last_run': dt.datetime.now(dt.timezone.utc).isoformat() + 'Z'
                }

            # Save state
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)

        except Exception as e:
            print(f"Warning: Failed to save download state: {e}")


if __name__ == "__main__":
    # Example usage
    downloader = StreamingGHRSSTDownloader()

    # Discover and download latest L3S data
    print("=== Discovering latest L3S data ===")
    files = downloader.discover_and_download("L3S", days_back=7, max_granules=3)
    print(f"Downloaded {len(files)} files")

    # Subsequent runs - only download new data
    print("\n=== Checking for new data since last run ===")
    new_files = downloader.download_since_last_run("L3S")
    print(f"Downloaded {len(new_files)} new files")