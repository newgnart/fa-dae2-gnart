#!/usr/bin/env python3
"""
EONET Vietnam Events Fetcher

Fetches natural disaster events in Vietnam region using NASA EONET API.
Vietnam bounding box: 102°E to 112°E longitude, 8°N to 22°N latitude
"""

import requests
import json
from datetime import datetime
from typing import Dict, List, Optional


class EONETVietnamFetcher:
    """Fetches EONET events for Vietnam region"""

    BASE_URL = "https://eonet.gsfc.nasa.gov/api/v3"
    # Vietnam bounding box: min_lon, max_lat, max_lon, min_lat
    VIETNAM_BBOX = "102,22,119,8"
    # 21.087608971732184, 112.23815919272472

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Vietnam-EONET-Fetcher/1.0"})

    def get_events(
        self,
        format_type: str = "json",
        category: Optional[str] = None,
        status: str = "open",
        limit: Optional[int] = None,
        days: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict:
        """
        Fetch events in Vietnam region

        Args:
            format_type: "json" or "geojson"
            category: Event category (e.g., "wildfires", "severeStorms")
            status: "open", "closed", or "all"
            limit: Maximum number of events to return
            days: Number of prior days to include
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Dict containing API response
        """
        if format_type == "geojson":
            url = f"{self.BASE_URL}/events/geojson"
        else:
            url = f"{self.BASE_URL}/events"

        params = {"bbox": self.VIETNAM_BBOX, "status": status}

        if category:
            params["category"] = category
        if limit:
            params["limit"] = limit
        if days:
            params["days"] = days
        if start_date:
            params["start"] = start_date
        if end_date:
            params["end"] = end_date

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching events: {e}")
            return {}

    def get_categories(self) -> Dict:
        """Get all available event categories"""
        url = f"{self.BASE_URL}/categories"
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching categories: {e}")
            return {}

    def print_events_summary(self, events_data: Dict) -> None:
        """Print a summary of events"""
        if not events_data:
            print("No events data to display")
            return

        if "events" in events_data:
            events = events_data["events"]
        elif "features" in events_data:  # GeoJSON format
            events = events_data["features"]
        else:
            print("Unexpected data format")
            return

        print(f"\n=== EONET Events in Vietnam Region ===")
        print(f"Total events found: {len(events)}")
        print(f"Retrieved at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 50)

        for i, event in enumerate(events, 1):
            if "properties" in event:  # GeoJSON format
                title = event["properties"]["title"]
                event_id = event["properties"]["id"]
                categories = [cat["title"] for cat in event["properties"]["categories"]]
                closed = event["properties"].get("closed")
            else:  # Regular JSON format
                title = event["title"]
                event_id = event["id"]
                categories = [cat["title"] for cat in event["categories"]]
                closed = event.get("closed")

            status = "CLOSED" if closed else "OPEN"
            categories_str = ", ".join(categories)

            print(f"{i}. {title}")
            print(f"   ID: {event_id}")
            print(f"   Categories: {categories_str}")
            print(f"   Status: {status}")
            if closed:
                print(f"   Closed: {closed}")
            print()


class EONETEventFetcher:
    """Fetches individual EONET events by ID"""

    BASE_URL = "https://eonet.gsfc.nasa.gov/api/v3"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "EONET-Event-Fetcher/1.0"})

    def get_event_by_id(self, event_id: str) -> Dict:
        """
        Fetch a specific event by its ID

        Args:
            event_id: EONET event ID (e.g., "EONET_15545")

        Returns:
            Dict containing event details
        """
        url = f"{self.BASE_URL}/events/{event_id}"

        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching event {event_id}: {e}")
            return {}

    def print_event_details(self, event_data: Dict) -> None:
        """Print detailed information about a single event"""
        if not event_data:
            print("No event data to display")
            return

        print(f"\n=== EONET Event Details ===")
        print(f"ID: {event_data.get('id', 'N/A')}")
        print(f"Title: {event_data.get('title', 'N/A')}")
        print(f"Description: {event_data.get('description', 'N/A')}")
        print(f"Link: {event_data.get('link', 'N/A')}")

        # Status
        closed = event_data.get("closed")
        status = "CLOSED" if closed else "OPEN"
        print(f"Status: {status}")
        if closed:
            print(f"Closed Date: {closed}")

        # Categories
        categories = event_data.get("categories", [])
        if categories:
            print(f"Categories: {', '.join([cat['title'] for cat in categories])}")

        # Magnitude (if available)
        mag_value = event_data.get("magnitudeValue")
        mag_unit = event_data.get("magnitudeUnit")
        if mag_value and mag_unit:
            print(f"Magnitude: {mag_value} {mag_unit}")

        # Sources
        sources = event_data.get("sources", [])
        if sources:
            print(f"Sources:")
            for source in sources:
                print(f"  - {source.get('id', 'N/A')}: {source.get('url', 'N/A')}")

        # Geometry points
        geometries = event_data.get("geometry", [])
        if geometries:
            print(f"\nGeometry Points ({len(geometries)} total):")
            for i, geom in enumerate(geometries[:5]):  # Show first 5
                date = geom.get("date", "N/A")
                geom_type = geom.get("type", "N/A")
                coordinates = geom.get("coordinates", [])
                if coordinates:
                    if geom_type == "Point":
                        lon, lat = coordinates
                        print(f"  {i+1}. {date} - Point: {lat:.3f}°N, {lon:.3f}°E")
                    else:
                        print(
                            f"  {i+1}. {date} - {geom_type}: {len(coordinates)} coordinates"
                        )
            if len(geometries) > 5:
                print(f"  ... and {len(geometries) - 5} more points")

        print("-" * 50)


def main():
    """Main function with examples"""
    fetcher = EONETVietnamFetcher()

    print("Fetching EONET events in Vietnam region...")

    # Example 1: Get all open events in Vietnam
    print("\n1. All open events in Vietnam:")
    events = fetcher.get_events(limit=10)
    fetcher.print_events_summary(events)

    # Example 2: Get severe storms in last 30 days
    print("\n2. Severe storms in last 30 days:")
    storms = fetcher.get_events(category="severeStorms", days=30, status="all")
    fetcher.print_events_summary(storms)

    # Example 3: Get events as GeoJSON
    print("\n3. Recent events as GeoJSON:")
    geojson_events = fetcher.get_events(format_type="geojson", limit=5)
    if geojson_events:
        print(f"GeoJSON format - Type: {geojson_events.get('type')}")
        print(f"Features count: {len(geojson_events.get('features', []))}")

    # Example 4: Show available categories
    print("\n4. Available event categories:")
    categories = fetcher.get_categories()
    if "categories" in categories:
        for cat in categories["categories"]:
            print(f"   - {cat['id']}: {cat['title']}")


if __name__ == "__main__":
    main()
    event_fetcher = EONETEventFetcher()
    event = event_fetcher.get_event_by_id("EONET_15545")
    event_fetcher.print_event_details(event)
