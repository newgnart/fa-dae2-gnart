#!/usr/bin/env python3
"""Simple CMR test script"""

import requests
import datetime as dt


def test_cmr_query():
    # Test with the GHRSST MODIS L2 concept ID from the docs
    concept_ids = {
        "MODIS_L2": "C1940473819-POCLOUD",  # From docs
        # "L3S_ACSPO": "C2805339147-POCLOUD",  # Our target
        # "L4_MUR": "C1996881146-POCLOUD"     # Alternative
    }

    end_time = dt.datetime.now(dt.timezone.utc)
    start_time = end_time - dt.timedelta(days=3)

    # Vietnam bounding box: W,S,E,N
    bbox = "102.0,8.0,112.0,22.0"

    for name, concept_id in concept_ids.items():
        print(f"\n=== Testing {name} ({concept_id}) ===")

        params = {
            "concept_id": concept_id,
            "temporal": f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')},{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}",
            "bounding_box": bbox,
            "page_size": 5,
        }

        print(f"Query URL: https://cmr.earthdata.nasa.gov/search/granules.json")
        print(f"Params: {params}")

        try:
            response = requests.get(
                "https://cmr.earthdata.nasa.gov/search/granules.json",
                params=params,
                headers={"Accept": "application/json"},
                timeout=30,
            )

            print(f"Status: {response.status_code}")
            print(f"CMR-Hits: {response.headers.get('CMR-Hits', 'Unknown')}")

            if response.status_code == 200:
                data = response.json()
                granules = data.get("feed", {}).get("entry", [])
                print(f"Granules returned: {len(granules)}")

                if granules:
                    for i, granule in enumerate(granules[:2]):
                        print(f"  {i+1}. {granule.get('title', 'No title')}")
                        print(f"     ID: {granule.get('id', 'No ID')}")
                        print(
                            f"     Time: {granule.get('time_start')} -> {granule.get('time_end')}"
                        )
            else:
                print(f"Error: {response.text}")

        except Exception as e:
            print(f"Request failed: {e}")


if __name__ == "__main__":
    test_cmr_query()
