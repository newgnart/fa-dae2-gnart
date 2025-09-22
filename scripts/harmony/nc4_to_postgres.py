#!/usr/bin/env python3
"""
Load GHRSST .nc4 files directly into PostgreSQL staging table
"""

import os
import json
import sys
from pathlib import Path
from datetime import datetime

import xarray as xr
import psycopg
import numpy as np
from dotenv import load_dotenv


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types."""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)

# Load environment variables
load_dotenv()


def get_connection():
    """Get database connection using existing connection pattern."""
    params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }
    return psycopg.connect(**params)


def extract_nc4_metadata(ds, file_path):
    """Extract metadata and summary from NC4 dataset."""
    metadata = {
        "file_path": str(file_path),
        "dimensions": dict(ds.sizes),
        "coordinates": {coord: str(ds[coord].dims) for coord in ds.coords},
        "data_variables": {},
        "global_attributes": dict(ds.attrs),
        "spatial_bounds": {},
        "temporal_bounds": {},
        "data_summary": {}
    }

    # Extract data variable info
    for var_name in ds.data_vars:
        var = ds[var_name]
        metadata["data_variables"][var_name] = {
            "dimensions": list(var.dims),
            "shape": list(var.shape),
            "dtype": str(var.dtype),
            "attributes": dict(var.attrs)
        }

    # Extract spatial bounds
    if 'lat' in ds.coords and 'lon' in ds.coords:
        metadata["spatial_bounds"] = {
            "lat_min": float(ds.lat.min()),
            "lat_max": float(ds.lat.max()),
            "lon_min": float(ds.lon.min()),
            "lon_max": float(ds.lon.max())
        }

    # Extract temporal bounds
    if 'time' in ds.coords:
        metadata["temporal_bounds"] = {
            "time_min": str(ds.time.min().values),
            "time_max": str(ds.time.max().values),
            "time_count": int(ds.sizes['time'])
        }

    # Extract data summaries for key variables
    for var_name in ['analysed_sst', 'sea_ice_fraction', 'sst_anomaly']:
        if var_name in ds.data_vars:
            var_data = ds[var_name]
            # Only compute stats for non-NaN values
            valid_data = var_data.where(var_data.notnull())
            if valid_data.count() > 0:
                metadata["data_summary"][var_name] = {
                    "min": float(valid_data.min()),
                    "max": float(valid_data.max()),
                    "mean": float(valid_data.mean()),
                    "valid_points": int(valid_data.count())
                }

    return metadata


def extract_sample_data(ds, sample_size=1000):
    """Extract a sample of actual data points for analysis."""
    sample_data = []

    # Get coordinates
    if 'time' in ds.coords and 'lat' in ds.coords and 'lon' in ds.coords:
        time_vals = ds.time.values
        lat_vals = ds.lat.values
        lon_vals = ds.lon.values

        # Sample indices
        lat_indices = np.linspace(0, len(lat_vals)-1, min(sample_size//10, len(lat_vals)), dtype=int)
        lon_indices = np.linspace(0, len(lon_vals)-1, min(sample_size//10, len(lon_vals)), dtype=int)

        for t_idx, time_val in enumerate(time_vals):
            for lat_idx in lat_indices[:5]:  # Limit to 5 lat points
                for lon_idx in lon_indices[:5]:  # Limit to 5 lon points
                    row = {
                        "time": str(time_val),
                        "lat": float(lat_vals[lat_idx]),
                        "lon": float(lon_vals[lon_idx])
                    }

                    # Add data variables
                    for var_name in ds.data_vars:
                        try:
                            val = ds[var_name].isel(time=t_idx, lat=lat_idx, lon=lon_idx).values
                            if not np.isnan(val):
                                row[var_name] = float(val)
                        except:
                            continue

                    sample_data.append(row)

                    if len(sample_data) >= sample_size:
                        break
                if len(sample_data) >= sample_size:
                    break
            if len(sample_data) >= sample_size:
                break

    return sample_data


def load_nc4_to_postgres(nc4_file_path, load_sample_data=True):
    """
    Load NC4 file metadata and optionally sample data into PostgreSQL.

    Args:
        nc4_file_path: Path to the .nc4 file
        load_sample_data: Whether to include sample data points
    """

    nc4_path = Path(nc4_file_path)
    if not nc4_path.exists():
        raise FileNotFoundError(f"NC4 file not found: {nc4_path}")

    print(f"Loading NC4 file: {nc4_path}")

    # Open the dataset
    ds = xr.open_dataset(nc4_path)

    try:
        # Extract metadata
        print("Extracting metadata...")
        metadata = extract_nc4_metadata(ds, nc4_path)

        # Extract sample data if requested
        sample_data = []
        if load_sample_data:
            print("Extracting sample data...")
            sample_data = extract_sample_data(ds)

        # Prepare data content for database
        data_content = {
            "metadata": metadata,
            "sample_data": sample_data,
            "processing_info": {
                "processed_at": datetime.now().isoformat(),
                "sample_size": len(sample_data),
                "file_size_mb": nc4_path.stat().st_size / (1024 * 1024)
            }
        }

        # Load into database
        print("Loading to PostgreSQL...")
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO staging.ghrsst_raw (data_content, file_name) VALUES (%s, %s) RETURNING id",
                    (json.dumps(data_content, cls=NumpyEncoder), nc4_path.name)
                )
                record_id = cur.fetchone()[0]

                print(f"✅ Loaded NC4 data to database record ID: {record_id}")
                print(f"   File: {nc4_path.name}")
                print(f"   Dimensions: {metadata['dimensions']}")
                print(f"   Variables: {list(metadata['data_variables'].keys())}")
                print(f"   Sample data points: {len(sample_data)}")

                return record_id

    finally:
        ds.close()


def load_all_nc4_files(search_dir=".", load_sample_data=True):
    """Load all .nc4 files found in directory."""
    nc4_files = list(Path(search_dir).rglob("*.nc4"))

    if not nc4_files:
        print("No .nc4 files found")
        return []

    print(f"Found {len(nc4_files)} .nc4 files:")
    for file in nc4_files:
        print(f"  {file}")

    record_ids = []
    for nc4_file in nc4_files:
        try:
            print(f"\n{'='*60}")
            record_id = load_nc4_to_postgres(nc4_file, load_sample_data)
            record_ids.append(record_id)
        except Exception as e:
            print(f"Error loading {nc4_file}: {e}")

    return record_ids


def verify_loaded_data():
    """Verify the loaded data in the database."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Get recent NC4 records
                cur.execute("""
                    SELECT id, file_name, loaded_at,
                           data_content->'processing_info'->>'file_size_mb' as file_size_mb,
                           jsonb_array_length(data_content->'sample_data') as sample_count
                    FROM staging.ghrsst_raw
                    WHERE file_name LIKE '%.nc4'
                    ORDER BY loaded_at DESC
                    LIMIT 10
                """)

                records = cur.fetchall()

                if records:
                    print(f"\n📊 Found {len(records)} NC4 records in database:")
                    for record in records:
                        print(f"   ID: {record[0]}, File: {record[1]}")
                        print(f"      Size: {record[3]} MB, Samples: {record[4]}")
                        print(f"      Loaded: {record[2]}")
                else:
                    print("\n📊 No NC4 records found in database")

                return len(records)

    except Exception as e:
        print(f"Error verifying data: {e}")
        return 0


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Load specific file
        nc4_file = sys.argv[1]
        if Path(nc4_file).exists():
            load_nc4_to_postgres(nc4_file)
            verify_loaded_data()
        else:
            print(f"File not found: {nc4_file}")
    else:
        # Load all .nc4 files
        record_ids = load_all_nc4_files()
        print(f"\n✅ Loaded {len(record_ids)} files to database")
        verify_loaded_data()