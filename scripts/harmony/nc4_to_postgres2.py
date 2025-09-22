#!/usr/bin/env python3
"""
Load full GHRSST data (all grid points) into PostgreSQL
This loads all ~1.4 million data points from each NC4 file
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime

import xarray as xr
import psycopg
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
from scripts.database import get_connection


def convert_timedelta_to_seconds(td_array):
    """Convert timedelta64 array to seconds (as integers)."""
    # Handle NaT (Not a Time) values
    td_seconds = td_array.astype("timedelta64[s]").astype("float64")
    td_seconds = np.where(np.isnan(td_seconds), None, td_seconds.astype("int64"))
    return td_seconds


def load_nc4_full_data(nc4_file_path, batch_size=10000):
    """
    Load all data points from NC4 file into PostgreSQL.

    Args:
        nc4_file_path: Path to the .nc4 file
        batch_size: Number of records to insert per batch (for memory efficiency)
    """

    nc4_path = Path(nc4_file_path)
    if not nc4_path.exists():
        raise FileNotFoundError(f"NC4 file not found: {nc4_path}")

    print(f"Loading full data from: {nc4_path.name}")
    start_time = time.time()

    # Open the dataset
    ds = xr.open_dataset(nc4_path)

    try:
        # Get dimensions
        time_vals = ds.time.values
        lat_vals = ds.lat.values
        lon_vals = ds.lon.values

        print(
            f"Data dimensions: {len(time_vals)} time × {len(lat_vals)} lat × {len(lon_vals)} lon"
        )
        total_points = len(time_vals) * len(lat_vals) * len(lon_vals)
        print(f"Total grid points: {total_points:,}")

        # Check which file already exists in database to avoid duplicates
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM staging.ghrsst_data WHERE file_name = %s",
                    (nc4_path.name,),
                )
                existing_count = cur.fetchone()[0]

                if existing_count > 0:
                    print(
                        f"⚠️  File {nc4_path.name} already has {existing_count} records in database"
                    )
                    response = input("Continue anyway? (y/n): ")
                    if response.lower() != "y":
                        return 0

        # Prepare data for batch insertion
        records_inserted = 0
        batch_data = []

        print("Extracting and preparing data...")

        for t_idx, time_val in enumerate(time_vals):
            time_stamp = pd.to_datetime(time_val)

            # Get all data for this time slice
            analysed_sst = ds["analysed_sst"].isel(time=t_idx)
            analysis_error = ds["analysis_error"].isel(time=t_idx)
            sst_anomaly = ds["sst_anomaly"].isel(time=t_idx)
            mask = ds["mask"].isel(time=t_idx)
            sea_ice_fraction = ds["sea_ice_fraction"].isel(time=t_idx)
            dt_1km_data = ds["dt_1km_data"].isel(time=t_idx)

            # Convert timedelta to seconds
            dt_1km_seconds = convert_timedelta_to_seconds(dt_1km_data.values)

            # Iterate through all lat/lon combinations
            for lat_idx, lat in enumerate(lat_vals):
                for lon_idx, lon in enumerate(lon_vals):

                    # Extract values at this grid point
                    sst_val = analysed_sst.isel(lat=lat_idx, lon=lon_idx).values
                    error_val = analysis_error.isel(lat=lat_idx, lon=lon_idx).values
                    anomaly_val = sst_anomaly.isel(lat=lat_idx, lon=lon_idx).values
                    mask_val = mask.isel(lat=lat_idx, lon=lon_idx).values
                    ice_val = sea_ice_fraction.isel(lat=lat_idx, lon=lon_idx).values
                    dt_val = dt_1km_seconds[lat_idx, lon_idx]

                    # Convert numpy types and handle NaN values
                    record = (
                        nc4_path.name,
                        time_stamp,
                        float(lat),
                        float(lon),
                        float(sst_val) if not np.isnan(sst_val) else None,
                        float(error_val) if not np.isnan(error_val) else None,
                        float(anomaly_val) if not np.isnan(anomaly_val) else None,
                        int(mask_val) if not np.isnan(mask_val) else None,
                        float(ice_val) if not np.isnan(ice_val) else None,
                        dt_val,  # Already converted to int or None
                    )

                    batch_data.append(record)

                    # Insert batch when it reaches batch_size
                    if len(batch_data) >= batch_size:
                        records_inserted += insert_batch(batch_data)
                        batch_data = []

                        # Progress update
                        progress = records_inserted / total_points * 100
                        elapsed = time.time() - start_time
                        print(
                            f"Progress: {records_inserted:,}/{total_points:,} ({progress:.1f}%) - {elapsed:.1f}s"
                        )

        # Insert remaining records
        if batch_data:
            records_inserted += insert_batch(batch_data)

        elapsed_time = time.time() - start_time
        print(f"✅ Loaded {records_inserted:,} records in {elapsed_time:.1f} seconds")
        print(f"   Rate: {records_inserted/elapsed_time:.0f} records/second")

        return records_inserted

    finally:
        ds.close()


def insert_batch(batch_data):
    """Insert a batch of records into the database."""
    if not batch_data:
        return 0

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Use COPY for very fast bulk insert
                insert_sql = """
                    INSERT INTO staging.ghrsst_data
                    (file_name, time_stamp, latitude, longitude, analysed_sst,
                     analysis_error, sst_anomaly, mask, sea_ice_fraction, dt_1km_data_seconds)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                cur.executemany(insert_sql, batch_data)
                return len(batch_data)

    except Exception as e:
        print(f"Error inserting batch: {e}")
        return 0


def verify_loaded_data(file_name=None):
    """Verify the loaded data in the database."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Get statistics
                where_clause = "WHERE file_name = %s" if file_name else ""
                params = (file_name,) if file_name else ()

                cur.execute(
                    f"""
                    SELECT
                        COUNT(*) as total_records,
                        COUNT(DISTINCT file_name) as file_count,
                        COUNT(DISTINCT time_stamp) as time_count,
                        MIN(time_stamp) as earliest_time,
                        MAX(time_stamp) as latest_time,
                        MIN(latitude) as min_lat,
                        MAX(latitude) as max_lat,
                        MIN(longitude) as min_lon,
                        MAX(longitude) as max_lon,
                        COUNT(*) FILTER (WHERE analysed_sst IS NOT NULL) as sst_data_points,
                        MIN(analysed_sst) as min_sst,
                        MAX(analysed_sst) as max_sst,
                        AVG(analysed_sst) as avg_sst
                    FROM staging.ghrsst_data
                    {where_clause}
                """,
                    params,
                )

                stats = cur.fetchone()

                print(f"\n📊 Database Statistics:")
                print(f"   Total records: {stats[0]:,}")
                print(f"   Files: {stats[1]}")
                print(f"   Time periods: {stats[2]}")
                print(f"   Time range: {stats[3]} to {stats[4]}")
                print(f"   Lat range: {stats[5]:.3f} to {stats[6]:.3f}")
                print(f"   Lon range: {stats[7]:.3f} to {stats[8]:.3f}")
                print(f"   SST data points: {stats[9]:,}")
                if stats[10]:
                    print(
                        f"   SST range: {stats[10]:.3f} to {stats[11]:.3f} K ({stats[10]-273.15:.1f} to {stats[11]-273.15:.1f} °C)"
                    )
                    print(
                        f"   SST average: {stats[12]:.3f} K ({stats[12]-273.15:.1f} °C)"
                    )

                return stats[0]

    except Exception as e:
        print(f"Error verifying data: {e}")
        return 0


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Load specific file
        nc4_file = sys.argv[1]
        if Path(nc4_file).exists():
            records = load_nc4_full_data(nc4_file)
            verify_loaded_data(Path(nc4_file).name)
        else:
            print(f"File not found: {nc4_file}")
    else:
        # Load all .nc4 files
        nc4_files = list(Path(".").rglob("*.nc4"))

        if not nc4_files:
            print("No .nc4 files found")
        else:
            print(f"Found {len(nc4_files)} .nc4 files")
            total_records = 0

            for nc4_file in nc4_files:
                try:
                    records = load_nc4_full_data(nc4_file)
                    total_records += records
                except Exception as e:
                    print(f"Error loading {nc4_file}: {e}")

            print(f"\n✅ Total records loaded: {total_records:,}")
            verify_loaded_data()
