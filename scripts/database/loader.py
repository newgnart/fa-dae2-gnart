#!/usr/bin/env python3
"""
Simple CRUD operations demo for Week 02 Lab
"""

import os

import psycopg
from dotenv import load_dotenv
from helper import get_connection

# Load environment variables
load_dotenv()


def load_parquet_to_postgres(
    parquet_file_path: str,
    table_name: str,
    postgres_client: PostgresClient,
    dataset_name: str = "etherscan_raw",
    write_disposition: str = "append",
    primary_key: Optional[List[str]] = None,
) -> Any:
    """Load data from a Parquet file to PostgreSQL using DLT.

    Args:
        parquet_file_path: Full path to the parquet file
        table_name: Target table name in PostgreSQL
        postgres_client: PostgreSQL client
        dataset_name: Target dataset/schema name
        write_disposition: How to handle existing data ("append", "replace", "merge")
        primary_key: Optional primary key columns for the table

    Returns:
        DLT pipeline run result
    """
    import dlt
    from pathlib import Path

    parquet_path = Path(parquet_file_path)

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_file_path}")

    # Set default primary keys based on file type (using snake_case column names)
    if primary_key is None:
        if "logs" in parquet_file_path.lower():
            primary_key = [
                "transaction_hash",
                "log_index",
            ]
        elif "transactions" in parquet_file_path.lower():
            primary_key = ["hash", "transaction_index"]
        else:
            primary_key = []

    logger.debug(f"Loading {parquet_file_path} to table {table_name}")

    try:
        # Use scan_parquet for memory efficiency
        lazy_df = pl.scan_parquet(parquet_path).unique()

        # Transform column names from camelCase to snake_case and handle NULL values
        df = lazy_df.collect()

        # Column name mapping for logs
        if "logs" in parquet_file_path.lower():
            # Handle NULL logIndex values by filtering them out or setting to 0
            if "logIndex" in df.columns:
                df = df.filter(pl.col("logIndex").is_not_null())
                df = df.rename({"logIndex": "log_index"})

            # Rename other camelCase columns to snake_case
            column_mapping = {
                "blockNumber": "block_number",
                "blockHash": "block_hash",
                "timeStamp": "time_stamp",
                "gasPrice": "gas_price",
                "gasUsed": "gas_used",
                "transactionHash": "transaction_hash",
                "transactionIndex": "transaction_index",
            }

            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df = df.rename({old_name: new_name})

        # Column name mapping for transactions
        elif "transactions" in parquet_file_path.lower():
            column_mapping = {
                "blockNumber": "block_number",
                "blockHash": "block_hash",
                "timeStamp": "time_stamp",
                "transactionIndex": "transaction_index",
            }

            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df = df.rename({old_name: new_name})

        # Get row count efficiently
        row_count = len(df)
        logger.debug(f"Loaded {row_count} rows from Parquet file")

        # Convert to records for DLT
        records = df.to_dicts()

        # Convert numpy arrays in topics to Python lists for JSON serialization
        if "logs" in parquet_file_path.lower():
            for record in records:
                if "topics" in record and record["topics"] is not None:
                    if hasattr(record["topics"], "tolist"):
                        record["topics"] = record["topics"].tolist()

        # Get destination from postgres client
        destination = postgres_client.get_dlt_destination()

        # Create and run pipeline
        pipeline = dlt.pipeline(
            pipeline_name="backfill_to_postgres",
            destination=destination,
            dataset_name=dataset_name,
        )

        # Define column hints for logs table to properly handle topics array
        columns = None
        if "logs" in parquet_file_path.lower() and "topics" in df.columns:
            columns = {"topics": {"data_type": "json", "nullable": True}}

        run_kwargs = {
            "table_name": table_name,
            "write_disposition": write_disposition,
        }
        if primary_key:
            run_kwargs["primary_key"] = primary_key
        if columns:
            run_kwargs["columns"] = columns

        result = pipeline.run(records, **run_kwargs)
        logger.debug(
            f"✅ Successfully loaded {len(records)} rows to table '{table_name}'"
        )

        return result

    except Exception as e:
        logger.error(
            f"❌ Failed to load {parquet_file_path} to table '{table_name}': {e}"
        )
        raise


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
