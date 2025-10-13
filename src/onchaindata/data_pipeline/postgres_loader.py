#!/usr/bin/env python3

import os
import dlt
from dlt.sources.filesystem import filesystem, read_parquet
import polars as pl
import json
from ..utils import PostgresClient


def load_parquet_to_postgres(
    file_path: str,
    table_name: str,
    postgres_client: PostgresClient,
    schema_name: str = os.getenv("DB_SCHEMA"),
    write_disposition: str = "append",
):
    """Load parquet file to PostgreSQL using DLT filesystem source."""

    # Create filesystem source
    fs_source = filesystem(bucket_url=".", file_glob=file_path)

    parquet_resource = fs_source | read_parquet()
    if table_name == "logs":
        parquet_resource.apply_hints(
            columns={"topics": {"data_type": "json", "nullable": True}}
        )

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="parquet_loader",
        destination=postgres_client.get_dlt_destination(),
        dataset_name=schema_name,
    )

    # Load data
    result = pipeline.run(
        parquet_resource,
        table_name=table_name,
        write_disposition=write_disposition,
    )
    return result


def load_parquet_to_postgres_wo_dlt(
    file_path: str,
    table_name: str,
    postgres_client: PostgresClient,
    schema_name: str,
):
    # Read parquet file
    df = pl.read_parquet(file_path).head(100)

    column_name_mapping = {
        "logs": {
            "blockNumber": "block_number",
            "blockHash": "block_hash",
            "timeStamp": "time_stamp",
            "gasPrice": "gas_price",
            "gasUsed": "gas_used",
            "transactionHash": "transaction_hash",
            "transactionIndex": "transaction_index",
            "logIndex": "log_index",
        },
        "transactions": {
            "blockNumber": "block_number",
            "blockHash": "block_hash",
            "timeStamp": "time_stamp",
            "transactionIndex": "transaction_index",
        },
    }

    df = df.rename(column_name_mapping[table_name])

    # Get connection and insert data
    with postgres_client.get_connection() as conn:
        with conn.cursor() as cur:
            # Create INSERT statement
            columns = df.columns
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO {schema_name}.logs_test ({', '.join(columns)}) VALUES ({placeholders})"

            # Insert data row by row
            for row in df.iter_rows():
                # Convert values for database insertion
                row_values = []
                for i, val in enumerate(row):
                    col_name = columns[i]
                    if isinstance(val, float) and val != val:  # NaN check
                        row_values.append(None)
                    elif col_name == "topics" and isinstance(val, list):
                        row_values.append(json.dumps(val))
                    else:
                        row_values.append(val)
                cur.execute(insert_sql, tuple(row_values))

            conn.commit()

    return len(df)
