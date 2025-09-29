#!/usr/bin/env python3

import os
import dlt
from dlt.sources.filesystem import filesystem, read_parquet
import polars as pl
import json
from ..utils import SnowflakeClient


def load_parquet_to_snowflake(
    file_path: str,
    table_name: str,
    snowflake_client: SnowflakeClient,
    schema_name: str = os.getenv("SNOWFLAKE_SCHEMA"),
    write_disposition: str = "append",
):
    """Load parquet file to Snowflake using DLT filesystem source."""

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
        destination=snowflake_client.get_dlt_destination(),
        dataset_name=schema_name,
    )

    # Load data
    result = pipeline.run(
        parquet_resource,
        table_name=table_name,
        write_disposition=write_disposition,
    )
    return result


def load_parquet_to_snowflake_wo_dlt(
    file_path: str,
    table_name: str,
    snowflake_client: SnowflakeClient,
    schema_name: str = os.getenv("SNOWFLAKE_SCHEMA"),
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
    with snowflake_client.connection() as conn:
        with conn.cursor() as cur:
            # Create INSERT statement
            columns = df.columns
            placeholders = ", ".join(["?"] * len(columns))
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


def upload_file_to_stage(file_path: str, stage_name: str) -> bool:
    """Upload file to Snowflake stage."""
    try:
        with SnowflakeClient().cursor() as cursor:
            put_command = f"PUT file://{file_path} @{stage_name}"
            cursor.execute(put_command)
            result = cursor.fetchone()
            print(f"✅ File uploaded: {result[0]} - {result[6]} status")
            return True
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False


def load_parquet_via_stage(
    file_path: str,
    table_name: str,
    stage_name: str,
    snowflake_client: SnowflakeClient,
    schema_name: str = os.getenv("SNOWFLAKE_SCHEMA") or "FA02_STAGING",
):
    """Load parquet file to Snowflake via internal stage."""

    # Upload file to stage
    if not upload_file_to_stage(file_path, stage_name):
        raise Exception("Failed to upload file to stage")

    # Get filename for COPY command
    filename = os.path.basename(file_path)

    try:
        with snowflake_client.connection() as conn:
            with conn.cursor() as cur:
                # COPY command to load from stage
                copy_sql = f"""
                COPY INTO {schema_name}.{table_name}
                FROM @{stage_name}/{filename}
                FILE_FORMAT = (TYPE = 'PARQUET')
                """

                cur.execute(copy_sql)
                result = cur.fetchone()
                print(f"✅ Data loaded: {result}")
                conn.commit()

                return result
    except Exception as e:
        print(f"❌ COPY command failed: {e}")
        raise


if __name__ == "__main__":
    load_parquet_to_snowflake(
        file_path=".data/ethereum_0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72/logs_sample.parquet",
        table_name="logs_sample",
        snowflake_client=SnowflakeClient(),
    )
    # load_parquet_to_snowflake(
    #     file_path=".data/ethereum_0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72/transactions.parquet",
    #     table_name="transactions",
    #     snowflake_client=SnowflakeClient(),
    # )
