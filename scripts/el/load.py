import argparse
from dotenv import load_dotenv
import polars as pl

load_dotenv()
from onchaindata.data_pipeline import Loader
from onchaindata.utils import SnowflakeClient, PostgresClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--file_path",
        type=str,
        help="File path",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--client",
        type=str,
        help="Client name, either 'snowflake' or 'postgres'",
        required=True,
    )
    # warehouse/database is configured in the client
    parser.add_argument(
        "-s",
        "--schema",
        type=str,
        help="Schema name",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--table",
        type=str,
        help="Table name",
        required=True,
    )
    parser.add_argument(
        "-w",
        "--write_disposition",
        type=str,
        help="Write disposition, either 'append' or 'replace' or 'merge'",
        default="append",
    )
    parser.add_argument(
        "-k",
        "--primary_key",
        type=str,
        help="Comma-separated list of column names to use as primary key for merge. Required when -w merge. Example: 'contract_address,chain'",
        default=None,
    )

    args = parser.parse_args()
    if args.client == "snowflake":
        client = SnowflakeClient().from_env()
    elif args.client == "postgres":
        client = PostgresClient.from_env()
    else:
        raise ValueError(
            f"Invalid client: {args.client}, use 'snowflake' or 'postgres', or implement new client"
        )

    loader = Loader(client=client)

    if args.file_path.endswith(".csv"):
        df = pl.read_csv(args.file_path)
    elif args.file_path.endswith(".parquet"):
        df = pl.read_parquet(args.file_path)
    else:
        raise ValueError(
            f"Invalid file extension: {args.file_path}, use 'csv' or 'parquet'"
        )

    # Parse primary key if provided
    primary_key = None
    if args.primary_key:
        primary_key = [col.strip() for col in args.primary_key.split(",")]

    loader.load_dataframe(
        df=df,
        schema=args.schema,
        table_name=args.table,
        write_disposition=args.write_disposition,
        primary_key=primary_key,
    )


if __name__ == "__main__":
    main()
