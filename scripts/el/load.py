import argparse
from dotenv import load_dotenv
from pathlib import Path


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

    args = parser.parse_args()
    if args.client == "snowflake":
        client = SnowflakeClient()
    elif args.client == "postgres":
        client = PostgresClient.from_env()
        print(client.get_connection_url())
    else:
        raise ValueError(
            f"Invalid client: {args.client}, use 'snowflake' or 'postgres', or implement new client"
        )

    loader = Loader(client=client)
    loader.load_parquet(
        file_path=args.file_path,
        schema=args.schema,
        table_name=args.table,
        write_disposition=args.write_disposition,
    )


if __name__ == "__main__":
    main()
