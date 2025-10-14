import argparse
from dotenv import load_dotenv

load_dotenv()
from onchaindata.data_pipeline import load_parquet_to_postgres
from onchaindata.utils import PostgresClient


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
        "-t",
        "--table_name",
        type=str,
        help="Table name",
        required=True,
    )

    args = parser.parse_args()
    load_parquet_to_postgres(
        file_path=args.file_path,
        postgres_client=PostgresClient.from_env(),
        schema="raw",
        table=args.table_name,
    )


if __name__ == "__main__":
    main()
