#!/usr/bin/env python3
"""
GraphQL data fetcher with streaming and batch modes.

This script fetches data from a GraphQL endpoint and either:
1. Saves to Parquet file (batch mode)
2. Pushes directly to database (streaming mode)
"""

import argparse, json, logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
from onchaindata.data_pipeline import Loader
from onchaindata.utils import PostgresClient, SnowflakeClient

from onchaindata.data_extraction import GraphQLBatch, GraphQLStream

logger = logging.getLogger(__name__)


def extract(args):
    """
    Execute batch mode: fetch data once and save to Parquet.

    Args:
        args: Parsed command-line arguments
    """
    # Load query
    if args.query_file:
        with open(args.query_file, "r") as f:
            query = f.read()
    elif args.query:
        query = args.query
    else:
        raise ValueError("Batch mode requires either --query or --query-file")

    # Fetch data
    logger.info(f"Fetching from: {args.endpoint}")
    extractor = GraphQLBatch(
        endpoint=args.endpoint,
        query=query,
    )
    df = extractor.extract_to_dataframe(args.graphql_table)

    if df.is_empty():
        logger.info("No data returned from query")
        return

    logger.info(f"Fetched {len(df)} records")

    # Save to Parquet
    min_block_number = df["blockNumber"].min()
    max_block_number = df["blockNumber"].max()
    output_path = (
        Path(args.output_dir)
        / f"{args.file_name}_{min_block_number}_{max_block_number}.parquet"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    logger.info(f"Saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch data from GraphQL endpoint with streaming or batch mode"
    )

    # GraphQL endpoint configuration
    parser.add_argument(
        "-e",
        "--endpoint",
        type=str,
        default="http://localhost:8080/v1/graphql",
        help="GraphQL endpoint URL",
    )
    parser.add_argument(
        "-q",
        "--query",
        type=str,
        help="GraphQL query string (or use --query-file)",
    )
    parser.add_argument(
        "--query-file",
        type=str,
        help="Path to file containing GraphQL query",
        default="scripts/el/stables_transfers.graphql",
    )
    parser.add_argument(
        "--graphql-table",
        type=str,
        help="Name of the table from GraphQL Endpoint, e.g. 'stablesTransfers'",
        default="stablesTransfers",
    )

    # Output configuration
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default=".data/raw",
        help="Output directory",
    )
    parser.add_argument(
        "-t",
        "--file_name",
        type=str,
        help="File name",
        default="data.parquet",
    )

    # Logging configuration
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity: -v for INFO, -vv for DEBUG",
    )

    args = parser.parse_args()

    # Configure logging based on verbosity
    if args.verbose == 0:
        log_level = logging.WARNING
    elif args.verbose == 1:
        log_level = logging.INFO
    else:  # >= 2
        log_level = logging.DEBUG

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    extract(args)


if __name__ == "__main__":
    main()
