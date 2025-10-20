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

from onchaindata.data_extraction import GraphQLStream

logger = logging.getLogger(__name__)


def stream(args):
    """
    Execute streaming mode: continuously poll and push to database.

    Args:
        args: Parsed command-line arguments
    """
    # Validate arguments
    if not all([args.database_client, args.schema, args.database_table, args.fields]):
        raise ValueError(
            "Streaming mode requires --database-client, --schema, --database-table, and --fields"
        )

    # Parse fields
    fields = [f.strip() for f in args.fields.split(",")]

    # Initialize database client
    if args.database_client == "snowflake":
        client = SnowflakeClient().from_env()
    elif args.database_client == "postgres":
        client = PostgresClient.from_env()
    else:
        raise ValueError(
            f"Invalid client: {args.database_client}, use 'snowflake' or 'postgres', or implement new client"
        )

    loader = Loader(client=client)
    streamer = GraphQLStream(
        endpoint=args.endpoint,
        table_name=args.graphql_table,
        fields=fields,
        poll_interval=args.poll_interval,
    )

    streamer.stream(
        loader=loader,
        schema=args.schema,
        table_name=args.database_table,
    )


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
        "--graphql-table",
        type=str,
        help="Name of the table from GraphQL Endpoint, e.g. 'stablesTransfers'",
        default="stablesTransfers",
    )

    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Polling interval in seconds for streaming mode (default: 5)",
    )

    parser.add_argument(
        "--fields",
        type=str,
        help="Comma-separated list of fields to fetch (required for streaming mode)",
        default="id,blockNumber,timestamp,contractAddress,from,to,value",
    )

    # Database configuration (for streaming mode)
    parser.add_argument(
        "-c",
        "--database-client",
        type=str,
        choices=["snowflake", "postgres"],
        help="Client name (required for streaming mode)",
    )
    parser.add_argument(
        "-s",
        "--schema",
        type=str,
        help="Schema name (required for streaming mode)",
    )
    parser.add_argument(
        "-t",
        "--database-table",
        type=str,
        help="Target table name in database (required for streaming mode)",
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

    stream(args)


if __name__ == "__main__":
    main()
