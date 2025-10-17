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


def batch_mode(args):
    """
    Execute batch mode: fetch data once and save to Parquet.

    Args:
        args: Parsed command-line arguments
    """
    # Validate arguments
    if not args.output:
        raise ValueError("Batch mode requires --output")

    # Load query
    if args.query_file:
        with open(args.query_file, "r") as f:
            query = f.read()
    elif args.query:
        query = args.query
    else:
        raise ValueError("Batch mode requires either --query or --query-file")

    # Parse variables
    variables = json.loads(args.variables) if args.variables else {}

    # Fetch data
    logger.info(f"Fetching from: {args.endpoint}")
    extractor = GraphQLBatch(
        endpoint=args.endpoint,
        query=query,
        variables=variables,
    )
    df = extractor.extract_to_dataframe(args.table_name)

    if df.is_empty():
        logger.info("No data returned from query")
        return

    logger.info(f"Fetched {len(df)} records")

    # Save to Parquet
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    logger.info(f"Saved to: {output_path}")


def stream_mode(args):
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
        "-q",
        "--query",
        type=str,
        help="GraphQL query string (or use --query-file)",
    )
    parser.add_argument(
        "--query-file",
        type=str,
        help="Path to file containing GraphQL query",
    )
    parser.add_argument(
        "--graphql-table",
        type=str,
        required=True,
        help="Name of the table from GraphQL Endpoint, e.g. 'stablesTransfers'",
    )

    # Streaming mode configuration
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Enable streaming mode (push to database continuously)",
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

    # Batch mode configuration
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Output Parquet file path (for batch mode)",
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

    # Route to appropriate mode
    if args.streaming:
        stream_mode(args)
    else:
        batch_mode(args)


if __name__ == "__main__":
    main()
