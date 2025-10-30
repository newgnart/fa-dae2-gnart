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


def _add_block_filters_to_query(
    query: str, table_name: str, from_block: int = None, to_block: int = None
) -> str:
    """
    Add block number filters to a GraphQL query.

    Args:
        query: Original GraphQL query string
        table_name: Name of the table in the query
        from_block: Minimum block number (inclusive)
        to_block: Maximum block number (inclusive)

    Returns:
        Modified query string with block filters
    """
    import re

    # Build where clause - combine conditions in a single blockNumber object
    block_conditions = []
    if from_block is not None:
        block_conditions.append(f"_gte: {from_block}")
    if to_block is not None:
        block_conditions.append(f"_lte: {to_block}")

    if not block_conditions:
        return query

    # Create proper GraphQL where clause with conditions in single blockNumber object
    where_clause = f"blockNumber: {{{', '.join(block_conditions)}}}"

    # Find the table call in the query and add/modify where clause
    # Pattern: tableName(...) or tableName(order_by: {...})
    pattern = rf"{table_name}\s*\((.*?)\)"

    def replacer(match):
        existing_args = match.group(1).strip()
        # Check if there's already a where clause
        if "where:" in existing_args:
            # Add conditions to existing where clause
            # This is complex, so for now we'll just append
            return f"{table_name}({existing_args}, where: {{{where_clause}}})"
        elif existing_args:
            # Has other args (like order_by), add where clause
            return f"{table_name}({existing_args}, where: {{{where_clause}}})"
        else:
            # No existing args, add where clause
            return f"{table_name}(where: {{{where_clause}}})"

    modified_query = re.sub(pattern, replacer, query, count=1)
    return modified_query


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

    # Modify query to include block number filters if provided
    if args.from_block is not None or args.to_block is not None:
        query = _add_block_filters_to_query(
            query, args.graphql_table, args.from_block, args.to_block
        )
        logger.info(
            f"Applied block filters: from_block={args.from_block}, to_block={args.to_block}"
        )

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

    # Block range filters
    parser.add_argument(
        "--from_block",
        type=int,
        help="Starting block number (inclusive)",
        default=None,
    )
    parser.add_argument(
        "--to_block",
        type=int,
        help="Ending block number (inclusive)",
        default=None,
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
        "-f",
        "--file_name",
        type=str,
        help="Parquet file name to save data to, without extension",
        default="data",
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
