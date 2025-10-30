#!/usr/bin/env python3

"""
Move data from PostgreSQL (raw.raw_transfer) to Snowflake (raw.transfer).
Queries data from PostgreSQL within a block range and loads to Snowflake.
"""

import argparse
import logging
from dotenv import load_dotenv
import polars as pl

load_dotenv()
from onchaindata.utils import PostgresClient, SnowflakeClient
from onchaindata.data_pipeline import Loader


def setup_logging(verbose: int = 0):
    """Setup logging based on verbosity level."""
    if verbose == 0:
        level = logging.WARNING
    elif verbose == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(__name__)


def query_postgres_data(
    pg_client: PostgresClient,
    from_block: int,
    to_block: int,
    logger: logging.Logger,
) -> pl.DataFrame:
    """
    Query data from PostgreSQL raw.raw_transfer table within block range.

    Args:
        pg_client: PostgresClient instance
        from_block: Starting block number (inclusive)
        to_block: Ending block number (inclusive)
        logger: Logger instance

    Returns:
        Polars DataFrame with queried data
    """
    query = f"""
        SELECT *
        FROM raw.raw_transfer
        WHERE block_number::integer >= {from_block}
          AND block_number::integer <= {to_block}
        ORDER BY block_number
    """

    logger.info(f"Querying PostgreSQL for blocks {from_block} to {to_block}")
    logger.debug(f"Query: {query}")
    with pg_client.get_connection() as conn:
        df = pl.read_database(connection=conn, query=query)

    logger.info(f"Retrieved {len(df)} rows from PostgreSQL")

    return df


def load_to_snowflake(
    df: pl.DataFrame,
    sf_loader: Loader,
    logger: logging.Logger,
) -> None:
    """
    Load DataFrame to Snowflake raw.transfer table.

    Args:
        df: Polars DataFrame to load
        sf_loader: Loader instance configured with SnowflakeClient
        logger: Logger instance
    """
    if len(df) == 0:
        logger.warning("No data to load to Snowflake")
        return

    logger.info(f"Loading {len(df)} rows to Snowflake raw.transfer")

    result = sf_loader.load_dataframe(
        df=df,
        schema="raw",
        table_name="transfer",
        write_disposition="append",
    )

    logger.info(f"Successfully loaded data to Snowflake: {result}")


def main():
    parser = argparse.ArgumentParser(
        description="Move data from PostgreSQL raw.raw_transfer to Snowflake raw.transfer"
    )
    parser.add_argument(
        "--from_block",
        type=int,
        required=True,
        help="Starting block number (inclusive)",
    )
    parser.add_argument(
        "--to_block",
        type=int,
        required=True,
        help="Ending block number (inclusive)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity level (use -v for INFO, -vv for DEBUG)",
    )

    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    # Validate block range
    if args.from_block > args.to_block:
        raise ValueError(
            f"from_block ({args.from_block}) must be <= to_block ({args.to_block})"
        )

    logger.info("Initializing database clients")

    # Step 1: Initialize clients
    pg_client = PostgresClient.from_env()
    sf_client = SnowflakeClient().from_env()
    sf_loader = Loader(client=sf_client)

    # Step 2: Query data from PostgreSQL
    df = query_postgres_data(
        pg_client=pg_client,
        from_block=args.from_block,
        to_block=args.to_block,
        logger=logger,
    )

    # Step 3: Load data to Snowflake
    load_to_snowflake(
        df=df,
        sf_loader=sf_loader,
        logger=logger,
    )

    logger.info("Data migration completed successfully")


if __name__ == "__main__":
    main()
