#!/usr/bin/env python
"""
Script to scrape contract name tags from Etherscan.

Reads addresses from a CSV file, scrapes their name tags, and saves the results
with a new 'name_tag' column.

Usage:
    uv run python scripts/el/scrape_etherscan.py -i addresses.csv -o addresses_with_tags.csv
    uv run python scripts/el/scrape_etherscan.py -i addresses.csv -o addresses_with_tags.csv --address-column contract_address
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from onchaindata.data_extraction.etherscan_scraper import EtherscanScraper
from onchaindata.data_extraction.etherscan import EtherscanClient

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False):
    """Set up logging configuration."""
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def extract_contract_name_tags(
    df: pd.DataFrame,
    etherscan_client: EtherscanClient,
    address_column: str = "address",
    headless: bool = True,
    timeout: int = 10,
) -> pd.DataFrame:
    """
    Scrape name tags for addresses in a DataFrame.

    Args:
        df: DataFrame containing addresses
        address_column: Name of the column containing addresses
        headless: Run browser in headless mode
        timeout: Maximum wait time for page elements

    Returns:
        DataFrame with added 'name_tag' column
    """
    # Make a copy to avoid modifying the original
    df = df.copy()

    # Validate address column exists
    if address_column not in df.columns:
        logger.error(
            f"Column '{address_column}' not found. Available: {', '.join(df.columns)}"
        )
        raise ValueError(f"Column '{address_column}' not found")

    # Initialize name_tag column if it doesn't exist
    if "name_tag" not in df.columns:
        df["name_tag"] = None

    total = len(df)
    logger.info(f"Scraping {total} addresses from Etherscan")

    # Scrape name tags
    with EtherscanScraper(headless=headless, timeout=timeout) as scraper:
        for idx, row in df.iterrows():
            address = row[address_column]

            # Skip if address is None or empty
            if pd.isna(address) or str(address).strip() == "":
                logger.debug(f"Skipping empty address at row {idx}")
                continue

            try:
                name_tag = scraper.get_contract_name_tag(str(address))
                contract_metadata = etherscan_client.get_contract_metadata(address)
                df.at[idx, "name_tag"] = name_tag
                df.at[idx, "contract_name"] = contract_metadata["ContractName"]
                logger.info(
                    f"[{idx + 1}/{total}] {address}: {name_tag or 'No tag found'} {contract_metadata['ContractName'] or 'No name found'}"
                )
            except Exception as e:
                logger.warning(f"[{idx + 1}/{total}] {address}: Error - {e}")
                df.at[idx, "name_tag"] = None

    # Log summary
    tagged_count = df["name_tag"].notna().sum()
    named_count = df["contract_name"].notna().sum()
    logger.info(
        f"Completed: {tagged_count}/{total} addresses tagged, {named_count}/{total} addresses named"
    )

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Scrape contract name tags from Etherscan and save to CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-i", "--input", required=True, help="Input CSV file containing addresses"
    )

    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Output CSV file to save results with name tags",
    )

    parser.add_argument(
        "--address-column",
        default="address",
        help="Name of the column containing addresses (default: 'address')",
    )

    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run browser in visible mode (not headless)",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Maximum wait time for page elements in seconds (default: 10)",
    )

    parser.add_argument(
        "--save-every",
        type=int,
        default=100,
        help="Save progress every N rows (default: 100)",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set up logging
    setup_logging(args.verbose)

    try:
        # Read input CSV
        logger.info(f"Reading from: {args.input}")
        df = pd.read_csv(args.input)

        # Validate address column exists
        if args.address_column not in df.columns:
            logger.error(
                f"Column '{args.address_column}' not found. Available: {', '.join(df.columns)}"
            )
            raise ValueError(f"Column '{args.address_column}' not found")

        # Check if output file exists to determine already processed addresses
        processed_addresses = set()
        if Path(args.output).exists():
            logger.info(f"Loading existing progress from: {args.output}")
            existing_df = pd.read_csv(args.output)
            processed_addresses = set(
                existing_df[args.address_column].dropna().astype(str).str.lower()
            )
            logger.info(f"Found {len(processed_addresses)} already processed addresses")

        total = len(df)
        logger.info(f"Processing {total} addresses from Etherscan")

        # Initialize Etherscan client
        etherscan_client = EtherscanClient(chain="ethereum")

        # Buffer to collect results before appending
        results_buffer = []

        # Scrape with periodic saves
        with EtherscanScraper(
            headless=not args.no_headless, timeout=args.timeout
        ) as scraper:
            for idx, row in df.iterrows():
                address = row[args.address_column]

                # Skip if address is None or empty
                if pd.isna(address) or str(address).strip() == "":
                    logger.debug(f"Skipping empty address at row {idx}")
                    continue

                # Skip if already processed
                if str(address).lower() in processed_addresses:
                    logger.debug(f"Skipping already processed address: {address}")
                    continue

                try:
                    name_tag = scraper.get_contract_name_tag(str(address))
                    contract_metadata = etherscan_client.get_contract_metadata(address)
                    creation_block_number = (
                        etherscan_client.get_contract_creation_block_number(address)
                    )

                    # Add to buffer
                    result = row.to_dict()
                    result["name_tag"] = name_tag
                    result["contract_name"] = contract_metadata["ContractName"]
                    result["is_contract"] = creation_block_number is not None
                    results_buffer.append(result)

                    logger.info(
                        f"[{idx + 1}/{total}] {address}: {name_tag or 'No tag found'} | {contract_metadata['ContractName'] or 'No name found'}"
                    )
                except Exception as e:
                    logger.warning(f"[{idx + 1}/{total}] {address}: Error - {e}")
                    result = row.to_dict()
                    result["name_tag"] = None
                    result["contract_name"] = None
                    result["is_contract"] = False
                    results_buffer.append(result)

                # Append to CSV every N rows
                if len(results_buffer) >= args.save_every:
                    append_df = pd.DataFrame(results_buffer)
                    append_df.to_csv(
                        args.output,
                        mode="a",
                        header=not Path(args.output).exists(),
                        index=False,
                    )
                    logger.info(f"Appended {len(results_buffer)} rows to {args.output}")

                    # Add newly saved addresses to processed set
                    processed_addresses.update(
                        append_df[args.address_column].astype(str).str.lower()
                    )
                    results_buffer = []

        # Append remaining results
        if results_buffer:
            append_df = pd.DataFrame(results_buffer)
            append_df.to_csv(
                args.output,
                mode="a",
                header=not Path(args.output).exists(),
                index=False,
            )
            logger.info(f"Appended final {len(results_buffer)} rows to {args.output}")

        # Read final output for summary
        final_df = pd.read_csv(args.output)
        tagged_count = final_df["name_tag"].notna().sum()
        named_count = final_df["contract_name"].notna().sum()
        logger.info(
            f"Completed: {tagged_count}/{len(final_df)} addresses tagged, {named_count}/{len(final_df)} addresses named"
        )
        logger.info(f"Total rows in output: {len(final_df)}")

    except KeyboardInterrupt:
        logger.warning("Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=args.verbose)
        sys.exit(1)


if __name__ == "__main__":
    main()
