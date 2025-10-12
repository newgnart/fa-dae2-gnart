import json, argparse, logging, os
import logging.handlers

from pathlib import Path
from re import A
from dotenv import load_dotenv
import polars as pl
import pandas as pd
from onchaindata.data_extraction.etherscan import etherscan_to_parquet, EtherscanClient

load_dotenv()

logger = logging.getLogger(__name__)


def parse_number_with_suffix(value: str) -> int:
    """Parse numbers with K/M/B suffixes (e.g., '18.5M' -> 18500000).

    Args:
        value: String number that may include K (thousand), M (million), or B (billion) suffix

    Returns:
        Integer value

    Examples:
        '18.5M' -> 18500000
        '1.2K' -> 1200
        '3B' -> 3000000000
        '1000' -> 1000
    """
    value = str(value).strip().upper()

    suffixes = {
        "K": 1_000,
        "M": 1_000_000,
        "B": 1_000_000_000,
    }

    for suffix, multiplier in suffixes.items():
        if value.endswith(suffix):
            number_part = value[:-1]
            return int(float(number_part) * multiplier)

    # No suffix, parse as regular int
    return int(value)


def setup_logging(log_filename: str = None, level: str = "INFO"):
    """Sets up logging with console streaming and optional file logging."""
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Console handler (always present)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (optional)
    if not Path("logging").exists():
        Path("logging").mkdir(parents=True, exist_ok=True)
    if log_filename:
        file_handler = logging.handlers.RotatingFileHandler(
            f"logging/{log_filename}", maxBytes=5 * 1024 * 1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def extract_with_retry(
    address: str,
    etherscan_client: EtherscanClient,
    chain: str,
    table: str,
    from_block: int,
    to_block: int,
    output_dir: Path,
    max_retries: int = 3,
):
    """Extract logs or transactions with automatic retry on failures.

    Args:
        address: The contract address to extract data from
        etherscan_client: Initialized EtherscanClient instance
        chain: The blockchain network name
        table: Either 'logs' or 'transactions'
        from_block: Starting block number
        to_block: Ending block number
        output_dir: Directory to save output parquet files
        max_retries: Maximum number of retry attempts for failed blocks
    """
    output_path = (
        output_dir / f"{chain}_{address}_{table}_{from_block}_{to_block}.parquet"
    )
    setup_logging(log_filename=f"extract_{chain}_{address}_{table}.log")

    etherscan_to_parquet(
        address=address,
        etherscan_client=etherscan_client,
        table=table,
        from_block=from_block,
        to_block=to_block,
        output_path=output_path,
    )

    # Retry failed blocks if error file exists
    error_file = Path(f"logging/extract_error/{chain}_{address}_{table}.csv")
    retries = 0
    while error_file.exists() and retries < max_retries:
        logger.info(f"Retrying failed blocks (attempt {retries + 1}/{max_retries})")
        retry_failed_blocks(
            error_file=error_file,
            table=table,
            output_path=output_path,
        )
        retries += 1
    n = pl.scan_parquet(output_path).select(pl.len()).collect().item()
    logger.info(f"{chain} - {address} - {table} - {from_block}-{to_block}, {n} ✅")


def retry_failed_blocks(error_file: Path, table: str, output_path: Path):
    """Retry failed block ranges with smaller chunk size."""

    df = pd.read_csv(error_file)

    # Create resolved directory if it doesn't exist
    resolved_dir = error_file.parent / "resolved"
    resolved_dir.mkdir(parents=True, exist_ok=True)
    # Generate resolved filename by adding timestamp
    resolved_file_path = resolved_dir / f"{error_file.stem}_resolved.csv"
    # Save resolved error file
    df.to_csv(resolved_file_path, index=False)
    os.remove(error_file)

    for _, row in df.iterrows():
        chain = row.chain
        etherscan_client = EtherscanClient(chain=chain)
        address = row.address

        from_block = row.from_block
        to_block = row.to_block
        block_chunk_size = int(row.block_chunk_size / 10)

        etherscan_to_parquet(
            address=address,
            etherscan_client=etherscan_client,
            from_block=from_block,
            to_block=to_block,
            block_chunk_size=block_chunk_size,
            table=table,
            output_path=output_path,
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--chain",
        type=str,
        default="ethereum",
        help="Chain name",
    )
    parser.add_argument(
        "--address",
        type=str,
        help="Address",
    )
    parser.add_argument(
        "--from_block",
        type=parse_number_with_suffix,
        help="From block (supports K/M/B suffixes, e.g., '18.5M')",
    )
    parser.add_argument(
        "--to_block",
        type=parse_number_with_suffix,
        help="To block (supports K/M/B suffixes, e.g., '20M')",
    )
    parser.add_argument(
        "--logs",
        action="store_true",
        help="Extract logs",
    )
    parser.add_argument(
        "--transactions",
        action="store_true",
        help="Extract transactions",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=".data/etherscan_raw",
        help="Output directory",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    etherscan_client = EtherscanClient(chain=args.chain)
    from_block = args.from_block or etherscan_client.get_contract_creation_block_number(
        args.address
    )
    to_block = args.to_block or etherscan_client.get_latest_block()

    if args.logs:
        extract_with_retry(
            address=args.address.lower(),
            etherscan_client=etherscan_client,
            chain=args.chain,
            table="logs",
            from_block=from_block,
            to_block=to_block,
            output_dir=output_dir,
        )

    if args.transactions:
        extract_with_retry(
            address=args.address.lower(),
            etherscan_client=etherscan_client,
            chain=args.chain,
            table="transactions",
            from_block=from_block,
            to_block=to_block,
            output_dir=output_dir,
        )


if __name__ == "__main__":
    main()
