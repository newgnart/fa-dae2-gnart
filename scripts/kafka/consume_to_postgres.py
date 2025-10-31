#!/usr/bin/env python3
"""
Kafka Consumer: Read from Kafka and write to PostgreSQL.

This script consumes messages from a Kafka topic and writes them to PostgreSQL
in batches for optimal performance. It tracks consumer offsets to ensure
at-least-once delivery semantics.

Usage:
    # Basic usage
    uv run python scripts/el/kafka/consume_to_postgres.py

    # Custom configuration
    uv run python scripts/el/kafka/consume_to_postgres.py \
        --kafka-bootstrap localhost:9092 \
        --kafka-topic stablecoin-transfers \
        --kafka-group postgres-sink-1 \
        --schema raw \
        --table transfers_kafka \
        --batch-size 200 \
        -vv

    # Multiple consumers in same group (load balancing)
    # Terminal 1:
    uv run python scripts/el/kafka/consume_to_postgres.py --kafka-group shared-group
    # Terminal 2:
    uv run python scripts/el/kafka/consume_to_postgres.py --kafka-group shared-group
"""

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from onchaindata.data_pipeline import Loader
from onchaindata.data_pipeline.kafka_consumer import KafkaToPostgresConsumer
from onchaindata.utils import PostgresClient

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Consume Kafka messages and write to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with defaults
  %(prog)s

  # Custom schema and table
  %(prog)s --schema staging --table stablecoin_transfers

  # Performance tuning for high throughput
  %(prog)s --batch-size 500 --batch-timeout-ms 2000

  # Multiple consumers for parallel processing (same group ID)
  %(prog)s --kafka-group my-group  # Run this in multiple terminals
        """
    )

    # Kafka configuration
    kafka_group = parser.add_argument_group('Kafka Options')
    kafka_group.add_argument(
        "--kafka-bootstrap",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers, comma-separated (default: %(default)s)"
    )
    kafka_group.add_argument(
        "--kafka-topic",
        type=str,
        default="stablecoin-transfers",
        help="Kafka topic to consume from (default: %(default)s)"
    )
    kafka_group.add_argument(
        "--kafka-group",
        type=str,
        default="postgres-sink",
        help="Consumer group ID (default: %(default)s)"
    )

    # Database configuration
    db_group = parser.add_argument_group('Database Options')
    db_group.add_argument(
        "-s", "--schema",
        type=str,
        default="raw",
        help="Target database schema (default: %(default)s)"
    )
    db_group.add_argument(
        "-t", "--table",
        type=str,
        default="transfers_kafka",
        help="Target table name (default: %(default)s)"
    )

    # Performance tuning
    perf_group = parser.add_argument_group('Performance Options')
    perf_group.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of messages to batch before writing (default: %(default)s)"
    )
    perf_group.add_argument(
        "--batch-timeout-ms",
        type=int,
        default=5000,
        help="Max milliseconds to wait for batch completion (default: %(default)s)"
    )

    # Logging configuration
    logging_group = parser.add_argument_group('Logging Options')
    logging_group.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase verbosity: -v for INFO, -vv for DEBUG"
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
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Log configuration
    logger.info("=" * 80)
    logger.info("Kafka Consumer Configuration")
    logger.info("=" * 80)
    logger.info(f"Kafka Bootstrap:     {args.kafka_bootstrap}")
    logger.info(f"Kafka Topic:         {args.kafka_topic}")
    logger.info(f"Consumer Group:      {args.kafka_group}")
    logger.info(f"Target:              {args.schema}.{args.table}")
    logger.info(f"Batch Size:          {args.batch_size}")
    logger.info(f"Batch Timeout:       {args.batch_timeout_ms}ms")
    logger.info("=" * 80)

    # Initialize database client and loader
    try:
        logger.info("Connecting to PostgreSQL...")
        client = PostgresClient.from_env()
        loader = Loader(client=client)
        logger.info("PostgreSQL connection established")

        # Initialize consumer
        consumer = KafkaToPostgresConsumer(
            kafka_bootstrap_servers=args.kafka_bootstrap,
            kafka_topic=args.kafka_topic,
            kafka_group_id=args.kafka_group,
            loader=loader,
            schema=args.schema,
            table_name=args.table,
            batch_size=args.batch_size,
            batch_timeout_ms=args.batch_timeout_ms,
        )

        # Start consuming
        logger.info("Starting consumer... Press Ctrl+C to stop")
        consumer.consume_and_load()

    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
