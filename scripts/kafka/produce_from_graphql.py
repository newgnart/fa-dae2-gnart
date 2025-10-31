#!/usr/bin/env python3
"""
Kafka Producer: Poll GraphQL endpoint and publish to Kafka.

This script continuously polls a GraphQL endpoint for new blockchain data
and publishes it to a Kafka topic. It maintains state to resume from the
last processed block number.

Usage:
    # Basic usage
    uv run python scripts/el/kafka/produce_from_graphql.py

    # Custom configuration
    uv run python scripts/el/kafka/produce_from_graphql.py \
        --endpoint http://localhost:8080/v1/graphql \
        --graphql-table stablesTransfers \
        --kafka-bootstrap localhost:9092 \
        --kafka-topic stablecoin-transfers \
        --poll-interval 10 \
        -vv

    # Production mode with multiple Kafka brokers
    uv run python scripts/el/kafka/produce_from_graphql.py \
        --kafka-bootstrap broker1:9092,broker2:9092,broker3:9092 \
        --state-file /var/lib/kafka-producer/state.json \
        -v
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

from onchaindata.data_extraction.kafka_stream import GraphQLStreamKafka

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Stream GraphQL data to Kafka topic",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with defaults
  %(prog)s

  # Custom GraphQL endpoint and topic
  %(prog)s --endpoint http://api.example.com/graphql --kafka-topic my-topic

  # High-frequency polling (every 2 seconds)
  %(prog)s --poll-interval 2 -vv
        """
    )

    # GraphQL configuration
    graphql_group = parser.add_argument_group('GraphQL Options')
    graphql_group.add_argument(
        "-e", "--endpoint",
        type=str,
        default="http://localhost:8080/v1/graphql",
        help="GraphQL endpoint URL (default: %(default)s)"
    )
    graphql_group.add_argument(
        "--graphql-table",
        type=str,
        default="stablesTransfers",
        help="Name of the GraphQL table/query (default: %(default)s)"
    )
    graphql_group.add_argument(
        "--fields",
        type=str,
        default="id,blockNumber,timestamp,contractAddress,from,to,value",
        help="Comma-separated list of fields to fetch (default: %(default)s)"
    )
    graphql_group.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Seconds to wait between polls (default: %(default)s)"
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
        help="Kafka topic name (default: %(default)s)"
    )

    # State management
    state_group = parser.add_argument_group('State Management')
    state_group.add_argument(
        "--state-file",
        type=str,
        default=".kafka_stream_state.json",
        help="Path to state file for resuming (default: %(default)s)"
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

    # Parse fields
    fields = [f.strip() for f in args.fields.split(",")]

    # Log configuration
    logger.info("=" * 80)
    logger.info("Kafka Producer Configuration")
    logger.info("=" * 80)
    logger.info(f"GraphQL Endpoint:    {args.endpoint}")
    logger.info(f"GraphQL Table:       {args.graphql_table}")
    logger.info(f"Fields:              {', '.join(fields)}")
    logger.info(f"Poll Interval:       {args.poll_interval}s")
    logger.info(f"Kafka Bootstrap:     {args.kafka_bootstrap}")
    logger.info(f"Kafka Topic:         {args.kafka_topic}")
    logger.info(f"State File:          {args.state_file}")
    logger.info("=" * 80)

    # Initialize streamer
    try:
        streamer = GraphQLStreamKafka(
            endpoint=args.endpoint,
            table_name=args.graphql_table,
            fields=fields,
            poll_interval=args.poll_interval,
            kafka_bootstrap_servers=args.kafka_bootstrap,
            kafka_topic=args.kafka_topic,
        )

        # Start streaming
        logger.info("Starting streaming... Press Ctrl+C to stop")
        streamer.stream_to_kafka(state_file=args.state_file)

    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
