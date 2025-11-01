#!/usr/bin/env python3
"""
Kafka-enhanced GraphQL streaming.

This module extends GraphQLStream to publish data to Kafka topics instead of
direct database writes, enabling event-driven architecture patterns.
"""

import json
import logging
import time
from typing import Optional, List
from pathlib import Path

from kafka import KafkaProducer
import polars as pl

from .graphql import GraphQLStream, GraphQLBatch

logger = logging.getLogger(__name__)


class GraphQLStreamKafka(GraphQLStream):
    """
    Enhanced GraphQL streamer that publishes to Kafka instead of direct DB write.

    Inherits all polling logic from GraphQLStream, but changes the sink to Kafka.
    This enables:
    - Decoupling of data ingestion from processing
    - Multiple consumers reading the same stream
    - Replay capability for reprocessing
    - Backpressure handling via Kafka buffering

    Example:
        >>> streamer = GraphQLStreamKafka(
        ...     endpoint="http://localhost:8080/v1/graphql",
        ...     table_name="stablesTransfers",
        ...     fields=["id", "blockNumber", "from", "to", "value"],
        ...     kafka_bootstrap_servers="localhost:9092",
        ...     kafka_topic="stablecoin-transfers"
        ... )
        >>> streamer.stream_to_kafka()
    """

    def __init__(
        self,
        endpoint: str,
        table_name: str,
        fields: List[str],
        poll_interval: int = 5,
        kafka_bootstrap_servers: str = "localhost:9092",
        kafka_topic: str = "stablecoin-transfers",
    ):
        """
        Initialize Kafka-enabled streaming fetcher.

        Args:
            endpoint: GraphQL endpoint URL
            table_name: Name of the table/query to fetch
            fields: List of fields to fetch
            poll_interval: Seconds to wait between polls
            kafka_bootstrap_servers: Kafka broker addresses (comma-separated)
            kafka_topic: Target Kafka topic name
        """
        super().__init__(endpoint, table_name, fields, poll_interval)

        self.kafka_topic = kafka_topic

        # Initialize Kafka producer with sensible defaults
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            # Performance tuning
            compression_type='gzip',  # Reduce network bandwidth
            batch_size=16384,  # Batch messages for throughput
            linger_ms=10,  # Wait 10ms for batching opportunities
            # Reliability settings
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,  # Retry failed sends
            max_in_flight_requests_per_connection=5,
        )

        logger.info(f"Kafka producer initialized: {kafka_bootstrap_servers} → {kafka_topic}")

    def stream_to_kafka(self, state_file: str = ".kafka_stream_state.json"):
        """
        Stream data from GraphQL endpoint to Kafka topic.

        Maintains state in a local file to resume from last seen block number.
        This provides fault tolerance - if the process crashes, it can resume
        from where it left off.

        Args:
            state_file: Path to JSON file storing last processed block number
        """
        logger.info(f"Starting Kafka streaming mode: {self.endpoint} → {self.kafka_topic}")

        # Load last seen block from state file (for recovery)
        self.last_seen_block_number = self._load_state(state_file)
        if self.last_seen_block_number is not None:
            logger.info(f"Resuming from block number: {self.last_seen_block_number}")
        else:
            logger.info("No previous state found, starting fresh")

        poll_count = 1
        total_records = 0

        try:
            while True:
                # Build query with current state
                where_clause = None
                if self.last_seen_block_number is not None:
                    where_clause = f"blockNumber: {{_gt: {self.last_seen_block_number}}}"

                query = self._build_query(where_clause)
                extractor = GraphQLBatch(endpoint=self.endpoint, query=query)

                # Fetch data
                df = extractor.extract_to_dataframe(self.table_name)

                if not df.is_empty():
                    records_count = len(df)
                    total_records += records_count

                    # Publish each record to Kafka
                    for record in df.iter_rows(named=True):
                        self._publish_to_kafka(record)

                    # Flush producer to ensure all messages are sent
                    self.producer.flush()

                    # Update last seen block number
                    if "blockNumber" in df.columns:
                        self.last_seen_block_number = df["blockNumber"].max()
                        self._save_state(state_file)  # Persist state

                        logger.info(
                            f"[Poll {poll_count}] Published {records_count} records to Kafka, "
                            f"max block: {self.last_seen_block_number}"
                        )

                    poll_count += 1
                else:
                    logger.debug(f"[Poll {poll_count}] No new records, waiting...")
                    poll_count += 1

                # Wait before next poll
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            logger.info("\nStreaming stopped by user")
            logger.info(f"Total polls: {poll_count}, Total records: {total_records}")
        finally:
            # Flush any pending messages and close producer
            logger.info("Flushing pending messages...")
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")

    def _publish_to_kafka(self, record: dict):
        """
        Publish a single record to Kafka.

        Uses contract_address as partition key to ensure ordering guarantees:
        - All events for the same stablecoin go to the same partition
        - Events within a partition are strictly ordered

        Args:
            record: Dictionary containing transfer data
        """
        # Use contract address as key for partitioning
        key = record.get("contractAddress") or record.get("contract_address") or "unknown"

        # Add metadata for tracking and debugging
        enriched_record = {
            **record,
            "_kafka_timestamp": int(time.time() * 1000),  # milliseconds
            "_source": "graphql_stream",
            "_producer_version": "1.0.0"
        }

        # Publish (async, non-blocking)
        future = self.producer.send(
            self.kafka_topic,
            key=key,
            value=enriched_record
        )

        # Add callback for error handling
        future.add_errback(self._on_send_error)

    def _on_send_error(self, exc):
        """Handle Kafka send errors."""
        logger.error(f"Failed to send message to Kafka: {exc}")
        # In production, you might want to:
        # - Write to a dead letter queue
        # - Send alert to monitoring system
        # - Increment error metrics

    def _load_state(self, state_file: str) -> Optional[int]:
        """
        Load last seen block number from state file.

        Args:
            state_file: Path to state file

        Returns:
            Last processed block number, or None if file doesn't exist
        """
        path = Path(state_file)
        if path.exists():
            try:
                with open(path) as f:
                    state = json.load(f)
                    return state.get("last_block_number")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load state file: {e}")
                return None
        return None

    def _save_state(self, state_file: str):
        """
        Save last seen block number to state file.

        Args:
            state_file: Path to state file
        """
        state = {
            "last_block_number": self.last_seen_block_number,
            "last_updated": time.time(),
            "topic": self.kafka_topic
        }
        try:
            with open(state_file, "w") as f:
                json.dump(state, f, indent=2)
        except IOError as e:
            logger.error(f"Failed to save state file: {e}")
