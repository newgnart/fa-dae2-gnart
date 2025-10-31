#!/usr/bin/env python3
"""
Kafka consumer that writes to PostgreSQL in batches.

This module provides a consumer that reads from Kafka topics and writes
to PostgreSQL using the Loader abstraction, with batching for performance.
"""

import json
import logging
import time
from typing import List, Optional

from kafka import KafkaConsumer
import polars as pl

from .loaders import Loader

logger = logging.getLogger(__name__)


class KafkaToPostgresConsumer:
    """
    Consumes messages from Kafka and writes to PostgreSQL in batches.

    Features:
    - Batch writes for performance (collect N messages or wait T seconds)
    - Manual offset commit after successful DB write (at-least-once delivery)
    - Automatic recovery from last committed offset
    - Error handling with dead letter queue support

    Example:
        >>> from onchaindata.utils import PostgresClient
        >>> client = PostgresClient.from_env()
        >>> loader = Loader(client=client)
        >>> consumer = KafkaToPostgresConsumer(
        ...     kafka_bootstrap_servers="localhost:9092",
        ...     kafka_topic="stablecoin-transfers",
        ...     kafka_group_id="postgres-sink",
        ...     loader=loader,
        ...     schema="raw",
        ...     table_name="transfers_kafka"
        ... )
        >>> consumer.consume_and_load()
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        kafka_group_id: str,
        loader: Loader,
        schema: str,
        table_name: str,
        batch_size: int = 100,
        batch_timeout_ms: int = 5000,
    ):
        """
        Initialize consumer.

        Args:
            kafka_bootstrap_servers: Kafka broker addresses (comma-separated)
            kafka_topic: Topic to consume from
            kafka_group_id: Consumer group ID (for offset tracking)
            loader: Loader instance for database writes
            schema: Target database schema
            table_name: Target table name
            batch_size: Number of messages to batch before writing
            batch_timeout_ms: Max wait time for batch (milliseconds)
        """
        self.loader = loader
        self.schema = schema
        self.table_name = table_name
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms

        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers.split(','),
            group_id=kafka_group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            # Reliability settings
            enable_auto_commit=False,  # Manual commit after successful write
            auto_offset_reset='earliest',  # Start from beginning if no offset
            # Performance settings
            fetch_min_bytes=1024,  # Wait for at least 1KB of data
            fetch_max_wait_ms=500,  # But don't wait more than 500ms
            max_poll_records=500,  # Fetch up to 500 records per poll
        )

        logger.info(f"Consumer initialized: {kafka_topic} → {schema}.{table_name}")
        logger.info(f"Consumer group: {kafka_group_id}, batch size: {batch_size}")

    def consume_and_load(self):
        """
        Consume messages and load to PostgreSQL in batches.

        Runs forever until interrupted (Ctrl+C).
        """
        batch = []
        batch_start_time = None

        try:
            for message in self.consumer:
                # Add message to batch
                batch.append(message.value)

                if batch_start_time is None:
                    batch_start_time = time.time()

                # Check if we should flush batch
                batch_elapsed_ms = (time.time() - batch_start_time) * 1000
                should_flush = (
                    len(batch) >= self.batch_size
                    or batch_elapsed_ms >= self.batch_timeout_ms
                )

                if should_flush:
                    self._flush_batch(batch)

                    # Commit Kafka offsets after successful write
                    self.consumer.commit()

                    # Reset batch
                    batch = []
                    batch_start_time = None

        except KeyboardInterrupt:
            logger.info("\nConsumer stopped by user")
            # Flush any remaining messages
            if batch:
                logger.info("Flushing remaining messages...")
                self._flush_batch(batch)
                self.consumer.commit()
        finally:
            self.consumer.close()
            logger.info("Kafka consumer closed")

    def _flush_batch(self, batch: List[dict]):
        """
        Write batch of messages to PostgreSQL.

        Args:
            batch: List of message dictionaries

        Raises:
            Exception: If database write fails
        """
        if not batch:
            return

        try:
            # Convert to Polars DataFrame
            df = pl.DataFrame(batch)

            # Remove Kafka metadata columns (optional)
            metadata_cols = ["_kafka_timestamp", "_source", "_producer_version"]
            for col in metadata_cols:
                if col in df.columns:
                    df = df.drop(col)

            # Load to database
            self.loader.load_dataframe(
                df=df,
                schema=self.schema,
                table_name=self.table_name,
                write_disposition="append"
            )

            logger.info(f"Loaded {len(batch)} records to {self.schema}.{self.table_name}")

        except Exception as e:
            logger.error(f"Failed to load batch: {e}")
            # TODO: Implement dead letter queue for failed batches
            # For now, we re-raise to prevent offset commit
            raise


class KafkaMetricsConsumer:
    """
    Consumes messages from Kafka and calculates real-time metrics.

    This consumer computes aggregations on the fly without writing to database.
    Useful for monitoring dashboards and alerting.

    Example:
        >>> consumer = KafkaMetricsConsumer(
        ...     kafka_bootstrap_servers="localhost:9092",
        ...     kafka_topic="stablecoin-transfers",
        ...     kafka_group_id="metrics-calculator",
        ...     window_size_seconds=60
        ... )
        >>> consumer.consume_and_aggregate()
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        kafka_group_id: str,
        window_size_seconds: int = 60,
    ):
        """
        Initialize metrics consumer.

        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            kafka_topic: Topic to consume from
            kafka_group_id: Consumer group ID
            window_size_seconds: Time window for aggregations
        """
        self.window_size_seconds = window_size_seconds
        self.metrics = {
            "count": 0,
            "total_volume": 0,
            "by_symbol": {},
            "window_start": time.time()
        }

        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers.split(','),
            group_id=kafka_group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',  # Only process new messages
        )

        logger.info(f"Metrics consumer initialized: {kafka_topic}")

    def consume_and_aggregate(self):
        """
        Consume messages and calculate rolling metrics.

        Prints metrics summary every window_size_seconds.
        """
        try:
            for message in self.consumer:
                transfer = message.value

                # Update metrics
                self._update_metrics(transfer)

                # Check if window elapsed
                if time.time() - self.metrics["window_start"] >= self.window_size_seconds:
                    self._print_metrics()
                    self._reset_metrics()

        except KeyboardInterrupt:
            logger.info("\nMetrics consumer stopped by user")
            self._print_metrics()
        finally:
            self.consumer.close()

    def _update_metrics(self, transfer: dict):
        """Update metrics with new transfer."""
        self.metrics["count"] += 1

        # Extract value (handle different decimal formats)
        value = transfer.get("value", 0)
        if isinstance(value, str):
            try:
                value = int(value) / 1e18  # Assume 18 decimals
            except ValueError:
                value = 0

        self.metrics["total_volume"] += value

        # Update by-symbol metrics
        symbol = transfer.get("symbol", "UNKNOWN")
        if symbol not in self.metrics["by_symbol"]:
            self.metrics["by_symbol"][symbol] = {"count": 0, "volume": 0}

        self.metrics["by_symbol"][symbol]["count"] += 1
        self.metrics["by_symbol"][symbol]["volume"] += value

    def _print_metrics(self):
        """Print current metrics summary."""
        elapsed = time.time() - self.metrics["window_start"]

        print("\n" + "="*80)
        print(f"Metrics Summary (last {elapsed:.1f}s)")
        print("="*80)
        print(f"Total Transfers: {self.metrics['count']}")
        print(f"Total Volume:    ${self.metrics['total_volume']:,.2f}")
        print(f"Avg per second:  {self.metrics['count'] / elapsed:.2f} transfers/sec")
        print("\nBy Stablecoin:")
        for symbol, data in sorted(
            self.metrics["by_symbol"].items(),
            key=lambda x: x[1]["volume"],
            reverse=True
        ):
            print(f"  {symbol:10s} | {data['count']:6d} transfers | ${data['volume']:>15,.2f}")
        print("="*80 + "\n")

    def _reset_metrics(self):
        """Reset metrics for next window."""
        self.metrics = {
            "count": 0,
            "total_volume": 0,
            "by_symbol": {},
            "window_start": time.time()
        }
