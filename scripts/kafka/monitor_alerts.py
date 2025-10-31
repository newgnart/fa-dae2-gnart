#!/usr/bin/env python3
"""
Alert Monitor: Detect and alert on large stablecoin transfers.

This script monitors a Kafka topic for stablecoin transfers and sends alerts
when significant events are detected (large transfers, potential depegs, etc.).

Usage:
    # Basic usage
    uv run python scripts/el/kafka/monitor_alerts.py

    # Custom thresholds
    uv run python scripts/el/kafka/monitor_alerts.py \
        --large-transfer 5000000 \
        --critical-transfer 50000000 \
        -v

    # Monitor from specific offset
    uv run python scripts/el/kafka/monitor_alerts.py \
        --from-beginning
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime

from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

logger = logging.getLogger(__name__)


class AlertMonitor:
    """
    Monitor Kafka stream for alert-worthy events.

    Detects:
    - Large transfers (> threshold)
    - Critical transfers (>> threshold)
    - Mint/burn events
    - Unusual patterns (future: ML-based anomaly detection)
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        large_threshold: float = 1_000_000,
        critical_threshold: float = 10_000_000,
    ):
        """
        Initialize alert monitor.

        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            kafka_topic: Topic to monitor
            large_threshold: USD threshold for large transfer alerts
            critical_threshold: USD threshold for critical alerts
        """
        self.large_threshold = large_threshold
        self.critical_threshold = critical_threshold

        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers.split(","),
            group_id="alert-monitor",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",  # Only process new messages
            enable_auto_commit=True,
        )

        self.stats = {
            "total_processed": 0,
            "large_transfers": 0,
            "critical_transfers": 0,
            "mints": 0,
            "burns": 0,
        }

        logger.info(f"Alert monitor initialized: {kafka_topic}")
        logger.info(f"Large transfer threshold:    ${large_threshold:,.0f}")
        logger.info(f"Critical transfer threshold: ${critical_threshold:,.0f}")

    def monitor(self):
        """
        Monitor Kafka stream and trigger alerts.

        Runs forever until interrupted.
        """
        logger.info("Monitoring for alerts... Press Ctrl+C to stop")
        print("\n" + "=" * 80)
        print("ALERT MONITOR ACTIVE")
        print("=" * 80 + "\n")

        try:
            for message in self.consumer:
                transfer = message.value
                self.stats["total_processed"] += 1

                # Check for alert conditions
                self._check_large_transfer(transfer)
                self._check_mint_burn(transfer)

                # Print stats periodically
                if self.stats["total_processed"] % 100 == 0:
                    self._print_stats()

        except KeyboardInterrupt:
            logger.info("\nMonitor stopped by user")
            self._print_stats()
        finally:
            self.consumer.close()

    def _check_large_transfer(self, transfer: dict):
        """
        Check if transfer exceeds thresholds and send alert.

        Args:
            transfer: Transfer event dictionary
        """
        # Calculate USD value
        amount_usd = self._calculate_usd_value(transfer)

        if amount_usd >= self.critical_threshold:
            self.stats["critical_transfers"] += 1
            self._send_alert(transfer, amount_usd, level="CRITICAL")
        elif amount_usd >= self.large_threshold:
            self.stats["large_transfers"] += 1
            self._send_alert(transfer, amount_usd, level="WARNING")

    def _check_mint_burn(self, transfer: dict):
        """
        Check for mint/burn events and track supply changes.

        Args:
            transfer: Transfer event dictionary
        """
        from_addr = transfer.get("from", "").lower()
        to_addr = transfer.get("to", "").lower()
        zero_addr = "0x0000000000000000000000000000000000000000"

        if from_addr == zero_addr:
            self.stats["mints"] += 1
            amount_usd = self._calculate_usd_value(transfer)
            if amount_usd >= self.large_threshold:
                self._send_mint_burn_alert(transfer, amount_usd, event_type="MINT")

        elif to_addr == zero_addr:
            self.stats["burns"] += 1
            amount_usd = self._calculate_usd_value(transfer)
            if amount_usd >= self.large_threshold:
                self._send_mint_burn_alert(transfer, amount_usd, event_type="BURN")

    def _calculate_usd_value(self, transfer: dict) -> float:
        """
        Calculate USD value of transfer.

        Args:
            transfer: Transfer event dictionary

        Returns:
            USD value (approximate)
        """
        value = transfer.get("value", 0)

        # Handle string values from GraphQL (blockchain data comes as strings)
        if isinstance(value, str):
            try:
                value = int(value)
            except ValueError:
                return 0.0

        # Hardcoded to 18 decimals (GraphQL data doesn't include decimals field)
        # In production, join with dim_stablecoin for actual decimals
        decimals = 18

        # For stablecoins, 1 token ≈ 1 USD
        return value / (10**decimals)

    def _send_alert(self, transfer: dict, amount_usd: float, level: str):
        """
        Send alert for large transfer.

        In production, integrate with:
        - Slack webhook
        - Email (SendGrid, AWS SES)
        - PagerDuty
        - Discord webhook
        - SMS (Twilio)

        Args:
            transfer: Transfer event dictionary
            amount_usd: USD value
            level: Alert level (WARNING, CRITICAL)
        """
        # Extract transaction hash from id (format: "0xtxhash_logindex")
        tx_hash = transfer.get("id", "unknown").split("_")[0]

        # Determine emoji based on level
        emoji = "🚨" if level == "CRITICAL" else "⚠️"

        print("\n" + "=" * 80)
        print(f"{emoji} {level} ALERT: Large Stablecoin Transfer Detected")
        print("=" * 80)
        print(f"Symbol:          {transfer.get('symbol', 'UNKNOWN')}")
        print(f"Amount:          ${amount_usd:,.2f}")
        print(f"From:            {transfer['from']}")
        print(f"To:              {transfer['to']}")
        print(f"Block Number:    {transfer.get('blockNumber', 'N/A')}")
        print(f"Timestamp:       {self._format_timestamp(transfer.get('timestamp'))}")
        print(f"Transaction:     {tx_hash}")
        print(f"Contract:        {transfer.get('contractAddress', 'N/A')}")
        print("=" * 80 + "\n")

        # TODO: Send to external alerting systems
        # Example:
        # requests.post(SLACK_WEBHOOK_URL, json={
        #     "text": f"{emoji} {level}: ${amount_usd:,.2f} transfer detected"
        # })

    def _send_mint_burn_alert(self, transfer: dict, amount_usd: float, event_type: str):
        """
        Send alert for significant mint/burn event.

        Args:
            transfer: Transfer event dictionary
            amount_usd: USD value
            event_type: "MINT" or "BURN"
        """
        emoji = "🟢" if event_type == "MINT" else "🔴"
        tx_hash = transfer.get("id", "unknown").split("_")[0]

        print("\n" + "=" * 80)
        print(f"{emoji} {event_type} EVENT: Large Supply Change Detected")
        print("=" * 80)
        print(f"Symbol:          {transfer.get('symbol', 'UNKNOWN')}")
        print(f"Amount:          ${amount_usd:,.2f}")
        print(
            f"Address:         {transfer.get('to' if event_type == 'MINT' else 'from', 'N/A')}"
        )
        print(f"Block Number:    {transfer.get('blockNumber', 'N/A')}")
        print(f"Transaction:     {tx_hash}")
        print("=" * 80 + "\n")

    def _format_timestamp(self, timestamp) -> str:
        """
        Format timestamp for display.

        Args:
            timestamp: Unix timestamp (seconds or milliseconds), as int/float or string

        Returns:
            Formatted datetime string
        """
        if timestamp is None:
            return "N/A"

        try:
            # Handle string timestamps from GraphQL
            if isinstance(timestamp, str):
                timestamp = float(timestamp)

            # Handle both seconds and milliseconds
            if timestamp > 1e12:  # Milliseconds
                timestamp = timestamp / 1000

            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OSError, TypeError):
            return "Invalid timestamp"

    def _print_stats(self):
        """Print monitoring statistics."""
        print("\n" + "-" * 80)
        print("MONITORING STATISTICS")
        print("-" * 80)
        print(f"Total Processed:       {self.stats['total_processed']:,}")
        print(f"Large Transfers:       {self.stats['large_transfers']:,}")
        print(f"Critical Transfers:    {self.stats['critical_transfers']:,}")
        print(f"Mints:                 {self.stats['mints']:,}")
        print(f"Burns:                 {self.stats['burns']:,}")
        print("-" * 80 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Monitor Kafka stream for alert-worthy events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with defaults
  %(prog)s

  # Custom thresholds
  %(prog)s --large-transfer 2000000 --critical-transfer 20000000

  # Start monitoring from beginning of topic
  %(prog)s --from-beginning -v
        """,
    )

    # Kafka configuration
    kafka_group = parser.add_argument_group("Kafka Options")
    kafka_group.add_argument(
        "--kafka-bootstrap",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers, comma-separated (default: %(default)s)",
    )
    kafka_group.add_argument(
        "--kafka-topic",
        type=str,
        default="stablecoin-transfers",
        help="Kafka topic to monitor (default: %(default)s)",
    )

    # Alert thresholds
    threshold_group = parser.add_argument_group("Alert Thresholds (USD)")
    threshold_group.add_argument(
        "--large-transfer",
        type=float,
        default=100_000,
        help="Threshold for large transfer warning (default: %(default)s)",
    )
    threshold_group.add_argument(
        "--critical-transfer",
        type=float,
        default=10_000_000,
        help="Threshold for critical transfer alert (default: %(default)s)",
    )

    # Logging configuration
    logging_group = parser.add_argument_group("Logging Options")
    logging_group.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity: -v for INFO, -vv for DEBUG",
    )

    args = parser.parse_args()

    # Configure logging
    if args.verbose == 0:
        log_level = logging.WARNING
    elif args.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Initialize and run monitor
    try:
        monitor = AlertMonitor(
            kafka_bootstrap_servers=args.kafka_bootstrap,
            kafka_topic=args.kafka_topic,
            large_threshold=args.large_transfer,
            critical_threshold=args.critical_transfer,
        )

        monitor.monitor()

    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
