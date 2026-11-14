"""Kafka Producer for Workshop 3: Streaming ETL with Apache Kafka and ML.

Reads the harmonized historical dataset (`data/processed/happiness_unified.csv`)
record by record, serializes each observation as a JSON event matching the
defined Event Contract, and publishes it to the `happiness-events` topic using
`country` as the Kafka partition key.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any

from confluent_kafka import Producer
from dotenv import load_dotenv
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("happiness.producer")


def build_delivery_callback():
    """Return a Confluent Kafka delivery report callback."""

    def delivery_report(err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("Delivery failed for key=%s: %s", msg.key(), err)
        else:
            key = msg.key().decode("utf-8") if msg.key() else None
            logger.debug(
                "Delivered topic=%s partition=%d offset=%d key=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                key,
            )

    return delivery_report


def row_to_event(row: pd.Series, event_id: str) -> dict[str, Any]:
    """Convert one harmonized dataset row into the Workshop-3 Event Contract JSON."""
    return {
        "event_id": event_id,
        "country": str(row["country"]),
        "year": int(row["year"]),
        "gdp": float(row["gdp"]),
        "family": float(row["family"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "generosity": float(row["generosity"]),
        "corruption": float(row["corruption"]),
        "actual_happiness_score": float(row["happiness_score"]),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    }


def build_invalid_test_messages() -> list[tuple[str | None, bytes]]:
    """Generate synthetic invalid payloads to verify consumer fault tolerance."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return [
        # 1. Malformed JSON -> expected INVALID_SCHEMA
        ("UnknownCountry", b"{invalid_json_payload"),
        # 2. Missing required feature 'gdp' -> expected INVALID_SCHEMA
        (
            "Colombia",
            json.dumps(
                {
                    "event_id": "BAD-SCHEMA-001",
                    "country": "Colombia",
                    "year": 2019,
                    "family": 0.8,
                    "health": 0.9,
                    "freedom": 0.6,
                    "generosity": 0.3,
                    "corruption": 0.1,
                    "actual_happiness_score": 6.2,
                    "event_time": now_iso,
                }
            ).encode("utf-8"),
        ),
        # 3. Negative indicator & invalid timestamp -> expected INVALID_VALUES
        (
            "Colombia",
            json.dumps(
                {
                    "event_id": "BAD-VALUES-001",
                    "country": "Colombia",
                    "year": 2019,
                    "gdp": -1.5,
                    "family": 0.8,
                    "health": 0.9,
                    "freedom": 0.6,
                    "generosity": 0.3,
                    "corruption": 0.1,
                    "actual_happiness_score": 6.2,
                    "event_time": "not-a-timestamp",
                }
            ).encode("utf-8"),
        ),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream harmonized World Happiness records to Kafka."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed" / "happiness_unified.csv",
        help="Path to the harmonized CSV file.",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "happiness-events"),
        help="Target Kafka topic name.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=float(os.getenv("PRODUCER_DELAY_SECONDS", "0.25")),
        help="Simulated delay in seconds between emitted events.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of valid records to stream (useful for quick tests).",
    )
    parser.add_argument(
        "--include-invalid",
        action="store_true",
        help="Also emit 3 invalid test events at the end to verify consumer resilience.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.input.exists():
        logger.error(
            "Input file not found: %s. Run notebooks/eda.ipynb first.", args.input
        )
        return 1

    df = pd.read_csv(args.input)
    if args.limit is not None:
        df = df.head(args.limit)

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "acks": "all",
            "retries": 5,
            "linger.ms": 5,
        }
    )
    callback = build_delivery_callback()
    run_tag = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    logger.info(
        "Starting producer -> topic='%s' | bootstrap='%s' | records=%d | delay=%.2fs",
        args.topic,
        args.bootstrap_servers,
        len(df),
        args.delay,
    )

    sent_count = 0
    try:
        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            event_id = f"E{idx:04d}-{run_tag}"
            payload = row_to_event(row, event_id=event_id)
            key_bytes = payload["country"].encode("utf-8")
            value_bytes = json.dumps(payload).encode("utf-8")

            producer.produce(
                topic=args.topic,
                key=key_bytes,
                value=value_bytes,
                on_delivery=callback,
            )
            producer.poll(0)
            sent_count += 1

            if sent_count % 25 == 0 or sent_count == len(df):
                logger.info(
                    "Sent %d/%d events (last: event_id=%s, country=%s, year=%d)",
                    sent_count,
                    len(df),
                    event_id,
                    payload["country"],
                    payload["year"],
                )

            if args.delay > 0:
                time.sleep(args.delay)

        if args.include_invalid:
            logger.info("Emitting 3 synthetic invalid events for fault-tolerance test...")
            for test_key, raw_bytes in build_invalid_test_messages():
                producer.produce(
                    topic=args.topic,
                    key=test_key.encode("utf-8") if test_key else None,
                    value=raw_bytes,
                    on_delivery=callback,
                )
                producer.poll(0)
                sent_count += 1
                if args.delay > 0:
                    time.sleep(args.delay)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user after %d events.", sent_count)
    finally:
        remaining = producer.flush(timeout=15.0)
        if remaining > 0:
            logger.warning("%d buffered messages were not delivered before timeout.", remaining)
        logger.info("Producer finished. Total emitted: %d events.", sent_count)

    return 0


if __name__ == "__main__":
    sys.exit(main())
