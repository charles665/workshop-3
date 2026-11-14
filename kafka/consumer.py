"""Kafka Consumer for Workshop 3: Streaming ETL with Apache Kafka and ML.

Subscribes to `happiness-events` using consumer group `happiness-ml-team` and
executes the fault-tolerant real-time inference pipeline:
1. Receive streaming message from Kafka.
2. Persist the exact raw message into `raw_happiness_events` BEFORE validation.
3. Validate the Event Contract (required fields, data types, numeric bounds,
   valid ISO-8601 timestamp, and required model features).
4. Apply the exact training feature ordering and run inference via `model.pkl`.
5. Persist dimensions (`dim_country`, `dim_date`) and `fact_predictions` linked
   by `raw_event_id`, updating `raw_happiness_events.processing_status`.
6. Invalid or failing events are marked appropriately and skipped without
   crashing the consumer loop.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import logging
import math
import os
from pathlib import Path
import signal
import sys
from typing import Any

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import joblib
import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PgConnection

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("happiness.consumer")

DEFAULT_MODEL_FEATURES = [
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
]

REQUIRED_EVENT_FIELDS = {
    "event_id",
    "country",
    "year",
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
    "actual_happiness_score",
    "event_time",
}

NUMERIC_FIELDS = [
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
    "actual_happiness_score",
]


class EventContractError(Exception):
    """Raised when an incoming Kafka payload violates the Event Contract."""

    def __init__(self, status: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def _is_strict_int(value: Any) -> bool:
    """Return True only for integers (explicitly rejecting bool)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_strict_number(value: Any) -> bool:
    """Return True only for real int/float numbers (explicitly rejecting bool)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_event_contract(
    payload: Any, required_model_features: list[str]
) -> tuple[dict[str, Any], datetime]:
    """Validate incoming payload against the Workshop-3 Event Contract.

    Returns the normalized payload dictionary and parsed `event_time` datetime.
    Raises `EventContractError` with `INVALID_SCHEMA` or `INVALID_VALUES`.
    """
    if not isinstance(payload, dict):
        raise EventContractError("INVALID_SCHEMA", "Payload must be a JSON object.")

    expected_fields = REQUIRED_EVENT_FIELDS | set(required_model_features)
    missing_fields = sorted(expected_fields - set(payload.keys()))
    if missing_fields:
        raise EventContractError(
            "INVALID_SCHEMA",
            f"Missing required fields: {', '.join(missing_fields)}",
        )

    # Type checks (schema level)
    for str_field in ("event_id", "country", "event_time"):
        if not isinstance(payload[str_field], str):
            raise EventContractError(
                "INVALID_SCHEMA", f"Field '{str_field}' must be a string."
            )

    if not _is_strict_int(payload["year"]):
        raise EventContractError("INVALID_SCHEMA", "Field 'year' must be an integer.")

    for num_field in set(NUMERIC_FIELDS) | set(required_model_features):
        if not _is_strict_number(payload[num_field]):
            raise EventContractError(
                "INVALID_SCHEMA", f"Field '{num_field}' must be numeric."
            )

    # Value checks (domain & format level)
    if not payload["event_id"].strip():
        raise EventContractError("INVALID_VALUES", "Field 'event_id' cannot be empty.")
    if not payload["country"].strip():
        raise EventContractError("INVALID_VALUES", "Field 'country' cannot be empty.")

    year_val = int(payload["year"])
    if not (1900 <= year_val <= 2100):
        raise EventContractError(
            "INVALID_VALUES", f"Field 'year' out of expected range: {year_val}"
        )

    for num_field in set(NUMERIC_FIELDS) | set(required_model_features):
        val = float(payload[num_field])
        if not math.isfinite(val) or val < 0.0:
            raise EventContractError(
                "INVALID_VALUES",
                f"Field '{num_field}' must be finite and >= 0 (got {payload[num_field]!r}).",
            )

    try:
        # Python 3.11+ handles trailing 'Z' as UTC in datetime.fromisoformat
        parsed_dt = datetime.fromisoformat(payload["event_time"])
        if parsed_dt.tzinfo is not None:
            parsed_dt = parsed_dt.replace(tzinfo=None)
    except ValueError as exc:
        raise EventContractError(
            "INVALID_VALUES",
            f"Invalid ISO-8601 timestamp in 'event_time': {payload['event_time']!r}",
        ) from exc

    return payload, parsed_dt


def get_pg_connection() -> PgConnection:
    """Create a PostgreSQL connection using environment variables."""
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = int(os.getenv("POSTGRES_PORT", "5433"))
    dbname = os.getenv("POSTGRES_DB", "happiness_db")
    user = os.getenv("POSTGRES_USER", "etl_user")
    password = os.getenv("POSTGRES_PASSWORD", "etl_password")

    try:
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
    except UnicodeDecodeError as exc:
        raise RuntimeError(
            f"Connection to PostgreSQL ({host}:{port}/{dbname} as {user}) failed with a "
            "localized Windows/libpq error message. Verify that Docker 'workshop3-postgres' "
            "is running and that POSTGRES_PORT matches the Docker port mapping (5433), "
            "not a local Windows PostgreSQL service on 5432."
        ) from exc


def insert_raw_event(
    conn: PgConnection,
    topic: str,
    partition: int,
    offset: int,
    event_key: str | None,
    raw_message: str,
    parsed_json: dict[str, Any] | None,
) -> int:
    """Persist the raw Kafka message immediately before validation or prediction."""
    event_id = None
    if isinstance(parsed_json, dict) and isinstance(parsed_json.get("event_id"), str):
        event_id = parsed_json["event_id"][:128]

    raw_payload_str = json.dumps(parsed_json) if parsed_json is not None else None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_happiness_events (
                event_id,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                event_key,
                raw_message,
                raw_payload,
                processing_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, 'PENDING')
            RETURNING raw_event_id;
            """,
            (
                event_id,
                topic,
                partition,
                offset,
                event_key,
                raw_message,
                raw_payload_str,
            ),
        )
        raw_event_id = int(cur.fetchone()[0])
    conn.commit()
    return raw_event_id


def update_raw_status(
    conn: PgConnection,
    raw_event_id: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """Update the processing status of a stored raw event."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raw_happiness_events
            SET processing_status = %s,
                error_message = %s,
                processed_at = NOW()
            WHERE raw_event_id = %s;
            """,
            (status, error_message, raw_event_id),
        )
    conn.commit()


def upsert_country(cur: Any, country_name: str) -> int:
    """Get or create a country_id in dim_country."""
    cur.execute(
        """
        INSERT INTO dim_country (country_name)
        VALUES (%s)
        ON CONFLICT (country_name)
        DO UPDATE SET country_name = EXCLUDED.country_name
        RETURNING country_id;
        """,
        (country_name.strip(),),
    )
    return int(cur.fetchone()[0])


def upsert_date(cur: Any, event_dt: datetime, observation_year: int) -> int:
    """Get or create a date_id in dim_date."""
    cur.execute(
        """
        INSERT INTO dim_date (
            event_timestamp,
            full_date,
            event_year,
            event_month,
            event_day,
            observation_year
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_timestamp, observation_year)
        DO UPDATE SET full_date = EXCLUDED.full_date
        RETURNING date_id;
        """,
        (
            event_dt,
            event_dt.date(),
            event_dt.year,
            event_dt.month,
            event_dt.day,
            observation_year,
        ),
    )
    return int(cur.fetchone()[0])


def persist_valid_prediction(
    conn: PgConnection,
    raw_event_id: int,
    country_name: str,
    event_dt: datetime,
    observation_year: int,
    actual_score: float,
    predicted_score: float,
    prediction_error: float,
) -> int:
    """Persist dimensions, fact_predictions row, and mark raw event VALID atomically."""
    with conn.cursor() as cur:
        country_id = upsert_country(cur, country_name)
        date_id = upsert_date(cur, event_dt, observation_year)
        cur.execute(
            """
            INSERT INTO fact_predictions (
                raw_event_id,
                country_id,
                date_id,
                actual_score,
                predicted_score,
                prediction_error
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING prediction_id;
            """,
            (
                raw_event_id,
                country_id,
                date_id,
                actual_score,
                predicted_score,
                prediction_error,
            ),
        )
        prediction_id = int(cur.fetchone()[0])

        cur.execute(
            """
            UPDATE raw_happiness_events
            SET processing_status = 'VALID',
                error_message = NULL,
                processed_at = NOW()
            WHERE raw_event_id = %s;
            """,
            (raw_event_id,),
        )
    conn.commit()
    return prediction_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume happiness-events from Kafka and run real-time ML inference."
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "happiness-events"),
        help="Kafka topic to consume from.",
    )
    parser.add_argument(
        "--group-id",
        default=os.getenv("KAFKA_CONSUMER_GROUP", "happiness-ml-team"),
        help="Kafka consumer group ID.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        default=PROJECT_ROOT / "models" / "model.pkl",
        help="Path to the serialized scikit-learn Pipeline.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.model_path.exists():
        logger.error(
            "Model file not found: %s. Run notebooks/model_training.ipynb first.",
            args.model_path,
        )
        return 1

    model = joblib.load(args.model_path)
    model_features = (
        list(model.feature_names_in_)
        if hasattr(model, "feature_names_in_")
        else list(DEFAULT_MODEL_FEATURES)
    )
    logger.info("Loaded model from %s | features=%s", args.model_path, model_features)

    conn = get_pg_connection()
    logger.info("Connected to PostgreSQL database '%s'.", os.getenv("POSTGRES_DB", "happiness_db"))

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])
    logger.info(
        "Subscribed to topic='%s' with group.id='%s'. Waiting for events...",
        args.topic,
        args.group_id,
    )

    running = True

    def handle_shutdown(signum: int, _frame: Any) -> None:
        nonlocal running
        logger.info("Received signal %d. Shutting down consumer gracefully...", signum)
        running = False

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    stats = {"valid": 0, "invalid": 0, "error": 0}

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka consumer error: %s", msg.error())
                continue

            event_key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
            raw_bytes = msg.value() or b""
            raw_message = raw_bytes.decode("utf-8", errors="replace")

            # Attempt JSON parsing only to enrich raw_payload JSONB; raw_message is always preserved.
            parsed_json: dict[str, Any] | None = None
            json_parse_error: str | None = None
            try:
                loaded = json.loads(raw_message)
                if isinstance(loaded, dict):
                    parsed_json = loaded
                else:
                    json_parse_error = "Incoming JSON root must be an object."
            except json.JSONDecodeError as exc:
                json_parse_error = f"Malformed JSON payload: {exc}"

            # Step 2: Store raw incoming event immediately before validation or prediction
            try:
                raw_event_id = insert_raw_event(
                    conn=conn,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    event_key=event_key,
                    raw_message=raw_message,
                    parsed_json=parsed_json,
                )
            except Exception as db_exc:
                conn.rollback()
                logger.exception(
                    "Failed to persist raw event (partition=%d offset=%d): %s",
                    msg.partition(),
                    msg.offset(),
                    db_exc,
                )
                continue

            # Step 3: Validate Event Contract
            if json_parse_error is not None:
                update_raw_status(conn, raw_event_id, "INVALID_SCHEMA", json_parse_error)
                consumer.commit(message=msg, asynchronous=False)
                stats["invalid"] += 1
                logger.warning(
                    "raw_event_id=%d marked INVALID_SCHEMA: %s",
                    raw_event_id,
                    json_parse_error,
                )
                continue

            try:
                validated_payload, event_dt = validate_event_contract(
                    parsed_json, required_model_features=model_features
                )
            except EventContractError as contract_exc:
                update_raw_status(
                    conn, raw_event_id, contract_exc.status, contract_exc.message
                )
                consumer.commit(message=msg, asynchronous=False)
                stats["invalid"] += 1
                logger.warning(
                    "raw_event_id=%d marked %s: %s",
                    raw_event_id,
                    contract_exc.status,
                    contract_exc.message,
                )
                continue

            # Step 4-7: Prepare features in exact training order, infer, and store results
            try:
                feature_row = [[float(validated_payload[col]) for col in model_features]]
                features_df = pd.DataFrame(feature_row, columns=model_features)
                predicted_score = float(model.predict(features_df)[0])
                if not math.isfinite(predicted_score):
                    raise ValueError(f"Non-finite model output: {predicted_score}")

                actual_score = float(validated_payload["actual_happiness_score"])
                prediction_error = abs(actual_score - predicted_score)

                prediction_id = persist_valid_prediction(
                    conn=conn,
                    raw_event_id=raw_event_id,
                    country_name=validated_payload["country"],
                    event_dt=event_dt,
                    observation_year=int(validated_payload["year"]),
                    actual_score=actual_score,
                    predicted_score=predicted_score,
                    prediction_error=prediction_error,
                )
                consumer.commit(message=msg, asynchronous=False)
                stats["valid"] += 1
                logger.info(
                    "VALID | raw_event_id=%d prediction_id=%d country=%s year=%d actual=%.3f pred=%.3f err=%.3f",
                    raw_event_id,
                    prediction_id,
                    validated_payload["country"],
                    validated_payload["year"],
                    actual_score,
                    predicted_score,
                    prediction_error,
                )
            except Exception as pred_exc:
                conn.rollback()
                update_raw_status(
                    conn, raw_event_id, "PREDICTION_ERROR", str(pred_exc)
                )
                consumer.commit(message=msg, asynchronous=False)
                stats["error"] += 1
                logger.exception(
                    "PREDICTION_ERROR for raw_event_id=%d (pipeline continues): %s",
                    raw_event_id,
                    pred_exc,
                )
    finally:
        consumer.close()
        conn.close()
        logger.info(
            "Consumer stopped cleanly. Summary -> valid=%d | invalid=%d | error=%d",
            stats["valid"],
            stats["invalid"],
            stats["error"],
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
