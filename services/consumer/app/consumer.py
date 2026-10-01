"""
Consumer service.

Subscribes to the 'user_activities' Kafka topic as part of a consumer group,
validates each event against the shared JSON schema, and persists valid
events into PostgreSQL via an idempotent UPSERT keyed on
(user_id, item_id, date).

Offset management: auto-commit is disabled. We only commit a message's
offset *after* its corresponding database transaction has committed
successfully. This means a crash between "DB commit" and "Kafka offset
commit" can cause a message to be reprocessed on restart (at-least-once
delivery) -- which is exactly why the UPSERT's UNIQUE constraint and
per-message design exist: reprocessing is safe by construction for the
row's existence, and acceptable for counters at this task's scope (see
README for the exactly-once vs. at-least-once discussion).
"""
import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime

from jsonschema import ValidationError
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from app.db import init_pool, get_connection, put_connection, close_pool, execute_upsert
from app.schemas import validate_event, build_upsert_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] consumer: %(message)s",
)
logger = logging.getLogger("consumer")

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "user_activities")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "activity_aggregator_group")

_shutdown_requested = False


def _handle_shutdown(signum, _frame):
    global _shutdown_requested
    logger.warning("Shutdown signal (%s) received. Finishing current message...", signum)
    _shutdown_requested = True


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL.split(","),
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=1000,
    )


def _connect_with_retry() -> KafkaConsumer:
    consumer = None
    while consumer is None and not _shutdown_requested:
        try:
            consumer = create_consumer()
            logger.info(
                "Connected to Kafka broker at %s, subscribed to '%s' as group '%s'.",
                KAFKA_BROKER_URL, KAFKA_TOPIC, KAFKA_GROUP_ID,
            )
        except KafkaError as exc:
            logger.warning("Kafka not ready yet (%s). Retrying in 5s...", exc)
            time.sleep(5)
    return consumer


def process_message(conn, raw_value: str) -> bool:
    """
    Parse, validate, and persist a single message.

    Returns True if it is now safe to advance the committed offset past this
    message (either it was processed successfully, or it was a poison pill
    that we intentionally skip). Returns False on a retryable error, in
    which case the caller must NOT advance the offset.
    """
    try:
        event = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        logger.warning("Skipping malformed message (invalid JSON: %s). Raw: %r", exc, raw_value)
        return True

    try:
        validate_event(event)
    except ValidationError as exc:
        logger.warning("Skipping message failing schema validation (%s). Payload: %s", exc.message, event)
        return True

    try:
        event_date = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00")).date()
        sql, insert_values = build_upsert_query(event["activity_type"])
        params = {
            "id": str(uuid.uuid4()),
            "user_id": event["user_id"],
            "item_id": event["item_id"],
            "date": event_date,
            **insert_values,
        }
        execute_upsert(conn, sql, params)
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        logger.exception("Database error while processing event; will retry on next poll: %s", event)
        return False


def main():
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    init_pool()
    conn = get_connection()
    consumer = _connect_with_retry()

    try:
        while not _shutdown_requested:
            try:
                records = consumer.poll(timeout_ms=1000, max_records=50)
            except KafkaError:
                logger.exception("Kafka poll error; will retry.")
                continue

            for topic_partition, messages in records.items():
                for message in messages:
                    success = process_message(conn, message.value)
                    if not success:
                        # Stop advancing this partition's offset here; the
                        # message (and Kafka's redelivery on restart) will
                        # retry it once the underlying issue is resolved.
                        logger.error(
                            "Halting progress on %s at offset %s due to a processing failure.",
                            topic_partition, message.offset,
                        )
                        break
                    # Commit immediately after each successful DB write so a
                    # crash here never loses an offset ahead of a completed write.
                    consumer.commit({topic_partition: OffsetAndMetadata(message.offset + 1, None)})
    finally:
        logger.info("Shutting down consumer gracefully...")
        try:
            consumer.commit()
        except Exception:
            logger.exception("Failed to commit final offsets during shutdown.")
        put_connection(conn)
        close_pool()
        if consumer is not None:
            consumer.close()
        logger.info("Consumer shutdown complete.")


if __name__ == "__main__":
    main()