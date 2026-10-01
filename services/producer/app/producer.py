"""
Producer service.

Continuously generates synthetic 'user_activity' events and publishes them
to the Kafka topic 'user_activities'. Designed to run forever inside a
container, with graceful shutdown on SIGTERM/SIGINT and resilience to
transient Kafka unavailability.
"""
import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] producer: %(message)s",
)
logger = logging.getLogger("producer")

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "user_activities")
DELAY_MIN = float(os.environ.get("PRODUCE_DELAY_MIN", "0.2"))
DELAY_MAX = float(os.environ.get("PRODUCE_DELAY_MAX", "1.0"))

_shutdown_requested = False


def _handle_shutdown(signum, _frame):
    global _shutdown_requested
    logger.warning("Shutdown signal (%s) received. Stopping after current event...", signum)
    _shutdown_requested = True


def generate_event() -> dict:
    """Build a single synthetic user_activity event matching the shared JSON schema."""
    return {
        "user_id": str(uuid.uuid4()),
        "activity_type": random.choice(["view", "click", "purchase"]),
        "item_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all",
        linger_ms=50,
    )


def _connect_with_retry() -> KafkaProducer:
    """Keep retrying the initial connection instead of crashing on startup races."""
    producer = None
    while producer is None and not _shutdown_requested:
        try:
            producer = create_producer()
            logger.info("Connected to Kafka broker at %s.", KAFKA_BROKER_URL)
        except KafkaError as exc:
            logger.warning("Kafka not reachable yet (%s). Retrying in 5s...", exc)
            time.sleep(5)
    return producer


def main():
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    producer = _connect_with_retry()
    events_sent = 0

    try:
        while not _shutdown_requested:
            event = generate_event()
            try:
                # send() is async; flush() forces delivery so we know immediately
                # whether the broker accepted the message (simple, robust for
                # this throughput level).
                producer.send(KAFKA_TOPIC, value=event)
                producer.flush(timeout=5)
                events_sent += 1
                if events_sent % 20 == 0:
                    logger.info("Produced %d events so far.", events_sent)
            except KafkaError as exc:
                # Do not let a transient broker/network blip kill the process.
                logger.warning("Failed to send event (%s). Backing off before retry.", exc)
                time.sleep(3)
                continue

            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    finally:
        logger.info("Shutting down producer gracefully after sending %d events.", events_sent)
        if producer is not None:
            producer.flush(timeout=5)
            producer.close(timeout=5)


if __name__ == "__main__":
    main()