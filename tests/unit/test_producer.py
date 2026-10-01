"""
Unit tests for the producer's event generation logic.

generate_event() has no side effects and never touches Kafka at import time,
so it can be tested in complete isolation.
"""
import importlib.util
import os
import uuid
from datetime import datetime


def _load_module(module_name: str, relative_path: str):
    """
    Load a service module by file path under a unique module name (see the
    matching helper in test_consumer.py for why this avoids "app" package
    name collisions between the two services).
    """
    file_path = os.path.join(os.path.dirname(__file__), "..", "..", relative_path)
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_producer = _load_module("producer_producer", "services/producer/app/producer.py")
generate_event = _producer.generate_event


def test_generate_event_has_exactly_required_keys():
    event = generate_event()
    assert set(event.keys()) == {"user_id", "activity_type", "item_id", "timestamp"}


def test_generate_event_user_id_and_item_id_are_valid_uuids():
    event = generate_event()
    # Raises ValueError if malformed -- the assertion is that this does not raise.
    uuid.UUID(event["user_id"])
    uuid.UUID(event["item_id"])


def test_generate_event_user_id_and_item_id_differ():
    event = generate_event()
    assert event["user_id"] != event["item_id"]


def test_generate_event_activity_type_is_always_valid():
    for _ in range(50):
        event = generate_event()
        assert event["activity_type"] in {"view", "click", "purchase"}


def test_generate_event_timestamp_is_iso8601_and_parseable():
    event = generate_event()
    parsed = datetime.fromisoformat(event["timestamp"])
    assert parsed.tzinfo is not None  # must be timezone-aware (UTC)