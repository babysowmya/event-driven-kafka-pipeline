"""
Unit tests for the consumer's schema validation and UPSERT SQL generation.

These tests import the consumer's app package directly (no Docker, no real
Kafka, no real database) by loading the module from its file path.
"""
import importlib.util
import os
import uuid
from datetime import datetime, timezone

import pytest
from jsonschema import ValidationError


def _load_module(module_name: str, relative_path: str):
    """
    Load a service module by file path under a unique module name.

    Both the producer and consumer services use the package name "app",
    so a plain sys.path insert would collide between test files in the
    same pytest session. Loading by explicit file path with a unique
    name avoids that collision entirely.
    """
    file_path = os.path.join(os.path.dirname(__file__), "..", "..", relative_path)
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_schemas = _load_module("consumer_schemas", "services/consumer/app/schemas.py")
validate_event = _schemas.validate_event
build_upsert_query = _schemas.build_upsert_query


def make_valid_event(activity_type="view"):
    return {
        "user_id": str(uuid.uuid4()),
        "activity_type": activity_type,
        "item_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---- Schema validation: valid payloads ----

def test_valid_event_passes_validation():
    validate_event(make_valid_event("click"))  # should not raise


@pytest.mark.parametrize("activity_type", ["view", "click", "purchase"])
def test_all_valid_activity_types_pass(activity_type):
    validate_event(make_valid_event(activity_type))


# ---- Schema validation: invalid / poison-pill payloads ----

def test_missing_field_raises_validation_error():
    event = make_valid_event()
    del event["item_id"]
    with pytest.raises(ValidationError):
        validate_event(event)


def test_invalid_activity_type_raises_validation_error():
    event = make_valid_event()
    event["activity_type"] = "hover"
    with pytest.raises(ValidationError):
        validate_event(event)


def test_invalid_uuid_format_raises_validation_error():
    event = make_valid_event()
    event["user_id"] = "not-a-uuid"
    with pytest.raises(ValidationError):
        validate_event(event)


def test_invalid_timestamp_format_raises_validation_error():
    event = make_valid_event()
    event["timestamp"] = "not-a-timestamp"
    with pytest.raises(ValidationError):
        validate_event(event)


def test_non_string_field_raises_validation_error():
    event = make_valid_event()
    event["user_id"] = 12345
    with pytest.raises(ValidationError):
        validate_event(event)


# ---- UPSERT SQL generation ----

@pytest.mark.parametrize(
    "activity_type,expected_column",
    [("view", "view_count"), ("click", "click_count"), ("purchase", "purchase_count")],
)
def test_build_upsert_query_targets_correct_column(activity_type, expected_column):
    sql, values = build_upsert_query(activity_type)

    assert "ON CONFLICT (user_id, item_id, date)" in sql
    assert f"{expected_column} = daily_user_item_summary.{expected_column} + 1" in sql
    assert values[expected_column] == 1

    other_columns = {"view_count", "click_count", "purchase_count"} - {expected_column}
    for col in other_columns:
        assert values[col] == 0


def test_build_upsert_query_rejects_unknown_activity_type():
    with pytest.raises(ValueError):
        build_upsert_query("hover")