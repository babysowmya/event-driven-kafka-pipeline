"""
JSON Schema definition for the user_activity event, plus the SQL-building
helper used to turn a validated event into an idempotent UPSERT statement.
"""
from jsonschema import validate, FormatChecker

# Strict schema: all four fields required, activity_type constrained to an
# enum, user_id/item_id constrained to UUID format, timestamp to date-time.
USER_ACTIVITY_SCHEMA = {
    "type": "object",
    "properties": {
        "user_id": {"type": "string", "format": "uuid"},
        "activity_type": {"type": "string", "enum": ["view", "click", "purchase"]},
        "item_id": {"type": "string", "format": "uuid"},
        "timestamp": {"type": "string", "format": "date-time"},
    },
    "required": ["user_id", "activity_type", "item_id", "timestamp"],
    "additionalProperties": True,
}

# format_checker is required for jsonschema to actually enforce "format":
# "uuid" / "date-time" rather than silently ignoring them.
_format_checker = FormatChecker()


def validate_event(event: dict) -> None:
    """
    Validate a decoded event dictionary against USER_ACTIVITY_SCHEMA.
    Raises jsonschema.exceptions.ValidationError on any violation
    (missing field, wrong type, bad enum value, malformed UUID/timestamp).
    """
    validate(instance=event, schema=USER_ACTIVITY_SCHEMA, format_checker=_format_checker)


_COLUMN_BY_ACTIVITY = {
    "view": "view_count",
    "click": "click_count",
    "purchase": "purchase_count",
}


def build_upsert_query(activity_type: str):
    """
    Build the parameterized UPSERT SQL statement for a given activity_type.

    Returns (sql, insert_values) where insert_values is a dict with the
    view_count/click_count/purchase_count values to use on first insert
    (1 for the relevant counter, 0 for the others). On conflict, only the
    relevant counter column is incremented by 1 -- the other two are left
    untouched, since the row already carries their correct running totals.
    """
    if activity_type not in _COLUMN_BY_ACTIVITY:
        raise ValueError(f"Unsupported activity_type: {activity_type}")

    count_column = _COLUMN_BY_ACTIVITY[activity_type]

    insert_values = {
        "view_count": 1 if activity_type == "view" else 0,
        "click_count": 1 if activity_type == "click" else 0,
        "purchase_count": 1 if activity_type == "purchase" else 0,
    }

    # count_column is chosen from a fixed internal map (never user input),
    # so interpolating it into the SQL string is safe from injection.
    sql = f"""
        INSERT INTO daily_user_item_summary
            (id, user_id, item_id, date, view_count, click_count, purchase_count)
        VALUES
            (%(id)s, %(user_id)s, %(item_id)s, %(date)s,
             %(view_count)s, %(click_count)s, %(purchase_count)s)
        ON CONFLICT (user_id, item_id, date)
        DO UPDATE SET
            {count_column} = daily_user_item_summary.{count_column} + 1,
            last_updated = CURRENT_TIMESTAMP;
    """
    return sql, insert_values