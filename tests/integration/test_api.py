"""
Integration tests for the API service.

These tests hit a REAL, running API + PostgreSQL (via `docker-compose up`),
so run them only after the stack is up and healthy:

    docker-compose up -d --build
    # wait until `docker-compose ps` shows postgres/kafka/api healthy
    pip install -r tests/requirements.txt
    pytest tests/integration/
"""
import os
import uuid

import requests

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")

# This UUID matches the seed row inserted by services/db/seed-data.sql
SEEDED_USER_ID = "22222222-2222-2222-2222-222222222222"


def test_seeded_user_returns_200_with_expected_shape():
    response = requests.get(f"{API_BASE_URL}/api/user-activity-summary/{SEEDED_USER_ID}")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1

    record = data[0]
    for key in ("item_id", "date", "view_count", "click_count", "purchase_count"):
        assert key in record


def test_nonexistent_user_returns_404():
    random_user_id = str(uuid.uuid4())
    response = requests.get(f"{API_BASE_URL}/api/user-activity-summary/{random_user_id}")
    assert response.status_code == 404
    assert "error" in response.json()


def test_invalid_uuid_string_returns_400():
    response = requests.get(f"{API_BASE_URL}/api/user-activity-summary/invalid-uuid-string")
    assert response.status_code == 400
    assert "error" in response.json()