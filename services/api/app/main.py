"""
Read-only REST API exposing aggregated user activity data.

GET /api/user-activity-summary/{user_id}
    200 -> JSON array of {item_id, date, view_count, click_count, purchase_count}
    400 -> {"error": "..."} if user_id is not a valid UUID
    404 -> {"error": "..."} if user_id is valid but has no rows
"""
import logging
import uuid
from datetime import date
from typing import List

from fastapi import FastAPI, Path
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.db import fetch_user_summary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] api: %(message)s")
logger = logging.getLogger("api")

app = FastAPI(title="User Activity Summary API")


class ActivitySummary(BaseModel):
    item_id: str
    date: date
    view_count: int
    click_count: int
    purchase_count: int


@app.get("/health")
async def health():
    """Simple liveness endpoint (also useful for CI/integration test wait-loops)."""
    return {"status": "ok"}


@app.get("/api/user-activity-summary/{user_id}", response_model=List[ActivitySummary])
async def get_user_summary(user_id: str = Path(..., title="The UUID of the user")):
    # 1. Validate user_id is a well-formed UUID before touching the database.
    try:
        parsed_uuid = uuid.UUID(user_id)
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"error": "user_id must be a valid UUID."},
        )

    # 2. Query the database for all matching records.
    try:
        rows = fetch_user_summary(str(parsed_uuid))
    except Exception:
        logger.exception("Database error while fetching summary for user_id=%s", user_id)
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error."},
        )

    # 3. Handle empty vs. populated result sets.
    if not rows:
        return JSONResponse(
            status_code=404,
            content={"error": "No activity data found for the specified user."},
        )

    # Serialize date objects for the JSON response.
    return [
        {**row, "date": row["date"].isoformat()}
        for row in rows
    ]