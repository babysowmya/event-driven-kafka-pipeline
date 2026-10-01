"""Read-only PostgreSQL access for the REST API service."""
import os

import psycopg2
import psycopg2.extras


def get_connection():
    """Open a connection to PostgreSQL using environment-provided configuration."""
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def fetch_user_summary(user_id: str):
    """Return all daily_user_item_summary rows for the given user_id (string UUID)."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT item_id, date, view_count, click_count, purchase_count
                FROM daily_user_item_summary
                WHERE user_id = %s
                ORDER BY date DESC;
                """,
                (user_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()