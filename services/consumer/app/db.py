"""
PostgreSQL connection management for the consumer service.

Uses a small connection pool so the consumer can recover from a dropped
connection without re-authenticating on every message, while still keeping
resource usage bounded.
"""
import logging
import os

import psycopg2
from psycopg2.pool import SimpleConnectionPool

logger = logging.getLogger("consumer.db")

_pool = None


def init_pool(minconn: int = 1, maxconn: int = 5) -> SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = SimpleConnectionPool(
            minconn,
            maxconn,
            host=os.environ["POSTGRES_HOST"],
            port=os.environ.get("POSTGRES_PORT", "5432"),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            dbname=os.environ["POSTGRES_DB"],
        )
        logger.info("PostgreSQL connection pool initialized.")
    return _pool


def get_connection():
    if _pool is None:
        init_pool()
    return _pool.getconn()


def put_connection(conn) -> None:
    if _pool is not None:
        _pool.putconn(conn)


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.closeall()
        logger.info("PostgreSQL connection pool closed.")
        _pool = None


def execute_upsert(conn, sql: str, params: dict) -> None:
    """Execute the given UPSERT statement. Caller is responsible for commit/rollback."""
    with conn.cursor() as cur:
        cur.execute(sql, params)