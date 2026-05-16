"""
Idempotency key handler.

Problem it solves:
    Partners retry failed webhooks. Networks have hiccups. A webhook can
    arrive 2, 3, or 10 times. Without idempotency, you'd process the same
    order multiple times — double-inserting, double-charging, double-enrolling.

Solution:
    Every inbound event carries an idempotency key (partner-provided UUID or
    our own hash of the payload). We store seen keys in a DB table with a TTL.
    If we've seen a key before → return the cached response, skip processing.

Storage: PostgreSQL table `idempotency_keys`
    key         TEXT PRIMARY KEY
    first_seen  TIMESTAMP
    response    JSONB          -- cached response to return on duplicate
    expires_at  TIMESTAMP      -- auto-expire old keys (default 7 days)

Usage:
    idem = IdempotencyHandler(db_conn)
    result = idem.check("key-abc-123")
    if result.is_duplicate:
        return result.cached_response    # return early, don't process

    # ... do your processing ...

    idem.mark_complete("key-abc-123", response={"status": "ok", "order_id": 42})
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Optional

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key          TEXT        PRIMARY KEY,
    first_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    response     JSONB,
    expires_at   TIMESTAMPTZ NOT NULL,
    partner_id   TEXT,
    endpoint     TEXT
);
CREATE INDEX IF NOT EXISTS idx_idem_expires ON idempotency_keys(expires_at);
"""


@dataclass
class IdempotencyResult:
    is_duplicate: bool
    cached_response: Optional[dict] = None
    first_seen: Optional[datetime] = None


class IdempotencyHandler:
    """
    Checks and records idempotency keys against a PostgreSQL backend.
    Thread-safe — each call opens its own transaction.
    """

    def __init__(self, db_conn, ttl_days: int = 7):
        self._conn = db_conn
        self._ttl  = timedelta(days=ttl_days)

    def ensure_table(self):
        """Create the idempotency_keys table if it doesn't exist."""
        with self._conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        self._conn.commit()

    def check(
        self,
        key: str,
        partner_id: str = "",
        endpoint: str = "",
    ) -> IdempotencyResult:
        """
        Check if a key has been seen before.
        If yes → return duplicate result with cached response.
        If no  → insert a pending record, return not-duplicate.
        """
        expires_at = datetime.utcnow() + self._ttl
        with self._conn.cursor() as cur:
            # Attempt insert — if key exists, do nothing and return existing row
            cur.execute(
                """
                INSERT INTO idempotency_keys (key, expires_at, partner_id, endpoint)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (key) DO NOTHING
                RETURNING first_seen, response
                """,
                (key, expires_at, partner_id, endpoint),
            )
            row = cur.fetchone()
            if row is not None:
                # INSERT succeeded — new key, not a duplicate
                self._conn.commit()
                logger.debug(f"New idempotency key: {key}")
                return IdempotencyResult(is_duplicate=False)

            # INSERT did nothing → key already exists → duplicate
            cur.execute(
                "SELECT first_seen, response FROM idempotency_keys WHERE key = %s",
                (key,),
            )
            existing = cur.fetchone()
            self._conn.commit()

        if existing:
            first_seen, cached = existing
            logger.info(f"Duplicate idempotency key: {key} (first seen {first_seen})")
            return IdempotencyResult(
                is_duplicate=True,
                cached_response=cached,
                first_seen=first_seen,
            )
        return IdempotencyResult(is_duplicate=False)

    def mark_complete(self, key: str, response: dict):
        """Store the response after successful processing."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE idempotency_keys
                SET completed_at = NOW(), response = %s
                WHERE key = %s
                """,
                (json.dumps(response), key),
            )
        self._conn.commit()
        logger.debug(f"Idempotency key completed: {key}")

    def purge_expired(self) -> int:
        """Delete expired keys. Run this on a schedule (e.g. nightly Airflow task)."""
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM idempotency_keys WHERE expires_at < NOW() RETURNING key"
            )
            count = cur.rowcount
        self._conn.commit()
        logger.info(f"Purged {count} expired idempotency keys")
        return count


def key_from_payload(payload: bytes) -> str:
    """
    Generate a deterministic idempotency key from raw payload bytes.
    Use this when the partner doesn't provide their own key.
    SHA-256 ensures identical payloads always produce the same key.
    """
    return hashlib.sha256(payload).hexdigest()
