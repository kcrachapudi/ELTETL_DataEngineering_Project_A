"""
Retry queue with exponential backoff and dead letter table.

Used for:
  INBOUND  — if processing a received event fails, queue it for retry
  OUTBOUND — if a webhook POST or HTTP call fails, retry with backoff

Backoff schedule (default):
    attempt 1 → wait  30s
    attempt 2 → wait  60s
    attempt 3 → wait 120s
    attempt 4 → wait 300s
    attempt 5 → wait 600s
    attempt 6+ → dead letter (give up, alert)

Dead letter:
    Events that exhaust retries go to `integration_dead_letter`.
    An Airflow task (Project 3) monitors this table and alerts.
    Dead-lettered events can be manually requeued after investigation.

Storage:
    integration_retry_queue  — pending retries
    integration_dead_letter  — exhausted events for manual review
"""

import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS integration_retry_queue (
    retry_id       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type     TEXT        NOT NULL,
    direction      TEXT        NOT NULL,
    partner_id     TEXT,
    payload        JSONB       NOT NULL,
    attempt_number INTEGER     NOT NULL DEFAULT 1,
    max_attempts   INTEGER     NOT NULL DEFAULT 6,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_retry_next ON integration_retry_queue(next_attempt_at)
    WHERE locked_until IS NULL OR locked_until < NOW();

CREATE TABLE IF NOT EXISTS integration_dead_letter (
    dlq_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type     TEXT        NOT NULL,
    direction      TEXT        NOT NULL,
    partner_id     TEXT,
    payload        JSONB       NOT NULL,
    total_attempts INTEGER     NOT NULL,
    final_error    TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at    TIMESTAMPTZ,
    resolution     TEXT
);
"""

BACKOFF_SECONDS = [30, 60, 120, 300, 600, 1800]


def _backoff(attempt: int) -> int:
    idx = min(attempt - 1, len(BACKOFF_SECONDS) - 1)
    return BACKOFF_SECONDS[idx]


class RetryQueue:
    """
    Postgres-backed retry queue. Works without a message broker —
    uses SELECT FOR UPDATE SKIP LOCKED for concurrent-safe dequeue.
    """

    def __init__(self, db_conn, max_attempts: int = 6):
        self._conn       = db_conn
        self._max        = max_attempts

    def ensure_tables(self):
        with self._conn.cursor() as cur:
            cur.execute(CREATE_TABLES_SQL)
        self._conn.commit()

    def enqueue(
        self,
        event_type: str,
        direction:  str,
        payload:    dict,
        partner_id: str = "",
        attempt:    int = 1,
        delay_seconds: Optional[int] = None,
    ) -> str:
        """Add an event to the retry queue. Returns retry_id."""
        delay    = delay_seconds if delay_seconds is not None else _backoff(attempt)
        next_at  = datetime.utcnow() + timedelta(seconds=delay)
        retry_id = str(uuid.uuid4())

        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO integration_retry_queue
                    (retry_id, event_type, direction, partner_id, payload,
                     attempt_number, max_attempts, next_attempt_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (retry_id, event_type, direction, partner_id,
                 json.dumps(payload), attempt, self._max, next_at),
            )
        self._conn.commit()
        logger.info(
            f"Enqueued retry {retry_id} — {direction} {event_type} "
            f"partner={partner_id} attempt={attempt} next_at={next_at.isoformat()}"
        )
        return retry_id

    def dequeue_batch(self, batch_size: int = 10) -> list[dict]:
        """
        Fetch up to batch_size due retries, locking them for processing.
        Uses SKIP LOCKED — safe for concurrent workers.
        """
        lock_until = datetime.utcnow() + timedelta(minutes=5)
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE integration_retry_queue
                SET locked_until = %s
                WHERE retry_id IN (
                    SELECT retry_id FROM integration_retry_queue
                    WHERE next_attempt_at <= NOW()
                      AND (locked_until IS NULL OR locked_until < NOW())
                    ORDER BY next_attempt_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING retry_id, event_type, direction, partner_id,
                          payload, attempt_number, max_attempts
                """,
                (lock_until, batch_size),
            )
            rows = cur.fetchall()
        self._conn.commit()
        return [
            {
                "retry_id":      r[0],
                "event_type":    r[1],
                "direction":     r[2],
                "partner_id":    r[3],
                "payload":       r[4],
                "attempt":       r[5],
                "max_attempts":  r[6],
            }
            for r in rows
        ]

    def mark_success(self, retry_id: str):
        """Remove a successfully processed retry from the queue."""
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM integration_retry_queue WHERE retry_id = %s",
                (retry_id,),
            )
        self._conn.commit()
        logger.info(f"Retry {retry_id} succeeded — removed from queue.")

    def mark_failed(self, retry_id: str, error: str):
        """
        Increment attempt count. If max reached → dead letter.
        Otherwise reschedule with next backoff interval.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT attempt_number, max_attempts, event_type,
                       direction, partner_id, payload
                FROM integration_retry_queue WHERE retry_id = %s
                """,
                (retry_id,),
            )
            row = cur.fetchone()
            if not row:
                return

            attempt, max_att, etype, direction, partner, payload = row
            next_attempt = attempt + 1

            if next_attempt > max_att:
                # → dead letter
                cur.execute(
                    """
                    INSERT INTO integration_dead_letter
                        (event_type, direction, partner_id, payload,
                         total_attempts, final_error)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (etype, direction, partner, json.dumps(payload),
                     attempt, error),
                )
                cur.execute(
                    "DELETE FROM integration_retry_queue WHERE retry_id = %s",
                    (retry_id,),
                )
                logger.error(
                    f"DEAD LETTER: {direction} {etype} partner={partner} "
                    f"after {attempt} attempts. Error: {error}"
                )
            else:
                next_at = datetime.utcnow() + timedelta(seconds=_backoff(next_attempt))
                cur.execute(
                    """
                    UPDATE integration_retry_queue
                    SET attempt_number = %s,
                        next_attempt_at = %s,
                        last_error = %s,
                        locked_until = NULL
                    WHERE retry_id = %s
                    """,
                    (next_attempt, next_at, error[:500], retry_id),
                )
                logger.warning(
                    f"Retry {retry_id} failed (attempt {attempt}/{max_att}). "
                    f"Next attempt at {next_at.isoformat()}. Error: {error}"
                )
        self._conn.commit()

    def dead_letter_count(self) -> int:
        """How many events are in the dead letter table — used for alerting."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM integration_dead_letter WHERE resolved_at IS NULL"
            )
            return cur.fetchone()[0]

    def resolve_dead_letter(self, dlq_id: str, resolution: str):
        """Mark a dead-lettered event as manually resolved."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE integration_dead_letter
                SET resolved_at = NOW(), resolution = %s
                WHERE dlq_id = %s
                """,
                (resolution, dlq_id),
            )
        self._conn.commit()
        logger.info(f"Dead letter {dlq_id} resolved: {resolution}")
