"""
Event Router — DB → outbound dispatcher bridge.

Watches for new records in source tables and fires outbound events
to partners. This is what makes the pipeline event-driven.

Pattern: polling loop (simple, reliable, no message broker needed).
    1. Query for unprocessed events in the outbound_event_queue table
    2. Route each event to the correct dispatcher (webhook / HTTP / EDI / SFTP)
    3. Mark as sent or failed
    4. Retry failures via RetryQueue

In Project 3 this becomes an Airflow sensor task.
In Project 4 it moves to a Cloud Pub/Sub push subscription.

Table: outbound_event_queue
    event_id       UUID PK
    event_type     TEXT    e.g. 'order.confirmed', 'claim.processed'
    partner_id     TEXT
    payload        JSONB
    transport      TEXT    'webhook' | 'http_api' | 'edi_sftp'
    status         TEXT    'pending' | 'sent' | 'failed'
    created_at     TIMESTAMPTZ
    processed_at   TIMESTAMPTZ

Usage:
    router = EventRouter(db_conn, dispatcher, http_caller, edi_gen, sftp_dropper)
    router.process_pending(batch_size=50)   # call from Airflow / cron
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS outbound_event_queue (
    event_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type    TEXT        NOT NULL,
    partner_id    TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    transport     TEXT        NOT NULL DEFAULT 'webhook',
    status        TEXT        NOT NULL DEFAULT 'pending',
    error         TEXT,
    attempt_count INTEGER     NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_oq_pending
    ON outbound_event_queue(status, created_at)
    WHERE status = 'pending';
"""


class EventRouter:

    def __init__(
        self,
        db_conn,
        webhook_dispatcher=None,
        http_caller=None,
        edi_generator=None,
        sftp_dropper=None,
        audit_log=None,
    ):
        self._conn       = db_conn
        self._webhook    = webhook_dispatcher
        self._http       = http_caller
        self._edi_gen    = edi_generator
        self._sftp       = sftp_dropper
        self._audit      = audit_log

    def ensure_table(self):
        with self._conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        self._conn.commit()

    def enqueue(
        self,
        event_type: str,
        partner_id: str,
        payload:    dict,
        transport:  str = "webhook",
    ) -> str:
        """Add an outbound event to the queue. Returns event_id."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO outbound_event_queue
                    (event_type, partner_id, payload, transport)
                VALUES (%s, %s, %s, %s)
                RETURNING event_id
                """,
                (event_type, partner_id, json.dumps(payload), transport),
            )
            event_id = str(cur.fetchone()[0])
        self._conn.commit()
        logger.info(f"Queued outbound event: {event_type} → {partner_id} [{transport}]")
        return event_id

    def process_pending(self, batch_size: int = 50) -> dict:
        """
        Process all pending outbound events.
        Returns summary: {sent: N, failed: N, skipped: N}
        """
        events = self._fetch_pending(batch_size)
        if not events:
            logger.debug("No pending outbound events.")
            return {"sent": 0, "failed": 0, "skipped": 0}

        sent = failed = skipped = 0
        for event in events:
            result = self._dispatch(event)
            if result == "sent":
                sent += 1
            elif result == "failed":
                failed += 1
            else:
                skipped += 1

        logger.info(f"Event router run: {sent} sent, {failed} failed, {skipped} skipped")
        return {"sent": sent, "failed": failed, "skipped": skipped}

    def _fetch_pending(self, limit: int) -> list[dict]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, event_type, partner_id, payload, transport
                FROM outbound_event_queue
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return [
            {
                "event_id":   str(r[0]),
                "event_type": r[1],
                "partner_id": r[2],
                "payload":    r[3],
                "transport":  r[4],
            }
            for r in rows
        ]

    def _dispatch(self, event: dict) -> str:
        transport  = event["transport"]
        event_type = event["event_type"]
        partner_id = event["partner_id"]
        payload    = event["payload"]
        event_id   = event["event_id"]

        try:
            if transport == "webhook" and self._webhook:
                result = self._webhook.send(
                    partner_id=partner_id,
                    event_type=event_type,
                    payload=payload,
                    idempotency_key=event_id,
                )
                success = result.success
                error   = result.error

            elif transport == "http_api" and self._http:
                endpoint = payload.get("_endpoint", "/events")
                result   = self._http.call(
                    partner_id=partner_id,
                    method="POST",
                    path=endpoint,
                    payload=payload,
                )
                success = result.success
                error   = result.error

            elif transport == "edi_sftp" and self._edi_gen and self._sftp:
                tx_type  = payload.get("edi_type", "850")
                edi_str  = self._generate_edi(tx_type, payload)
                filename = f"{tx_type}_{event_id[:8]}.edi"
                result   = self._sftp.drop_bytes(edi_str.encode(), filename)
                success  = result.success
                error    = result.error

            else:
                logger.warning(f"No dispatcher for transport '{transport}' — skipping {event_id}")
                return "skipped"

            if success:
                self._mark(event_id, "sent")
            else:
                self._mark(event_id, "failed", error)

            return "sent" if success else "failed"

        except Exception as exc:
            logger.error(f"Event dispatch exception for {event_id}: {exc}")
            self._mark(event_id, "failed", str(exc))
            return "failed"

    def _generate_edi(self, tx_type: str, payload: dict) -> str:
        if tx_type == "850":
            return self._edi_gen.generate_850(payload)
        if tx_type == "856":
            return self._edi_gen.generate_856(payload)
        if tx_type == "834":
            return self._edi_gen.generate_834(payload)
        raise ValueError(f"Unsupported EDI type for generation: {tx_type}")

    def _mark(self, event_id: str, status: str, error: str = ""):
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE outbound_event_queue
                SET status = %s,
                    error  = %s,
                    processed_at = NOW(),
                    attempt_count = attempt_count + 1
                WHERE event_id = %s
                """,
                (status, error[:500] if error else None, event_id),
            )
        self._conn.commit()
