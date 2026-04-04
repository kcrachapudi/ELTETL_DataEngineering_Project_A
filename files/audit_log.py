"""
Audit log — structured record of every integration event in and out.

Why this matters:
    - Partner disputes ("we sent that order!") — you have the receipt
    - Debugging failures ("what exactly did they send us?") — payload stored
    - Compliance — full audit trail of every data exchange
    - Metrics — how many events per partner, failure rates, latency

Storage: PostgreSQL table `integration_audit_log`

Every row records:
    event_id         UUID
    direction        'inbound' | 'outbound'
    event_type       e.g. 'order.created', 'claim.submitted', 'edi.856'
    transport        'rest' | 'webhook' | 'edi' | 'sftp' | 'http_api'
    partner_id       which trading partner
    status           'received' | 'processed' | 'failed' | 'sent' | 'retried'
    idempotency_key  link to idempotency_keys table
    payload_hash     SHA-256 of raw payload (for dedup checks without storing PII)
    payload_size     bytes
    error_message    if failed
    duration_ms      processing time
    created_at       timestamp
"""

import hashlib
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Literal, Optional

logger = logging.getLogger(__name__)

Direction = Literal["inbound", "outbound"]
Status    = Literal["received", "processed", "failed", "sent", "retried", "rejected"]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS integration_audit_log (
    event_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    direction        TEXT        NOT NULL,
    event_type       TEXT        NOT NULL,
    transport        TEXT        NOT NULL,
    partner_id       TEXT,
    status           TEXT        NOT NULL,
    idempotency_key  TEXT,
    payload_hash     TEXT,
    payload_size     INTEGER,
    error_message    TEXT,
    duration_ms      INTEGER,
    http_status_code INTEGER,
    target_url       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_audit_partner   ON integration_audit_log(partner_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_direction ON integration_audit_log(direction, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_status    ON integration_audit_log(status, created_at DESC);
"""


class AuditLog:
    """
    Writes structured audit entries to PostgreSQL.
    Falls back to structured logger if DB is unavailable — never raises.
    """

    def __init__(self, db_conn=None):
        self._conn = db_conn

    def ensure_table(self):
        if not self._conn:
            return
        with self._conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        self._conn.commit()

    def log(
        self,
        direction:        Direction,
        event_type:       str,
        transport:        str,
        status:           Status,
        partner_id:       str       = "",
        idempotency_key:  str       = "",
        payload:          bytes     = b"",
        error_message:    str       = "",
        duration_ms:      int       = 0,
        http_status_code: int       = 0,
        target_url:       str       = "",
    ) -> str:
        """
        Record one integration event. Returns the event_id UUID string.
        Never raises — logs error and continues if DB write fails.
        """
        event_id     = str(uuid.uuid4())
        payload_hash = hashlib.sha256(payload).hexdigest() if payload else ""
        payload_size = len(payload) if payload else 0

        entry = {
            "event_id":        event_id,
            "direction":       direction,
            "event_type":      event_type,
            "transport":       transport,
            "partner_id":      partner_id,
            "status":          status,
            "idempotency_key": idempotency_key,
            "payload_hash":    payload_hash,
            "payload_size":    payload_size,
            "error_message":   error_message,
            "duration_ms":     duration_ms,
            "http_status_code":http_status_code,
            "target_url":      target_url,
        }

        logger.info(
            f"[AUDIT] {direction.upper()} {event_type} partner={partner_id} "
            f"status={status} transport={transport} "
            f"{'error=' + error_message if error_message else ''}"
        )

        if self._conn:
            try:
                with self._conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO integration_audit_log
                            (event_id, direction, event_type, transport, partner_id,
                             status, idempotency_key, payload_hash, payload_size,
                             error_message, duration_ms, http_status_code, target_url)
                        VALUES
                            (%(event_id)s, %(direction)s, %(event_type)s, %(transport)s,
                             %(partner_id)s, %(status)s, %(idempotency_key)s,
                             %(payload_hash)s, %(payload_size)s, %(error_message)s,
                             %(duration_ms)s, %(http_status_code)s, %(target_url)s)
                        """,
                        entry,
                    )
                self._conn.commit()
            except Exception as exc:
                logger.error(f"Audit log DB write failed: {exc} — entry: {entry}")

        return event_id

    def inbound(self, **kwargs) -> str:
        return self.log(direction="inbound", **kwargs)

    def outbound(self, **kwargs) -> str:
        return self.log(direction="outbound", **kwargs)
