"""
Outbound webhook dispatcher.

When something happens in our system, we notify partners by POSTing
a signed event to their registered webhook URL.

Features:
    HMAC-SHA256 signing     — partners verify it came from us
    Exponential backoff     — 30s / 60s / 120s / 300s / 600s
    Idempotency headers     — partners can deduplicate retries
    Timeout + connection pool — httpx async client
    Structured logging      — every delivery attempt recorded
    Dead letter queue       — exhausted retries flagged for ops

Usage:
    dispatcher = WebhookDispatcher()
    result = dispatcher.send(
        partner_id   = "PARTNER-ACME",
        event_type   = "order.confirmed",
        payload      = {"order_id": "ORD-001", "status": "confirmed"},
        idempotency_key = "evt-uuid-abc123",
    )
"""

import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

import httpx

from shared.hmac_signer import sign_payload

logger = logging.getLogger(__name__)

# Partner webhook registry — in production this lives in DB
PARTNER_WEBHOOKS: dict[str, dict] = {
    "PARTNER-ACME": {
        "webhook_url":    "https://acme.example.com/webhooks/inbound",
        "webhook_secret": "whsec-our-secret-for-acme",
        "timeout":        10,
        "active":         True,
    },
    "PARTNER-BLUECROSS": {
        "webhook_url":    "https://bluecross.example.com/api/events",
        "webhook_secret": "whsec-our-secret-for-bcbs",
        "timeout":        15,
        "active":         True,
    },
}

BACKOFF_SCHEDULE = [30, 60, 120, 300, 600]


@dataclass
class DeliveryResult:
    success:       bool
    partner_id:    str
    event_type:    str
    http_status:   int   = 0
    attempt:       int   = 1
    duration_ms:   int   = 0
    error:         str   = ""
    delivery_id:   str   = ""


class WebhookDispatcher:
    """
    Sends signed webhook events to registered partner URLs.
    Synchronous — for async use, wrap in an Airflow task or thread pool.
    """

    def __init__(self, max_attempts: int = 5, audit_log=None):
        self._max     = max_attempts
        self._audit   = audit_log

    def send(
        self,
        partner_id:       str,
        event_type:       str,
        payload:          dict,
        idempotency_key:  Optional[str] = None,
        attempt:          int = 1,
    ) -> DeliveryResult:
        """
        Deliver a webhook to a partner.
        Retries synchronously up to max_attempts with backoff.
        Returns DeliveryResult — caller decides whether to dead-letter.
        """
        delivery_id = str(uuid.uuid4())
        idem_key    = idempotency_key or delivery_id

        partner = PARTNER_WEBHOOKS.get(partner_id)
        if not partner:
            logger.error(f"No webhook config for partner: {partner_id}")
            return DeliveryResult(
                success=False, partner_id=partner_id,
                event_type=event_type, error="No webhook config",
                delivery_id=delivery_id,
            )

        if not partner.get("active", True):
            logger.info(f"Webhook disabled for {partner_id} — skipping.")
            return DeliveryResult(
                success=True, partner_id=partner_id,
                event_type=event_type, error="disabled",
                delivery_id=delivery_id,
            )

        body = json.dumps({
            "event_id":       delivery_id,
            "event_type":     event_type,
            "idempotency_key": idem_key,
            "occurred_at":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "data":           payload,
        }, default=str).encode("utf-8")

        secret    = partner["webhook_secret"]
        signature = sign_payload(body, secret)
        timestamp = str(int(time.time()))

        headers = {
            "Content-Type":         "application/json",
            "X-Webhook-Signature":  signature,
            "X-Webhook-Timestamp":  timestamp,
            "X-Idempotency-Key":    idem_key,
            "X-Event-Type":         event_type,
            "X-Delivery-ID":        delivery_id,
            "User-Agent":           "PartnerIntegration/1.0",
        }

        url     = partner["webhook_url"]
        timeout = partner.get("timeout", 10)

        for att in range(attempt, self._max + 1):
            start = time.monotonic()
            try:
                with httpx.Client(timeout=timeout) as client:
                    resp = client.post(url, content=body, headers=headers)

                duration_ms = int((time.monotonic() - start) * 1000)
                logger.info(
                    f"Webhook {delivery_id} → {partner_id} {event_type} "
                    f"attempt={att} status={resp.status_code} {duration_ms}ms"
                )

                if resp.status_code in (200, 201, 202, 204):
                    if self._audit:
                        self._audit.outbound(
                            event_type=event_type, transport="webhook",
                            partner_id=partner_id, status="sent",
                            idempotency_key=idem_key, payload=body,
                            duration_ms=duration_ms, http_status_code=resp.status_code,
                            target_url=url,
                        )
                    return DeliveryResult(
                        success=True, partner_id=partner_id,
                        event_type=event_type, http_status=resp.status_code,
                        attempt=att, duration_ms=duration_ms,
                        delivery_id=delivery_id,
                    )

                # 4xx — don't retry (partner rejected it permanently)
                if 400 <= resp.status_code < 500:
                    err = f"HTTP {resp.status_code} — {resp.text[:200]}"
                    logger.error(f"Webhook {delivery_id} rejected by {partner_id}: {err}")
                    return DeliveryResult(
                        success=False, partner_id=partner_id,
                        event_type=event_type, http_status=resp.status_code,
                        attempt=att, duration_ms=duration_ms,
                        error=err, delivery_id=delivery_id,
                    )

                # 5xx — retry
                err = f"HTTP {resp.status_code}"

            except httpx.TimeoutException as exc:
                duration_ms = int((time.monotonic() - start) * 1000)
                err = f"Timeout after {timeout}s"
                logger.warning(f"Webhook {delivery_id} timeout (attempt {att}): {exc}")

            except httpx.RequestError as exc:
                duration_ms = int((time.monotonic() - start) * 1000)
                err = f"Network error: {exc}"
                logger.warning(f"Webhook {delivery_id} network error (attempt {att}): {exc}")

            if att < self._max:
                sleep_s = BACKOFF_SCHEDULE[min(att - 1, len(BACKOFF_SCHEDULE) - 1)]
                logger.info(f"Webhook {delivery_id} retry in {sleep_s}s...")
                time.sleep(sleep_s)

        # Exhausted all attempts
        if self._audit:
            self._audit.outbound(
                event_type=event_type, transport="webhook",
                partner_id=partner_id, status="failed",
                idempotency_key=idem_key, payload=body,
                error_message=err, target_url=url,
            )
        logger.error(
            f"Webhook {delivery_id} exhausted {self._max} attempts → dead letter. "
            f"partner={partner_id} event={event_type} last_error={err}"
        )
        return DeliveryResult(
            success=False, partner_id=partner_id,
            event_type=event_type, attempt=self._max,
            error=err, delivery_id=delivery_id,
        )

    def send_to_all(
        self,
        event_type: str,
        payload:    dict,
        partner_ids: Optional[list[str]] = None,
    ) -> list[DeliveryResult]:
        """
        Fan-out: send the same event to multiple partners.
        Useful for broadcast events (e.g. price update, catalog change).
        """
        targets = partner_ids or list(PARTNER_WEBHOOKS.keys())
        results = []
        for pid in targets:
            result = self.send(
                partner_id=pid,
                event_type=event_type,
                payload=payload,
                idempotency_key=str(uuid.uuid4()),
            )
            results.append(result)
        return results
