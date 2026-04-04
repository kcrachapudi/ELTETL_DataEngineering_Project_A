"""
Auth middleware — per-partner API key and OAuth token validation.

Plugs into FastAPI as a dependency. Every protected endpoint uses
one of the two auth methods:
    api_key_auth   — X-Partner-ID + X-API-Key headers
    webhook_auth   — X-Partner-ID + X-Webhook-Signature (HMAC)

Rate limiting:
    Simple in-memory counter per partner per minute.
    Replace with Redis in production for multi-worker deployments.

Usage:
    @app.post("/inbound/orders")
    def receive_order(partner: dict = Depends(require_api_key)):
        ...
"""

import time
import logging
from collections import defaultdict
from typing import Optional

from fastapi import Header, HTTPException, Request

logger = logging.getLogger(__name__)

# Replace with DB-backed partner store in production
PARTNER_REGISTRY: dict[str, dict] = {
    "PARTNER-ACME": {
        "name":            "ACME Corporation",
        "api_key":         "key-acme-abc123",
        "webhook_secret":  "whsec-acme-xyz789",
        "rate_limit_rpm":  100,
        "active":          True,
        "allowed_events":  ["order.created", "order.updated", "order.cancelled"],
        "allowed_formats": ["json", "edi_850", "edi_856"],
    },
    "PARTNER-BLUECROSS": {
        "name":            "Blue Cross Blue Shield",
        "api_key":         "key-bcbs-def456",
        "webhook_secret":  "whsec-bcbs-qrs012",
        "rate_limit_rpm":  200,
        "active":          True,
        "allowed_events":  ["claim.submitted", "eligibility.request", "enrollment.change"],
        "allowed_formats": ["edi_837", "edi_834", "edi_270"],
    },
}

# In-memory rate limit counters {partner_id: [(timestamp, count)]}
_rate_counters: dict[str, list] = defaultdict(list)


def _check_rate_limit(partner_id: str, limit_rpm: int) -> bool:
    """Returns True if within limit, False if exceeded."""
    now    = time.time()
    window = [ts for ts in _rate_counters[partner_id] if now - ts < 60]
    if len(window) >= limit_rpm:
        return False
    window.append(now)
    _rate_counters[partner_id] = window
    return True


def _lookup_partner(partner_id: str) -> dict:
    partner = PARTNER_REGISTRY.get(partner_id)
    if not partner:
        logger.warning(f"Auth: unknown partner {partner_id}")
        raise HTTPException(status_code=401, detail="Unknown partner ID.")
    if not partner.get("active", True):
        raise HTTPException(status_code=403, detail="Partner account is inactive.")
    return {**partner, "partner_id": partner_id}


def require_api_key(
    x_partner_id: str           = Header(..., description="Partner identifier"),
    x_api_key:    str           = Header(..., description="Partner API key"),
) -> dict:
    """
    FastAPI dependency — validates X-Partner-ID + X-API-Key headers.
    Use with Depends(require_api_key) on REST endpoints.
    """
    partner = _lookup_partner(x_partner_id)

    if partner["api_key"] != x_api_key:
        logger.warning(f"Auth: invalid API key for {x_partner_id}")
        raise HTTPException(status_code=401, detail="Invalid API key.")

    if not _check_rate_limit(x_partner_id, partner["rate_limit_rpm"]):
        logger.warning(f"Auth: rate limit exceeded for {x_partner_id}")
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. Max {partner['rate_limit_rpm']} requests/minute.",
        )

    return partner


def require_webhook_auth(
    x_partner_id:        str           = Header(...),
    x_webhook_signature: str           = Header(...),
    x_webhook_timestamp: Optional[str] = Header(None),
) -> dict:
    """
    FastAPI dependency — validates HMAC webhook signature.
    Use with Depends(require_webhook_auth) on webhook endpoints.
    Note: raw body must be verified in the endpoint itself after calling this.
    """
    partner = _lookup_partner(x_partner_id)

    if not _check_rate_limit(x_partner_id, partner["rate_limit_rpm"]):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    # return partner with secret — endpoint does the HMAC check with raw body
    return {**partner, "webhook_secret": partner["webhook_secret"]}


def get_partner_secret(partner_id: str) -> Optional[str]:
    """Utility — get a partner's webhook secret by ID."""
    partner = PARTNER_REGISTRY.get(partner_id)
    return partner["webhook_secret"] if partner else None


def register_partner(partner_id: str, config: dict):
    """Register a new partner at runtime (use DB-backed store in production)."""
    PARTNER_REGISTRY[partner_id] = config
    logger.info(f"Partner registered: {partner_id} — {config.get('name', '')}")
