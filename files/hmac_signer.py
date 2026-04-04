"""
HMAC-SHA256 signing and verification.

Used in two places:
  INBOUND  — verify that a webhook arriving from a partner was genuinely sent by them
  OUTBOUND — sign webhooks we send to partners so they can verify it was us

Industry standard: same approach used by Stripe, GitHub, Shopify.

Signature format:  sha256=<hex_digest>
Header name:       X-Webhook-Signature  (configurable per partner)
"""

import hashlib
import hmac
import time
import secrets
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def sign_payload(payload: bytes, secret: str) -> str:
    """
    Sign a raw payload bytes with a partner secret.
    Returns the full signature string: 'sha256=<hexdigest>'
    """
    sig = hmac.new(
        secret.encode("utf-8"),
        msg=payload,
        digestmod=hashlib.sha256,
    ).hexdigest()
    return f"sha256={sig}"


def verify_signature(
    payload: bytes,
    received_sig: str,
    secret: str,
    tolerance_seconds: int = 300,
    timestamp: Optional[str] = None,
) -> bool:
    """
    Verify an inbound webhook signature.

    Args:
        payload:           raw request body bytes
        received_sig:      value of X-Webhook-Signature header
        secret:            partner's shared secret
        tolerance_seconds: reject signatures older than this (replay attack prevention)
        timestamp:         optional X-Webhook-Timestamp header (Unix epoch string)

    Returns True if valid, False otherwise. Always constant-time compare.
    """
    if not received_sig or not secret:
        logger.warning("Missing signature or secret — rejecting.")
        return False

    # timestamp tolerance check (replay attack prevention)
    if timestamp:
        try:
            ts = int(timestamp)
            age = abs(time.time() - ts)
            if age > tolerance_seconds:
                logger.warning(f"Webhook timestamp too old: {age:.0f}s > {tolerance_seconds}s")
                return False
        except (ValueError, TypeError):
            logger.warning(f"Invalid webhook timestamp: {timestamp}")
            return False

    expected = sign_payload(payload, secret)
    # constant-time comparison — prevents timing attacks
    valid = hmac.compare_digest(expected.encode(), received_sig.encode())
    if not valid:
        logger.warning("Webhook signature mismatch — possible tampering.")
    return valid


def generate_secret(length: int = 32) -> str:
    """Generate a cryptographically secure webhook secret for a new partner."""
    return secrets.token_hex(length)
