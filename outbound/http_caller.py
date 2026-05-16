"""
Outbound HTTP API caller.

When we need to CALL a partner's REST API (vs them calling ours),
this module handles auth, retry, timeout, and structured logging.

Common use cases:
    - Confirm an order back to a marketplace (Amazon, Walmart)
    - Submit a status update to a logistics partner's API
    - Push an enrollment update to a health plan's API
    - Trigger a payment via a fintech partner's API

Auth methods supported:
    API key     — X-API-Key header or query param
    Bearer token — Authorization: Bearer <token>
    OAuth 2.0   — client_credentials flow, auto-refreshes token
    Basic auth  — username:password (legacy partners)
"""

import base64
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

import httpx

logger = logging.getLogger(__name__)

AuthMethod = Literal["api_key", "bearer", "oauth2_client_credentials", "basic"]

# Partner API registry
PARTNER_APIS: dict[str, dict] = {
    "PARTNER-ACME": {
        "base_url":    "https://api.acme.example.com/v2",
        "auth_method": "api_key",
        "api_key":     "acme-outbound-key-xyz",
        "api_key_header": "X-API-Key",
        "timeout":     15,
    },
    "PARTNER-BLUECROSS": {
        "base_url":    "https://api.bluecross.example.com/v1",
        "auth_method": "oauth2_client_credentials",
        "client_id":   "our-client-id",
        "client_secret": "our-client-secret",
        "token_url":   "https://auth.bluecross.example.com/oauth/token",
        "scope":       "claims.submit eligibility.query",
        "timeout":     20,
    },
}

BACKOFF = [5, 15, 30, 60, 120]


@dataclass
class APICallResult:
    success:       bool
    partner_id:    str
    endpoint:      str
    http_status:   int   = 0
    response_body: dict  = field(default_factory=dict)
    duration_ms:   int   = 0
    attempt:       int   = 1
    error:         str   = ""


class TokenCache:
    """Simple in-memory OAuth token cache. Replace with Redis in production."""
    _cache: dict[str, dict] = {}

    @classmethod
    def get(cls, partner_id: str) -> Optional[str]:
        entry = cls._cache.get(partner_id)
        if entry and entry["expires_at"] > time.time() + 30:
            return entry["token"]
        return None

    @classmethod
    def set(cls, partner_id: str, token: str, expires_in: int):
        cls._cache[partner_id] = {
            "token":      token,
            "expires_at": time.time() + expires_in,
        }


class HTTPAPICaller:
    """
    Makes outbound HTTP calls to partner REST APIs.
    Handles auth, retry, logging, and response parsing.
    """

    def __init__(self, max_attempts: int = 4, audit_log=None):
        self._max   = max_attempts
        self._audit = audit_log

    def call(
        self,
        partner_id: str,
        method:     str,
        path:       str,
        payload:    Optional[dict] = None,
        params:     Optional[dict] = None,
        extra_headers: Optional[dict] = None,
    ) -> APICallResult:
        """
        Make an authenticated HTTP call to a partner API.

        Args:
            partner_id:  registered partner key
            method:      HTTP method ('GET', 'POST', 'PUT', 'PATCH')
            path:        path relative to base_url (e.g. '/orders/123/confirm')
            payload:     JSON body (optional)
            params:      query params (optional)

        Returns:
            APICallResult with success flag, status code, parsed response
        """
        config = PARTNER_APIS.get(partner_id)
        if not config:
            return APICallResult(
                success=False, partner_id=partner_id,
                endpoint=path, error=f"No API config for {partner_id}",
            )

        url      = config["base_url"].rstrip("/") + "/" + path.lstrip("/")
        timeout  = config.get("timeout", 10)
        headers  = {"Content-Type": "application/json", "Accept": "application/json"}
        headers.update(extra_headers or {})

        # Attach auth
        auth_err = self._attach_auth(config, partner_id, headers)
        if auth_err:
            return APICallResult(
                success=False, partner_id=partner_id,
                endpoint=path, error=auth_err,
            )

        body = json.dumps(payload, default=str).encode() if payload else None
        err  = ""

        for att in range(1, self._max + 1):
            start = time.monotonic()
            try:
                with httpx.Client(timeout=timeout) as client:
                    resp = client.request(
                        method=method.upper(),
                        url=url,
                        content=body,
                        params=params,
                        headers=headers,
                    )

                duration_ms = int((time.monotonic() - start) * 1000)
                logger.info(
                    f"API call → {partner_id} {method} {path} "
                    f"attempt={att} status={resp.status_code} {duration_ms}ms"
                )

                try:
                    resp_body = resp.json()
                except Exception:
                    resp_body = {"raw": resp.text[:500]}

                if resp.status_code < 300:
                    if self._audit:
                        self._audit.outbound(
                            event_type=f"api.{method.lower()}",
                            transport="http_api",
                            partner_id=partner_id,
                            status="sent",
                            payload=body or b"",
                            duration_ms=duration_ms,
                            http_status_code=resp.status_code,
                            target_url=url,
                        )
                    return APICallResult(
                        success=True, partner_id=partner_id,
                        endpoint=path, http_status=resp.status_code,
                        response_body=resp_body, duration_ms=duration_ms,
                        attempt=att,
                    )

                if 400 <= resp.status_code < 500:
                    err = f"HTTP {resp.status_code}: {resp.text[:300]}"
                    logger.error(f"Partner {partner_id} rejected call: {err}")
                    return APICallResult(
                        success=False, partner_id=partner_id,
                        endpoint=path, http_status=resp.status_code,
                        response_body=resp_body, duration_ms=duration_ms,
                        attempt=att, error=err,
                    )

                err = f"HTTP {resp.status_code}"

            except httpx.TimeoutException:
                duration_ms = int((time.monotonic() - start) * 1000)
                err = f"Timeout after {timeout}s"
                logger.warning(f"API call timeout (attempt {att}): {partner_id} {path}")

            except httpx.RequestError as exc:
                duration_ms = int((time.monotonic() - start) * 1000)
                err = f"Network error: {exc}"
                logger.warning(f"API call network error (attempt {att}): {exc}")

            if att < self._max:
                sleep_s = BACKOFF[min(att - 1, len(BACKOFF) - 1)]
                logger.info(f"Retrying in {sleep_s}s...")
                time.sleep(sleep_s)

        logger.error(f"API call exhausted {self._max} attempts: {partner_id} {path}")
        return APICallResult(
            success=False, partner_id=partner_id,
            endpoint=path, attempt=self._max, error=err,
        )

    def _attach_auth(self, config: dict, partner_id: str, headers: dict) -> str:
        """Attach auth headers. Returns error string or empty string on success."""
        method = config.get("auth_method", "api_key")

        if method == "api_key":
            key_header = config.get("api_key_header", "X-API-Key")
            headers[key_header] = config["api_key"]

        elif method == "bearer":
            headers["Authorization"] = f"Bearer {config['token']}"

        elif method == "oauth2_client_credentials":
            token = TokenCache.get(partner_id)
            if not token:
                token, err = self._fetch_oauth_token(config, partner_id)
                if err:
                    return err
            headers["Authorization"] = f"Bearer {token}"

        elif method == "basic":
            creds = base64.b64encode(
                f"{config['username']}:{config['password']}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {creds}"

        return ""

    def _fetch_oauth_token(self, config: dict, partner_id: str) -> tuple[str, str]:
        """Fetch OAuth2 client_credentials token. Returns (token, error)."""
        try:
            with httpx.Client(timeout=10) as client:
                resp = client.post(
                    config["token_url"],
                    data={
                        "grant_type":    "client_credentials",
                        "client_id":     config["client_id"],
                        "client_secret": config["client_secret"],
                        "scope":         config.get("scope", ""),
                    },
                )
                resp.raise_for_status()
                data       = resp.json()
                token      = data["access_token"]
                expires_in = data.get("expires_in", 3600)
                TokenCache.set(partner_id, token, expires_in)
                logger.info(f"OAuth token fetched for {partner_id}, expires in {expires_in}s")
                return token, ""
        except Exception as exc:
            err = f"OAuth token fetch failed: {exc}"
            logger.error(err)
            return "", err
