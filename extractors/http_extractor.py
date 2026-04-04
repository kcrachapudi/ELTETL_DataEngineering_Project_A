"""
HTTP Extractor — fetches data from REST APIs.

Handles:
    - GET / POST requests
    - API key, Bearer token, Basic auth
    - Automatic pagination (next-page URL, page number, offset/limit)
    - Retry with exponential backoff on 5xx / timeouts
    - Response as raw bytes, parsed JSON, or raw text

Real sources this covers:
    Open Meteo weather API (no auth, free)
    Okta Users / Groups / Logs API (Bearer token)
    Google Workspace Admin SDK (OAuth2)
    Any standard REST API

Usage:
    # Simple no-auth GET
    ext = HTTPExtractor("https://api.open-meteo.com/v1/forecast",
                        params={"latitude": 32.77, "longitude": -96.79,
                                "hourly": "temperature_2m"})
    raw = ext.fetch()   # returns JSON string
    df  = JSONParser().parse(raw)

    # Paginated API with Bearer token
    ext = HTTPExtractor(
        url="https://yourorg.okta.com/api/v1/users",
        auth_method="bearer",
        token="your-okta-token",
        paginate=True,
    )
    raw = ext.fetch()   # returns JSON array of ALL pages merged
"""

import json
import logging
import time
from typing import Any, Literal, Optional
from urllib.request import Request, urlopen
from urllib.parse import urlencode, urljoin
from urllib.error import HTTPError, URLError

from .base_extractor import BaseExtractor, ExtractorError

logger = logging.getLogger(__name__)

AuthMethod = Literal["none", "api_key", "bearer", "basic"]
BACKOFF    = [2, 5, 10, 30]


class HTTPExtractor(BaseExtractor):
    """
    Fetches data from a REST API endpoint.
    Uses only stdlib urllib — no httpx/requests dependency needed.
    """

    def __init__(
        self,
        url:            str,
        method:         str                = "GET",
        params:         Optional[dict]     = None,
        headers:        Optional[dict]     = None,
        body:           Optional[dict]     = None,
        auth_method:    AuthMethod         = "none",
        api_key:        Optional[str]      = None,
        api_key_header: str                = "X-API-Key",
        token:          Optional[str]      = None,
        username:       Optional[str]      = None,
        password:       Optional[str]      = None,
        paginate:       bool               = False,
        page_param:     str                = "page",
        limit_param:    str                = "limit",
        page_size:      int                = 200,
        max_pages:      int                = 100,
        next_key:       Optional[str]      = None,
        data_key:       Optional[str]      = None,
        timeout:        int                = 30,
        max_attempts:   int                = 4,
    ):
        self._url         = url
        self._method      = method.upper()
        self._params      = params or {}
        self._headers     = headers or {}
        self._body        = body
        self._auth        = auth_method
        self._api_key     = api_key
        self._key_header  = api_key_header
        self._token       = token
        self._username    = username
        self._password    = password
        self._paginate    = paginate
        self._page_param  = page_param
        self._limit_param = limit_param
        self._page_size   = page_size
        self._max_pages   = max_pages
        self._next_key    = next_key   # JSON key that holds next-page URL
        self._data_key    = data_key   # JSON key that holds the records array
        self._timeout     = timeout
        self._max         = max_attempts

    @property
    def source_name(self) -> str:
        return self._url

    def fetch(self) -> str:
        """
        Fetch from the API. Returns JSON string.
        If paginate=True, fetches all pages and merges records into one array.
        """
        if self._paginate:
            return self._fetch_all_pages()
        return self._fetch_one(self._url, self._params)

    def _fetch_one(self, url: str, params: dict) -> str:
        """Single request with retry."""
        full_url = self._build_url(url, params)
        req      = Request(full_url, method=self._method)
        self._attach_auth(req)
        for k, v in self._headers.items():
            req.add_header(k, v)

        if self._body:
            req.add_header("Content-Type", "application/json")
            req.data = json.dumps(self._body).encode()

        last_err = ""
        for attempt in range(1, self._max + 1):
            try:
                with urlopen(req, timeout=self._timeout) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                logger.info(
                    f"HTTP {self._method} {url} → 200 "
                    f"({len(raw)} chars, attempt {attempt})"
                )
                return raw
            except HTTPError as exc:
                last_err = f"HTTP {exc.code}: {exc.reason}"
                if exc.code < 500:
                    raise ExtractorError(f"Client error from {url}: {last_err}")
                logger.warning(f"HTTP {exc.code} on attempt {attempt}: {url}")
            except URLError as exc:
                last_err = f"Network error: {exc.reason}"
                logger.warning(f"Network error on attempt {attempt}: {exc}")

            if attempt < self._max:
                sleep = BACKOFF[min(attempt - 1, len(BACKOFF) - 1)]
                logger.info(f"Retrying in {sleep}s...")
                time.sleep(sleep)

        raise ExtractorError(
            f"Failed to fetch {url} after {self._max} attempts. Last: {last_err}"
        )

    def _fetch_all_pages(self) -> str:
        """
        Fetch all pages and merge into a single JSON array.
        Supports two pagination styles:
            1. next-page URL embedded in response (next_key)
            2. page-number / offset params incremented each request
        """
        all_records = []
        url         = self._url
        params      = {**self._params, self._limit_param: self._page_size}
        page        = 1

        while page <= self._max_pages:
            if not self._next_key:
                params[self._page_param] = page

            raw  = self._fetch_one(url, params)
            data = json.loads(raw)

            # extract records array
            if self._data_key and isinstance(data, dict):
                records = data.get(self._data_key, [])
            elif isinstance(data, list):
                records = data
            else:
                records = [data]

            if not records:
                break

            all_records.extend(records)
            logger.info(f"Page {page}: +{len(records)} records ({len(all_records)} total)")

            # next page
            if self._next_key and isinstance(data, dict):
                next_url = data.get(self._next_key)
                if not next_url:
                    break
                url    = next_url
                params = {}
            else:
                if len(records) < self._page_size:
                    break
                page += 1

        logger.info(f"Pagination complete — {len(all_records)} total records from {self._url}")
        return json.dumps(all_records)

    def _build_url(self, url: str, params: dict) -> str:
        if not params:
            return url
        return f"{url}?{urlencode(params, doseq=True)}"

    def _attach_auth(self, req: Request):
        if self._auth == "api_key" and self._api_key:
            req.add_header(self._key_header, self._api_key)
        elif self._auth == "bearer" and self._token:
            req.add_header("Authorization", f"Bearer {self._token}")
        elif self._auth == "basic" and self._username:
            import base64
            creds = base64.b64encode(
                f"{self._username}:{self._password}".encode()
            ).decode()
            req.add_header("Authorization", f"Basic {creds}")
