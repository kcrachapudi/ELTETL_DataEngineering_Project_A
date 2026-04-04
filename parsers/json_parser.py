"""
JSON Parser — handles REST API responses and webhook payloads.

Supports:
    - Single JSON object      {"order_id": "1", ...}
    - Array of objects        [{"order_id": "1"}, ...]
    - Nested with a data key  {"data": [...], "meta": {...}}
    - JSONL (one object per line)

Usage:
    parser = JSONParser()
    df = parser.parse("path/to/file.json")
    df = parser.parse('{"id": 1, "name": "test"}')
    df = parser.parse({"id": 1, "name": "test"})   # already a dict
    df = parser.parse([{"id": 1}, {"id": 2}])       # already a list
"""

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .base_parser import BaseParser, ParserError

logger = logging.getLogger(__name__)

# Common envelope keys that wrap the real data array
DATA_ENVELOPE_KEYS = ["data", "results", "items", "records", "rows", "content", "payload"]


class JSONParser(BaseParser):

    @property
    def format_name(self) -> str:
        return "JSON"

    def parse(self, source: Any) -> pd.DataFrame:
        """
        Args:
            source: file path, raw JSON string, dict, or list.
        Returns:
            Normalised DataFrame — one row per record.
        """
        data = self._load(source)
        records = self._normalise(data)

        if not records:
            raise ParserError("JSON parsed but produced no records.")

        df = pd.json_normalize(records)  # flattens nested dicts with dot notation
        logger.info(f"JSON parse complete — {len(df)} rows, {len(df.columns)} columns")
        return df

    def _load(self, source: Any) -> Any:
        if isinstance(source, (dict, list)):
            return source
        if isinstance(source, bytes):
            source = source.decode("utf-8", errors="replace")
        if isinstance(source, str):
            # try file path first
            if len(source) < 512 and not source.strip().startswith(("{", "[")):
                path = Path(source)
                if path.exists():
                    text = path.read_text(encoding="utf-8")
                    return self._parse_text(text)
            return self._parse_text(source)
        raise ParserError(f"Unsupported source type: {type(source)}")

    def _parse_text(self, text: str) -> Any:
        text = text.strip()
        # JSONL — one JSON object per line
        if "\n" in text and not text.startswith("["):
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            try:
                return [json.loads(l) for l in lines]
            except json.JSONDecodeError:
                pass  # fall through to standard JSON parse
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise ParserError(f"Invalid JSON: {exc}")

    def _normalise(self, data: Any) -> list[dict]:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # check for envelope wrapper
            for key in DATA_ENVELOPE_KEYS:
                if key in data and isinstance(data[key], list):
                    logger.debug(f"JSON envelope key found: '{key}'")
                    return data[key]
            # single object
            return [data]
        raise ParserError(f"JSON root must be object or array, got: {type(data)}")
