"""
Fixed-Width File Parser — mainframe dumps, government files, legacy bank exports.

Fixed-width files have no delimiters. Each field occupies an exact
column range. You need a schema (field name + start position + length)
to parse them correctly.

Common sources:
    - IRS / SSA government data files
    - NACHA ACH payment files
    - Mainframe COBOL data dumps
    - Legacy insurance and banking exports
    - Utility billing files

Usage:
    schema = [
        {"name": "record_type",    "start": 0,  "length": 1},
        {"name": "routing_number", "start": 1,  "length": 9},
        {"name": "account_number", "start": 10, "length": 17},
        {"name": "amount",         "start": 27, "length": 10},
        {"name": "name",           "start": 37, "length": 22},
    ]
    parser = FixedWidthParser(schema)
    df = parser.parse("path/to/file.txt")

    # Or use a named preset
    parser = FixedWidthParser.nacha_ach()
    df = parser.parse("path/to/ach_file.txt")
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .base_parser import BaseParser, ParserError

logger = logging.getLogger(__name__)


class FixedWidthParser(BaseParser):

    def __init__(self, schema: list[dict], skip_record_types: list[str] = None):
        """
        Args:
            schema: list of {"name": str, "start": int, "length": int, "type": str}
                    type is optional: "str" (default), "int", "float", "date_YYYYMMDD"
            skip_record_types: if set, skip rows where record_type field matches these values
                               e.g. ["1", "9"] to skip NACHA file header/control records
        """
        self._schema      = schema
        self._skip_types  = set(skip_record_types or [])
        self._colspecs    = [(f["start"], f["start"] + f["length"]) for f in schema]
        self._names       = [f["name"] for f in schema]
        self._types       = {f["name"]: f.get("type", "str") for f in schema}

    @property
    def format_name(self) -> str:
        return "Fixed-width"

    def parse(self, source: Any) -> pd.DataFrame:
        text = self._load(source)
        lines = [l for l in text.splitlines() if l.strip()]

        if not lines:
            raise ParserError("Fixed-width file is empty.")

        records = []
        skipped = 0
        for line in lines:
            # pad short lines to avoid index errors
            line = line.ljust(max(s + l for s, l in self._colspecs))
            record = {}
            for field, (start, end) in zip(self._schema, self._colspecs):
                raw = line[start:end].strip()
                record[field["name"]] = self._cast(raw, field.get("type", "str"))

            # skip header/trailer record types
            rt = record.get("record_type", "")
            if self._skip_types and rt in self._skip_types:
                skipped += 1
                continue

            records.append(record)

        if not records:
            raise ParserError("Fixed-width file had no data records after filtering.")

        df = pd.DataFrame(records)
        logger.info(
            f"Fixed-width parse complete — {len(df)} records "
            f"({skipped} skipped), {len(df.columns)} fields"
        )
        return df

    def _cast(self, value: str, type_hint: str) -> Any:
        if not value:
            return None
        if type_hint == "int":
            try:
                return int(value)
            except ValueError:
                return None
        if type_hint == "float":
            try:
                return float(value)
            except ValueError:
                return None
        if type_hint == "implied_decimal_2":
            # common in financial files: 1000 = $10.00
            try:
                return round(int(value) / 100, 2)
            except ValueError:
                return None
        if type_hint.startswith("date_"):
            from datetime import datetime
            fmt = type_hint.replace("date_", "%").replace("YYYY", "%Y").replace(
                "MM", "%m").replace("DD", "%d")
            # simplify
            fmt_map = {"date_YYYYMMDD": "%Y%m%d", "date_MMDDYYYY": "%m%d%Y",
                       "date_YYMMDD": "%y%m%d"}
            fmt = fmt_map.get(type_hint, "%Y%m%d")
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                return None
        return value  # str default

    def _load(self, source: Any) -> str:
        if isinstance(source, bytes):
            return source.decode("latin-1", errors="replace")
        if isinstance(source, Path):
            return source.read_text(encoding="latin-1")
        if isinstance(source, str):
            if len(source) < 512:
                path = Path(source)
                if path.exists():
                    return path.read_text(encoding="latin-1")
            return source
        raise ParserError(f"Unsupported source type: {type(source)}")

    # ── Named presets ──────────────────────────────────────────────────────

    @classmethod
    def nacha_ach(cls) -> "FixedWidthParser":
        """
        NACHA ACH detail record (record type 6).
        Used for payroll direct deposit and ACH payment files.
        """
        schema = [
            {"name": "record_type",          "start": 0,  "length": 1},
            {"name": "transaction_code",     "start": 1,  "length": 2},
            {"name": "routing_number",       "start": 3,  "length": 9},
            {"name": "account_number",       "start": 12, "length": 17},
            {"name": "amount",               "start": 29, "length": 10, "type": "implied_decimal_2"},
            {"name": "individual_id",        "start": 39, "length": 15},
            {"name": "individual_name",      "start": 54, "length": 22},
            {"name": "discretionary_data",   "start": 76, "length": 2},
            {"name": "addenda_indicator",    "start": 78, "length": 1},
            {"name": "trace_number",         "start": 79, "length": 15},
        ]
        return cls(schema, skip_record_types=["1", "5", "8", "9"])

    @classmethod
    def irs_1099(cls) -> "FixedWidthParser":
        """IRS 1099 payee B record — simplified key fields."""
        schema = [
            {"name": "record_type",      "start": 0,   "length": 1},
            {"name": "tax_year",         "start": 1,   "length": 4},
            {"name": "payer_tin",        "start": 12,  "length": 9},
            {"name": "payer_name_ctrl",  "start": 21,  "length": 4},
            {"name": "amount_1",         "start": 28,  "length": 12, "type": "implied_decimal_2"},
            {"name": "amount_2",         "start": 40,  "length": 12, "type": "implied_decimal_2"},
            {"name": "payee_tin",        "start": 64,  "length": 9},
            {"name": "payee_name",       "start": 93,  "length": 40},
            {"name": "payee_address",    "start": 173, "length": 40},
            {"name": "payee_city",       "start": 213, "length": 40},
            {"name": "payee_state",      "start": 253, "length": 2},
            {"name": "payee_zip",        "start": 255, "length": 9},
        ]
        return cls(schema, skip_record_types=["T", "A", "C", "K", "F"])
