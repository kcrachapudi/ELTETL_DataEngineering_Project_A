"""
Shared utilities for all EDI parsers.

EDIEnvelopeParser  — reads ISA/GS/ST/SE structure, extracts transactions
safe_date          — parse YYYYMMDD / YYMMDD / YYYY-MM-DD → date or None
safe_float         — parse string → float or None
e(seg, n)          — safely get element n from a segment list
"""

from datetime import datetime, date
from pathlib import Path
from typing import Any

from .base_parser import ParserError


def e(seg: list, n: int, default: str = "") -> str:
    """Safely retrieve element n from a segment, stripping whitespace."""
    return seg[n].strip() if len(seg) > n else default


def safe_float(value: str) -> float | None:
    try:
        return float(value.strip())
    except (ValueError, AttributeError):
        return None


def safe_date(value: str) -> date | None:
    value = (value or "").strip().replace("-", "")
    formats = ["%Y%m%d", "%y%m%d"]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


class EDIEnvelopeParser:
    """
    Reads any X12 5010 file and extracts individual transactions by type.
    Handles:
        - Auto-detection of element / segment / sub-element delimiters
        - Multiple functional groups in one interchange
        - Multiple transaction sets in one functional group
        - Multiple interchanges in one file
    """

    def __init__(self, source: Any):
        self._raw = self._load(source)
        self._element_sep = "*"
        self._segment_sep = "~"
        self._sub_sep     = ":"
        self._interchanges: list[dict] = []
        self._parse()

    def _load(self, source: Any) -> str:
        if isinstance(source, bytes):
            return source.decode("utf-8", errors="replace")
        if isinstance(source, Path):
            return source.read_text(encoding="utf-8", errors="replace")
        if isinstance(source, str):
            if len(source) < 512 and not source.strip().startswith("ISA"):
                p = Path(source)
                if p.exists():
                    return p.read_text(encoding="utf-8", errors="replace")
            return source
        raise ParserError(f"Unsupported source type: {type(source)}")

    def _detect_delimiters(self, raw: str):
        raw = raw.strip()
        if not raw.startswith("ISA"):
            raise ParserError("EDI file must begin with ISA segment.")
        self._element_sep = raw[3]
        self._sub_sep     = raw[104] if len(raw) > 104 else ":"
        self._segment_sep = raw[105] if len(raw) > 105 else "~"

    def _parse(self):
        self._detect_delimiters(self._raw)
        raw_segs = self._raw.split(self._segment_sep)

        isa_data, gs_data = {}, {}
        current_tx = None

        for raw_seg in raw_segs:
            raw_seg = raw_seg.strip().lstrip("\n\r")
            if not raw_seg:
                continue
            seg = raw_seg.split(self._element_sep)
            sid = seg[0].strip()

            if sid == "ISA":
                isa_data = {
                    "sender_id":                  e(seg, 6),
                    "receiver_id":                e(seg, 8),
                    "interchange_date":           safe_date(e(seg, 9)),
                    "interchange_control_number": e(seg, 13),
                    "version":                    e(seg, 12),
                }

            elif sid == "GS":
                gs_data = {
                    "functional_id":        e(seg, 1),
                    "group_sender":         e(seg, 2),
                    "group_receiver":       e(seg, 3),
                    "group_date":           safe_date(e(seg, 4)),
                    "group_control_number": e(seg, 6),
                }

            elif sid == "ST":
                tx_type = e(seg, 1)
                current_tx = {
                    "transaction_type": tx_type,
                    "isa":              isa_data.copy(),
                    "gs":               gs_data.copy(),
                    "segments":         [],
                }

            elif sid == "SE":
                if current_tx:
                    self._interchanges.append(current_tx)
                    current_tx = None

            elif current_tx is not None:
                current_tx["segments"].append(seg)

    def extract_transactions(self, types: list[str]) -> list[dict]:
        """Return all transactions matching any of the given type codes."""
        types_upper = [t.upper() for t in types]
        return [tx for tx in self._interchanges
                if tx["transaction_type"] in types_upper]
