"""
EDI X12 Parser — fully working, pure Python, no external dependencies.

Supports the four most common transaction sets you will encounter:
    850  Purchase Order
    856  Advance Ship Notice (ASN)
    810  Invoice
    997  Functional Acknowledgement

EDI X12 structure (outermost to innermost):
    ISA  Interchange envelope     — one per file, sender/receiver IDs
    GS   Functional group         — groups related transactions
    ST   Transaction set          — the actual document (850, 856, etc.)
    <segments>                    — the data
    SE   Transaction set trailer
    GE   Group trailer
    IEA  Interchange trailer

A segment looks like:
    BEG*00*SA*PO-12345**20240101~
    └─┘ └────────────────────────┘└┘
    ID   elements (split on *)    terminator (~)

Usage:
    parser = EDIParser()
    df = parser.parse("path/to/file.edi")       # from file path
    df = parser.parse(edi_string)               # from raw string
"""

import re
import logging
from pathlib import Path
from typing import Any, Union
from datetime import datetime, date

import pandas as pd

from .base_parser import BaseParser, ParserError

logger = logging.getLogger(__name__)


class EDIParser(BaseParser):
    """
    Parses EDI X12 files into a pandas DataFrame.

    Each row in the output represents one line-item or one transaction,
    depending on the transaction type. The 'transaction_type' column
    always tells you which document the row came from.

    Delimiters are auto-detected from the ISA envelope — you do not
    need to configure anything for standard X12 files.
    """

    SUPPORTED_TRANSACTIONS = {"850", "856", "810", "997"}

    def __init__(self):
        self._element_sep = "*"
        self._segment_sep = "~"
        self._sub_sep = ">"

    @property
    def format_name(self) -> str:
        return "EDI X12"

    def parse(self, source: Any) -> pd.DataFrame:
        """
        Args:
            source: file path string, Path object, or raw EDI string/bytes.

        Returns:
            DataFrame with columns depending on transaction type.
            Always includes: transaction_type, sender_id, receiver_id,
            interchange_date, interchange_control_number.
        """
        raw = self._load_source(source)
        raw = self._detect_and_normalise_delimiters(raw)
        segments = self._split_segments(raw)

        if not segments:
            raise ParserError("No segments found — empty or invalid EDI input.")

        envelopes = self._split_into_transactions(segments)
        logger.info(f"Found {len(envelopes)} transaction(s) in EDI source.")

        frames = []
        for envelope in envelopes:
            try:
                df = self._parse_transaction(envelope)
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception as e:
                tx_id = envelope.get("transaction_type", "unknown")
                logger.warning(f"Skipping transaction {tx_id}: {e}")

        if not frames:
            raise ParserError("EDI parsed but produced no usable rows.")

        result = pd.concat(frames, ignore_index=True)
        logger.info(f"EDI parse complete — {len(result)} rows, "
                    f"types: {result['transaction_type'].unique().tolist()}")
        return result

    def _load_source(self, source: Any) -> str:
        """Accept file path, Path object, bytes, or raw string."""
        if isinstance(source, bytes):
            return source.decode("utf-8", errors="replace")
        if isinstance(source, Path):
            return source.read_text(encoding="utf-8", errors="replace")
        if isinstance(source, str):
            # treat as file path only if short and doesn't look like raw EDI
            if len(source) < 512 and not source.strip().startswith("ISA"):
                path = Path(source)
                if path.exists():
                    return path.read_text(encoding="utf-8", errors="replace")
            return source
        raise ParserError(f"Unsupported source type: {type(source)}")

    def _detect_and_normalise_delimiters(self, raw: str) -> str:
        """
        The ISA segment encodes delimiters at fixed positions:
            ISA[103] = element separator  (char at position 3)
            ISA[104] = sub-element sep    (char at position 104)
            ISA[105] = segment terminator (char at position 105)

        Standard X12: element=*, sub=:, terminator=~
        Some trading partners use non-standard delimiters — we handle them.
        """
        raw = raw.strip()
        if not raw.startswith("ISA"):
            raise ParserError("EDI file must begin with ISA segment.")

        self._element_sep = raw[3]
        self._sub_sep = raw[104] if len(raw) > 104 else ">"
        self._segment_sep = raw[105] if len(raw) > 105 else "~"

        logger.debug(f"Delimiters detected — element: {repr(self._element_sep)} "
                     f"sub: {repr(self._sub_sep)} "
                     f"segment: {repr(self._segment_sep)}")
        return raw

    def _split_segments(self, raw: str) -> list[list[str]]:
        """Split raw EDI into a list of segments, each a list of elements."""
        raw_segments = raw.split(self._segment_sep)
        segments = []
        for seg in raw_segments:
            seg = seg.strip().strip("\n").strip("\r")
            if seg:
                elements = seg.split(self._element_sep)
                segments.append(elements)
        return segments

    def _split_into_transactions(self, segments: list) -> list[dict]:
        """
        Walk the segment list and group segments by transaction set (ST→SE).
        Attach ISA/GS envelope data to each transaction.
        """
        envelopes = []
        isa_data = {}
        gs_data = {}
        current_tx = None

        for seg in segments:
            seg_id = seg[0].strip()

            if seg_id == "ISA":
                isa_data = self._parse_isa(seg)

            elif seg_id == "GS":
                gs_data = self._parse_gs(seg)

            elif seg_id == "ST":
                tx_type = seg[1].strip() if len(seg) > 1 else "unknown"
                current_tx = {
                    "transaction_type": tx_type,
                    "isa": isa_data.copy(),
                    "gs": gs_data.copy(),
                    "segments": [],
                }

            elif seg_id == "SE":
                if current_tx is not None:
                    envelopes.append(current_tx)
                    current_tx = None

            elif current_tx is not None:
                current_tx["segments"].append(seg)

        return envelopes

    def _parse_isa(self, seg: list) -> dict:
        """ISA has exactly 16 elements at fixed positions."""
        def _e(n): return seg[n].strip() if len(seg) > n else ""
        return {
            "sender_id":                  _e(6),
            "receiver_id":                _e(8),
            "interchange_date":           self._parse_date(_e(9), short=True),
            "interchange_control_number": _e(13),
            "version":                    _e(12),
        }

    def _parse_gs(self, seg: list) -> dict:
        def _e(n): return seg[n].strip() if len(seg) > n else ""
        return {
            "functional_id":        _e(1),
            "group_sender":         _e(2),
            "group_receiver":       _e(3),
            "group_date":           self._parse_date(_e(4)),
            "group_control_number": _e(6),
        }

    def _parse_transaction(self, envelope: dict) -> pd.DataFrame:
        tx_type = envelope["transaction_type"]
        if tx_type not in self.SUPPORTED_TRANSACTIONS:
            logger.warning(f"Transaction type {tx_type} not supported — skipping.")
            return None

        parsers = {
            "850": self._parse_850,
            "856": self._parse_856,
            "810": self._parse_810,
            "997": self._parse_997,
        }
        df = parsers[tx_type](envelope["segments"])

        for col, val in {**envelope["isa"], **envelope["gs"]}.items():
            df[col] = val
        df["transaction_type"] = tx_type
        return df

    def _base_cols(self, envelope_override: dict = None) -> dict:
        return {}

    def _e(self, seg: list, n: int, default: str = "") -> str:
        return seg[n].strip() if len(seg) > n else default

    def _parse_850(self, segments: list) -> pd.DataFrame:
        """
        850 Purchase Order.
        One row per PO1 (line item). Header fields repeated on every row.
        Key segments:
            BEG  — PO number, date, type
            REF  — reference numbers
            DTM  — dates
            N1   — name/address (buyer, seller, ship-to)
            PO1  — line item detail
            PID  — product description
        """
        header = {
            "po_number": "", "po_date": None, "po_type": "",
            "buyer_name": "", "seller_name": "", "ship_to_name": "",
            "currency": "USD",
        }
        line_items = []
        current_item = {}
        n1_entity = ""

        for seg in segments:
            sid = seg[0].strip()

            if sid == "BEG":
                header["po_type"]   = self._e(seg, 1)
                header["po_number"] = self._e(seg, 3)
                header["po_date"]   = self._parse_date(self._e(seg, 5))

            elif sid == "CUR":
                header["currency"] = self._e(seg, 2)

            elif sid == "N1":
                n1_entity = self._e(seg, 1)
                name = self._e(seg, 2)
                if n1_entity == "BY":
                    header["buyer_name"] = name
                elif n1_entity == "SE":
                    header["seller_name"] = name
                elif n1_entity == "ST":
                    header["ship_to_name"] = name

            elif sid == "PO1":
                if current_item:
                    line_items.append(current_item.copy())
                current_item = {
                    **header,
                    "line_number":     self._e(seg, 1),
                    "quantity":        self._safe_float(self._e(seg, 2)),
                    "unit_of_measure": self._e(seg, 3),
                    "unit_price":      self._safe_float(self._e(seg, 4)),
                    "basis_of_price":  self._e(seg, 5),
                    "product_id_1":    self._e(seg, 7),
                    "product_id_2":    self._e(seg, 9),
                    "description":     "",
                    "line_total":      None,
                }
                if current_item["quantity"] and current_item["unit_price"]:
                    current_item["line_total"] = round(
                        current_item["quantity"] * current_item["unit_price"], 4
                    )

            elif sid == "PID" and current_item:
                current_item["description"] = self._e(seg, 5)

        if current_item:
            line_items.append(current_item)

        if not line_items:
            line_items.append(header)

        return pd.DataFrame(line_items)

    def _parse_856(self, segments: list) -> pd.DataFrame:
        """
        856 Advance Ship Notice.
        One row per HL shipment/order/pack/item hierarchy level.
        Key segments:
            BSN  — shipment number, date
            HL   — hierarchy level (S=shipment, O=order, P=pack, I=item)
            TD1  — carrier/packaging
            TD5  — routing/carrier
            REF  — tracking numbers
            DTM  — ship date
            LIN  — item identification
            SN1  — shipped quantity
        """
        header = {
            "shipment_id": "", "ship_date": None,
            "carrier_code": "", "tracking_number": "",
            "po_number": "",
        }
        rows = []
        current_hl = {}

        for seg in segments:
            sid = seg[0].strip()

            if sid == "BSN":
                header["shipment_id"] = self._e(seg, 2)
                header["ship_date"]   = self._parse_date(self._e(seg, 3))

            elif sid == "TD5":
                header["carrier_code"] = self._e(seg, 2)

            elif sid == "REF":
                ref_qual = self._e(seg, 1)
                if ref_qual == "BM":
                    header["tracking_number"] = self._e(seg, 2)
                elif ref_qual == "PO":
                    header["po_number"] = self._e(seg, 2)

            elif sid == "HL":
                if current_hl:
                    rows.append(current_hl.copy())
                level_map = {"S": "shipment", "O": "order",
                             "P": "pack", "I": "item"}
                level_code = self._e(seg, 3)
                current_hl = {
                    **header,
                    "hl_number":      self._e(seg, 1),
                    "hl_parent":      self._e(seg, 2),
                    "hl_level_code":  level_code,
                    "hl_level":       level_map.get(level_code, level_code),
                    "item_id":        "",
                    "quantity_shipped": None,
                    "unit_of_measure": "",
                }

            elif sid == "LIN" and current_hl:
                current_hl["item_id"] = self._e(seg, 3)

            elif sid == "SN1" and current_hl:
                current_hl["quantity_shipped"] = self._safe_float(self._e(seg, 2))
                current_hl["unit_of_measure"]  = self._e(seg, 3)

        if current_hl:
            rows.append(current_hl)

        if not rows:
            rows.append(header)

        return pd.DataFrame(rows)

    def _parse_810(self, segments: list) -> pd.DataFrame:
        """
        810 Invoice.
        One row per IT1 (invoice line item).
        Key segments:
            BIG  — invoice number, date, PO number
            N1   — name/address
            IT1  — line item
            TDS  — total invoice amount
            SAC  — service/allowance/charge
        """
        header = {
            "invoice_number": "", "invoice_date": None,
            "po_number": "", "seller_name": "", "buyer_name": "",
            "total_amount": None, "currency": "USD",
        }
        line_items = []
        current_item = {}
        n1_entity = ""

        for seg in segments:
            sid = seg[0].strip()

            if sid == "BIG":
                header["invoice_date"]   = self._parse_date(self._e(seg, 1))
                header["invoice_number"] = self._e(seg, 2)
                header["po_number"]      = self._e(seg, 4)

            elif sid == "CUR":
                header["currency"] = self._e(seg, 2)

            elif sid == "N1":
                n1_entity = self._e(seg, 1)
                name = self._e(seg, 2)
                if n1_entity == "SE":
                    header["seller_name"] = name
                elif n1_entity == "BY":
                    header["buyer_name"] = name

            elif sid == "IT1":
                if current_item:
                    line_items.append(current_item.copy())
                current_item = {
                    **header,
                    "line_number":     self._e(seg, 1),
                    "quantity":        self._safe_float(self._e(seg, 2)),
                    "unit_of_measure": self._e(seg, 3),
                    "unit_price":      self._safe_float(self._e(seg, 4)),
                    "product_id":      self._e(seg, 7),
                    "description":     "",
                    "line_total":      None,
                }
                if current_item["quantity"] and current_item["unit_price"]:
                    current_item["line_total"] = round(
                        current_item["quantity"] * current_item["unit_price"], 4
                    )

            elif sid == "PID" and current_item:
                current_item["description"] = self._e(seg, 5)

            elif sid == "TDS":
                # TDS is in cents in X12 standard — divide by 100 for dollars
                raw_total = self._safe_float(self._e(seg, 1))
                header["total_amount"] = round(raw_total / 100, 2) if raw_total else None

        if current_item:
            line_items.append(current_item)

        if not line_items:
            line_items.append(header)

        df = pd.DataFrame(line_items)
        # total_amount is set by TDS after IT1 rows are built — backfill it
        df["total_amount"] = header.get("total_amount")
        return df

    def _parse_997(self, segments: list) -> pd.DataFrame:
        """
        997 Functional Acknowledgement.
        One row per AK2 (transaction set acknowledged).
        Key segments:
            AK1  — group being acknowledged
            AK2  — transaction set acknowledged
            AK5  — transaction set response (accepted/rejected)
            AK9  — group response
        """
        header = {
            "ack_functional_id": "",
            "ack_group_control":  "",
        }
        rows = []
        current_ack = {}

        ack_codes = {
            "A": "accepted",
            "E": "accepted_with_errors",
            "R": "rejected",
            "P": "partially_accepted",
        }

        for seg in segments:
            sid = seg[0].strip()

            if sid == "AK1":
                header["ack_functional_id"] = self._e(seg, 1)
                header["ack_group_control"]  = self._e(seg, 2)

            elif sid == "AK2":
                if current_ack:
                    rows.append(current_ack.copy())
                current_ack = {
                    **header,
                    "ack_transaction_type":    self._e(seg, 1),
                    "ack_control_number":      self._e(seg, 2),
                    "ack_status_code":         "",
                    "ack_status":              "",
                    "ack_error_codes":         "",
                }

            elif sid == "AK5":
                code = self._e(seg, 1)
                current_ack["ack_status_code"] = code
                current_ack["ack_status"]      = ack_codes.get(code, code)
                errors = [self._e(seg, i) for i in range(2, 7) if self._e(seg, i)]
                current_ack["ack_error_codes"] = ",".join(errors)

            elif sid == "AK9":
                if current_ack:
                    rows.append(current_ack.copy())
                    current_ack = {}

        if current_ack:
            rows.append(current_ack)

        if not rows:
            rows.append(header)

        return pd.DataFrame(rows)

    def _parse_date(self, value: str, short: bool = False) -> date | None:
        """Parse YYYYMMDD or YYMMDD into a Python date. Returns None on failure."""
        value = value.strip()
        try:
            if short and len(value) == 6:
                return datetime.strptime(value, "%y%m%d").date()
            if len(value) == 8:
                return datetime.strptime(value, "%Y%m%d").date()
        except ValueError:
            pass
        return None

    def _safe_float(self, value: str) -> float | None:
        """Convert string to float, returning None if it fails."""
        try:
            return float(value.strip())
        except (ValueError, AttributeError):
            return None
