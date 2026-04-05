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


SERVICE_TYPE_CODES = {
    "1":  "medical_care",
    "2":  "surgical",
    "3":  "consultation",
    "4":  "diagnostic_xray",
    "5":  "diagnostic_lab",
    "6":  "radiation_therapy",
    "7":  "anesthesia",
    "12": "durable_medical_equipment",
    "23": "emergency_services",
    "30": "health_benefit_plan_coverage",
    "33": "chiropractic",
    "35": "dental_care",
    "42": "home_health_care",
    "45": "hospice",
    "47": "hospital",
    "48": "hospital_inpatient",
    "50": "hospital_outpatient",
    "51": "hospital_room_board",
    "54": "long_term_care",
    "55": "major_medical",
    "60": "vision_optometry",
    "62": "maternity",
    "63": "mental_health",
    "65": "newborn_care",
    "68": "occupational_therapy",
    "76": "dialysis",
    "78": "pharmacy",
    "82": "physical_therapy",
    "83": "physician_visit_office",
    "84": "physician_visit_hospital",
    "86": "podiatry",
    "88": "preventive",
    "93": "substance_abuse",
    "98": "professional_physician",
    "A0": "skilled_nursing",
    "A3": "vision",
    "AJ": "mental_health_inpatient",
    "AK": "mental_health_outpatient",
    "AL": "substance_abuse_inpatient",
    "BB": "substance_abuse_outpatient",
    "UC": "urgent_care",
}

ELIGIBILITY_CODES = {
    "1":  "active_coverage",
    "2":  "active_full_risk_capitation",
    "3":  "active_services_capitated",
    "4":  "active_services_capitated_primary",
    "5":  "active_pending_investigation",
    "6":  "inactive",
    "7":  "inactive_pending_eligibility",
    "8":  "inactive_pending_investigation",
    "A":  "co_insurance",
    "B":  "co_payment",
    "C":  "deductible",
    "CB": "coverage_basis",
    "D":  "benefit_description",
    "E":  "exclusions",
    "F":  "limitations",
    "G":  "out_of_pocket_stop_loss",
    "H":  "unlimited",
    "I":  "non_covered",
    "J":  "cost_containment",
    "K":  "reserve",
    "L":  "primary_care_provider",
    "M":  "pre_existing_condition",
    "MC": "co_payment",
    "N":  "services_restricted_to_following_provider",
    "O":  "not_deemed_medical_necessary",
    "P":  "benefit_disclaimer",
    "Q":  "second_surgical_opinion_required",
    "R":  "other_or_additional_payor",
    "S":  "prior_year_deductible",
    "T":  "card_reported_lost_stolen",
    "U":  "contact_following_entity",
    "V":  "cannot_process",
    "W":  "other_source_of_data",
    "X":  "health_care_facility",
    "Y":  "spend_down",
}

COVERAGE_LEVEL = {
    "CHD": "children_only",
    "DEP": "dependents_only",
    "ECH": "employee_children",
    "EMP": "employee_only",
    "ESP": "employee_spouse",
    "FAM": "family",
    "IND": "individual",
    "SPC": "sponsored_dependent",
    "TWO": "two_party",
}

BENEFIT_PERIOD = {
    "23": "calendar_year",
    "24": "year_to_date",
    "25": "contract",
    "26": "episode",
    "27": "visit",
    "28": "outlook_12_months",
    "29": "remaining",
    "30": "exceeded",
    "31": "not_exceeded",
    "32": "unlimited",
    "33": "day",
    "34": "week",
    "35": "month",
    "36": "episode",
    "6":  "benefit_year",
    "7":  "lifetime",
    "WT": "week",
}

IN_NETWORK_CODES = {"Y": "in_network", "N": "out_of_network", "W": "not_applicable"}

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
