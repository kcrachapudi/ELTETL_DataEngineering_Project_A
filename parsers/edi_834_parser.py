"""
EDI 834 — Benefit Enrollment and Maintenance (ANSI X12 5010 / X220A1)

Direction:  Employer / Sponsor → Health Plan / Payer
Purpose:    Add, change, terminate member coverage. The authoritative source
            of truth for who is enrolled in what plan.

Real-world uses:
    - HR systems (Workday, SAP) sending enrollment files to Blue Cross, Aetna, etc.
    - Open enrollment changes
    - New hire additions / termination removals
    - COBRA enrollment

Loop structure (simplified):
    Loop 1000A  Sponsor (employer)
    Loop 1000B  Payer (health plan)
    Loop 2000   Member level
      Loop 2100A  Member name / demographics
      Loop 2200   Disability / employment info
      Loop 2300   Health coverage
        Loop 2310   Provider info (PCP)
        Loop 2320   Coordination of benefits
      Loop 2700   Member supplemental info

Output: one row per member-coverage combination.
Key columns: member_id, subscriber_id, first_name, last_name, dob, gender,
             relationship_code, relationship, plan_id, coverage_type,
             maintenance_type, effective_date, termination_date,
             employer_name, payer_name, street, city, state, zip
"""

import logging
from typing import Any
from datetime import datetime, date

import pandas as pd

from .base_parser import BaseParser, ParserError
from .edi_utils import EDIEnvelopeParser, safe_float, safe_date, e

logger = logging.getLogger(__name__)

RELATIONSHIP_CODES = {
    "18": "self",
    "01": "spouse",
    "19": "child",
    "15": "disabled_dependent",
    "17": "step_child",
    "53": "life_partner",
    "G8": "other_relationship",
}

MAINTENANCE_TYPES = {
    "001": "change",
    "021": "addition",
    "024": "cancellation_termination",
    "025": "reinstatement",
    "030": "audit_compare",
    "032": "employee_information_not_applicable",
}

GENDER_CODES = {"M": "male", "F": "female", "U": "unknown"}

COVERAGE_TYPES = {
    "HLT": "health",
    "DEN": "dental",
    "VIS": "vision",
    "FAC": "facility",
    "MED": "medical",
    "PDG": "pharmacy",
}


class EDI834Parser(BaseParser):
    """Parses EDI 834 Benefit Enrollment files into a member-coverage DataFrame."""

    @property
    def format_name(self) -> str:
        return "EDI X12 5010 834 Benefit Enrollment"

    def parse(self, source: Any) -> pd.DataFrame:
        env = EDIEnvelopeParser(source)
        transactions = env.extract_transactions(["834"])
        if not transactions:
            raise ParserError("No 834 transactions found.")

        all_rows = []
        for tx in transactions:
            rows = self._parse_834(tx["segments"], tx["isa"], tx["gs"])
            all_rows.extend(rows)

        if not all_rows:
            raise ParserError("834 parsed but produced no member rows.")

        df = pd.DataFrame(all_rows)
        logger.info(f"834 parse complete — {len(df)} member-coverage rows, "
                    f"{df['member_id'].nunique()} unique members")
        return df

    def _parse_834(self, segments, isa, gs):
        rows = []

        sponsor = {"employer_name": "", "employer_id": ""}
        payer   = {"payer_name": "",   "payer_id": ""}

        member   = {}
        coverage = {}
        in_2000  = False

        def _flush():
            if member and coverage:
                rows.append({
                    **isa, **gs,
                    "transaction_type": "834",
                    **sponsor, **payer, **member, **coverage,
                })

        for seg in segments:
            sid = seg[0].strip()

            # ── Sponsor (employer) ──────────────────────────────────────────
            if sid == "N1" and e(seg, 1) == "P5":
                sponsor["employer_name"] = e(seg, 2)
                sponsor["employer_id"]   = e(seg, 4)

            # ── Payer ───────────────────────────────────────────────────────
            elif sid == "N1" and e(seg, 1) == "IN":
                payer["payer_name"] = e(seg, 2)
                payer["payer_id"]   = e(seg, 4)

            # ── Member loop (INS opens a new member) ────────────────────────
            elif sid == "INS":
                _flush()
                maint_code = e(seg, 3)
                rel_code   = e(seg, 2)
                member = {
                    "subscriber_indicator": e(seg, 1),
                    "relationship_code":    rel_code,
                    "relationship":         RELATIONSHIP_CODES.get(rel_code, rel_code),
                    "maintenance_type_code": maint_code,
                    "maintenance_type":     MAINTENANCE_TYPES.get(maint_code, maint_code),
                    "maintenance_reason":   e(seg, 4),
                    "benefit_status":       e(seg, 5),
                    "member_id": "", "subscriber_id": "",
                    "first_name": "", "last_name": "", "middle_name": "",
                    "dob": None, "gender": "", "gender_code": "",
                    "ssn": "", "street": "", "city": "",
                    "state": "", "zip": "",
                }
                coverage = {
                    "plan_id": "", "coverage_type": "", "coverage_type_code": "",
                    "effective_date": None, "termination_date": None,
                    "group_number": "", "group_name": "",
                    "cobra_indicator": "",
                }
                in_2000 = True

            elif sid == "REF" and in_2000:
                ref_qual = e(seg, 1)
                ref_val  = e(seg, 2)
                if ref_qual == "0F":
                    member["subscriber_id"] = ref_val
                elif ref_qual == "1L":
                    member["member_id"] = ref_val
                elif ref_qual == "17":
                    coverage["group_number"] = ref_val
                elif ref_qual == "ZZ":
                    member["member_id"] = member["member_id"] or ref_val

            # ── Member name / demographics ──────────────────────────────────
            elif sid == "NM1" and in_2000 and e(seg, 1) in ("IL", "74"):
                member["last_name"]  = e(seg, 3)
                member["first_name"] = e(seg, 4)
                member["middle_name"]= e(seg, 5)
                member["member_id"]  = member["member_id"] or e(seg, 9)

            elif sid == "N3" and in_2000:
                member["street"] = e(seg, 1)

            elif sid == "N4" and in_2000:
                member["city"]  = e(seg, 1)
                member["state"] = e(seg, 2)
                member["zip"]   = e(seg, 3)

            elif sid == "DMG" and in_2000:
                member["dob"]          = safe_date(e(seg, 2))
                gc = e(seg, 3)
                member["gender_code"]  = gc
                member["gender"]       = GENDER_CODES.get(gc, gc)

            elif sid == "SSN" and in_2000:
                member["ssn"] = e(seg, 1)

            # ── Health coverage (HD) ────────────────────────────────────────
            elif sid == "HD" and in_2000:
                cov_code = e(seg, 3)
                coverage["maintenance_type_code"] = e(seg, 1)
                coverage["coverage_type_code"]    = cov_code
                coverage["coverage_type"]         = COVERAGE_TYPES.get(cov_code, cov_code)
                coverage["plan_id"]               = e(seg, 4)
                coverage["group_name"]            = e(seg, 5)

            elif sid == "DTP" and in_2000:
                dtp_qual = e(seg, 1)
                dtp_val  = e(seg, 3)
                if dtp_qual == "348":
                    coverage["effective_date"]   = safe_date(dtp_val)
                elif dtp_qual == "349":
                    coverage["termination_date"] = safe_date(dtp_val)
                elif dtp_qual == "336":
                    member["dob"] = member["dob"] or safe_date(dtp_val)

            elif sid == "COB" and in_2000:
                coverage["cobra_indicator"] = e(seg, 1)

        _flush()
        return rows
