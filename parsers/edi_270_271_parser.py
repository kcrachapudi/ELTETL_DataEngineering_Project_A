"""
EDI 270 / 271 — Eligibility Benefit Inquiry and Response (ANSI X12 5010 / X279A1)

270  Direction:  Provider → Payer   "Is this patient covered?"
271  Direction:  Payer → Provider   "Yes, here are their benefits."

These are typically real-time request/response pairs. A provider's
practice management system sends a 270 at check-in, receives a 271
back within seconds.

Key loop structure (same for both):
    Loop 2000A  Information source (payer for 271, receiver for 270)
    Loop 2000B  Information receiver (provider)
    Loop 2000C  Subscriber
      Loop 2000D  Dependent (if applicable)
        Loop 2100C/D  Patient name / address
          Loop 2110C/D  Eligibility / benefit information (271 only)

Output:
    270 → one row per subscriber inquiry
    271 → one row per benefit reported (EB segment)

Key columns (270): subscriber_id, subscriber_name, dob, gender,
                   service_type_code, service_type,
                   provider_npi, provider_name, payer_id, payer_name,
                   trace_number, inquiry_date

Key columns (271 additional): coverage_active, eligibility_code,
                   benefit_amount, benefit_percent, in_network,
                   plan_id, plan_name, group_number, group_name,
                   deductible, deductible_remaining,
                   oop_max, oop_remaining, copay, coinsurance
"""

import logging
from typing import Any

import pandas as pd

from .base_parser import BaseParser, ParserError
from .edi_utils import EDIEnvelopeParser, safe_float, safe_date, e

logger = logging.getLogger(__name__)

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


class EDI270Parser(BaseParser):
    """Parses EDI 270 Eligibility Inquiry files."""

    @property
    def format_name(self) -> str:
        return "EDI X12 5010 270 Eligibility Inquiry"

    def parse(self, source: Any) -> pd.DataFrame:
        env = EDIEnvelopeParser(source)
        transactions = env.extract_transactions(["270"])
        if not transactions:
            raise ParserError("No 270 transactions found.")

        all_rows = []
        for tx in transactions:
            rows = self._parse_270(tx["segments"], tx["isa"], tx["gs"])
            all_rows.extend(rows)

        if not all_rows:
            raise ParserError("270 parsed but produced no inquiry rows.")

        df = pd.DataFrame(all_rows)
        logger.info(f"270 parse complete — {len(df)} eligibility inquiries")
        return df

    def _parse_270(self, segments, isa, gs):
        rows = []
        payer    = {"payer_name": "", "payer_id": ""}
        provider = {"provider_npi": "", "provider_name": "", "provider_tax_id": ""}
        trace    = {"trace_number": "", "inquiry_date": None}
        subscriber = {}
        loop_ctx = None

        def _flush():
            if subscriber:
                rows.append({
                    **isa, **gs,
                    "transaction_type": "270",
                    **payer, **provider, **trace, **subscriber,
                })

        for seg in segments:
            sid = seg[0].strip()

            if sid == "BHT":
                trace["trace_number"]  = e(seg, 3)
                trace["inquiry_date"]  = safe_date(e(seg, 4))

            elif sid == "NM1":
                nm1_qual = e(seg, 1)
                if nm1_qual == "PR":
                    payer["payer_name"] = e(seg, 3)
                    payer["payer_id"]   = e(seg, 9)
                    loop_ctx = "2000A"
                elif nm1_qual in ("1P", "FA"):
                    provider["provider_name"] = (
                        e(seg, 3) if e(seg, 2) == "2"
                        else f"{e(seg, 3)}, {e(seg, 4)}"
                    )
                    provider["provider_npi"]  = e(seg, 9)
                    loop_ctx = "2000B"
                elif nm1_qual == "IL":
                    _flush()
                    subscriber = {
                        "subscriber_id":       e(seg, 9),
                        "subscriber_last":     e(seg, 3),
                        "subscriber_first":    e(seg, 4),
                        "subscriber_dob":      None,
                        "subscriber_gender":   "",
                        "service_type_code":   "",
                        "service_type":        "",
                        "relationship_code":   "18",
                        "relationship":        "self",
                    }
                    loop_ctx = "2000C"

            elif sid == "REF" and loop_ctx == "2000B":
                if e(seg, 1) in ("EI", "TJ"):
                    provider["provider_tax_id"] = e(seg, 2)

            elif sid == "DMG" and loop_ctx in ("2000C", "2000D"):
                subscriber["subscriber_dob"]    = safe_date(e(seg, 2))
                subscriber["subscriber_gender"] = e(seg, 3)

            elif sid == "EQ" and loop_ctx == "2000C":
                stc = e(seg, 1)
                subscriber["service_type_code"] = stc
                subscriber["service_type"]      = SERVICE_TYPE_CODES.get(stc, stc)

            elif sid == "NM1" and e(seg, 1) == "03":
                loop_ctx = "2000D"
                subscriber["relationship_code"] = "19"
                subscriber["relationship"]      = "child"

        _flush()
        return rows


class EDI271Parser(BaseParser):
    """Parses EDI 271 Eligibility Response files — one row per EB benefit segment."""

    @property
    def format_name(self) -> str:
        return "EDI X12 5010 271 Eligibility Response"

    def parse(self, source: Any) -> pd.DataFrame:
        env = EDIEnvelopeParser(source)
        transactions = env.extract_transactions(["271"])
        if not transactions:
            raise ParserError("No 271 transactions found.")

        all_rows = []
        for tx in transactions:
            rows = self._parse_271(tx["segments"], tx["isa"], tx["gs"])
            all_rows.extend(rows)

        if not all_rows:
            raise ParserError("271 parsed but produced no benefit rows.")

        df = pd.DataFrame(all_rows)
        logger.info(f"271 parse complete — {len(df)} benefit rows, "
                    f"{df['subscriber_id'].nunique()} subscribers")
        return df

    def _parse_271(self, segments, isa, gs):
        rows = []
        payer    = {"payer_name": "", "payer_id": ""}
        provider = {"provider_npi": "", "provider_name": ""}
        trace    = {"trace_number": "", "response_date": None}
        subscriber = {}
        plan     = {"plan_id": "", "plan_name": "", "group_number": "", "group_name": ""}
        benefits = []
        loop_ctx = None

        def _flush_subscriber():
            if not subscriber:
                return
            base = {
                **isa, **gs,
                "transaction_type": "271",
                **payer, **provider, **trace,
                **subscriber, **plan,
            }
            if benefits:
                for b in benefits:
                    rows.append({**base, **b})
            else:
                rows.append({**base,
                             "eligibility_code": "", "eligibility_description": "",
                             "coverage_level": "", "service_type_code": "",
                             "service_type": "", "in_network": "",
                             "benefit_amount": None, "benefit_percent": None,
                             "benefit_period": "", "coverage_active": False,
                             "messages": ""})

        for seg in segments:
            sid = seg[0].strip()

            if sid == "BHT":
                trace["trace_number"]  = e(seg, 3)
                trace["response_date"] = safe_date(e(seg, 4))

            elif sid == "NM1":
                nm1_qual = e(seg, 1)
                if nm1_qual == "PR":
                    payer["payer_name"] = e(seg, 3)
                    payer["payer_id"]   = e(seg, 9)
                    loop_ctx = "2000A"
                elif nm1_qual in ("1P", "FA"):
                    provider["provider_name"] = e(seg, 3)
                    provider["provider_npi"]  = e(seg, 9)
                    loop_ctx = "2000B"
                elif nm1_qual == "IL":
                    _flush_subscriber()
                    benefits = []
                    plan = {"plan_id": "", "plan_name": "",
                            "group_number": "", "group_name": ""}
                    subscriber = {
                        "subscriber_id":     e(seg, 9),
                        "subscriber_last":   e(seg, 3),
                        "subscriber_first":  e(seg, 4),
                        "subscriber_dob":    None,
                        "subscriber_gender": "",
                    }
                    loop_ctx = "2000C"

            elif sid == "DMG" and loop_ctx in ("2000C", "2000D"):
                subscriber["subscriber_dob"]    = safe_date(e(seg, 2))
                subscriber["subscriber_gender"] = e(seg, 3)

            elif sid == "DTP" and loop_ctx in ("2000C", "2000D"):
                if e(seg, 1) == "307":
                    subscriber["eligibility_begin"] = safe_date(e(seg, 3))
                elif e(seg, 1) == "308":
                    subscriber["eligibility_end"] = safe_date(e(seg, 3))

            elif sid == "REF" and loop_ctx in ("2000C", "2000D"):
                ref_qual = e(seg, 1)
                ref_val  = e(seg, 2)
                if ref_qual == "18":
                    plan["plan_id"]      = ref_val
                elif ref_qual == "1L":
                    plan["group_number"] = ref_val
                elif ref_qual == "9F":
                    plan["group_name"]   = ref_val
                elif ref_qual == "CE":
                    plan["plan_name"]    = ref_val

            # ── Eligibility/Benefit (EB) — the payload of the 271 ──────────
            elif sid == "EB":
                elig_code = e(seg, 1)
                stc       = e(seg, 3)
                cov_level = e(seg, 2)
                in_net    = e(seg, 12)
                benefit = {
                    "eligibility_code":        elig_code,
                    "eligibility_description": ELIGIBILITY_CODES.get(elig_code, elig_code),
                    "coverage_level_code":     cov_level,
                    "coverage_level":          COVERAGE_LEVEL.get(cov_level, cov_level),
                    "service_type_code":       stc,
                    "service_type":            SERVICE_TYPE_CODES.get(stc, stc),
                    "insurance_type":          e(seg, 4),
                    "plan_coverage_description": e(seg, 5),
                    "time_period_qualifier":   e(seg, 6),
                    "benefit_period":          BENEFIT_PERIOD.get(e(seg, 6), e(seg, 6)),
                    "benefit_amount":          safe_float(e(seg, 7)),
                    "benefit_percent":         safe_float(e(seg, 8)),
                    "quantity_qualifier":      e(seg, 9),
                    "quantity":                safe_float(e(seg, 10)),
                    "authorization_required":  e(seg, 11),
                    "in_network_code":         in_net,
                    "in_network":              IN_NETWORK_CODES.get(in_net, in_net),
                    "procedure_code":          e(seg, 13),
                    "coverage_active":         elig_code in ("1","2","3","4","5"),
                    "messages":                "",
                }
                benefits.append(benefit)
                loop_ctx = "2110"

            # ── Benefit-level messages (MSG) ─────────────────────────────────
            elif sid == "MSG" and loop_ctx == "2110" and benefits:
                benefits[-1]["messages"] = (
                    (benefits[-1]["messages"] + " | " if benefits[-1]["messages"] else "")
                    + e(seg, 1)
                )

            # ── Benefit dates (DTP in 2110) ──────────────────────────────────
            elif sid == "DTP" and loop_ctx == "2110" and benefits:
                dtp_qual = e(seg, 1)
                if dtp_qual == "292":
                    benefits[-1]["benefit_begin"] = safe_date(e(seg, 3))
                elif dtp_qual == "093":
                    benefits[-1]["benefit_end"]   = safe_date(e(seg, 3))

        _flush_subscriber()
        return rows
