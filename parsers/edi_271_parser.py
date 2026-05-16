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
from .edi_utils import *

logger = logging.getLogger(__name__)

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
