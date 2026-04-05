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


