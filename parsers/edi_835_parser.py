"""
EDI 835 — Health Care Claim Payment/Advice (ANSI X12 5010 / X221A1)

Direction:  Payer → Provider
Purpose:    Explain exactly what was paid, adjusted, or denied for each claim.
            The financial backbone of healthcare revenue cycle management.
            Every dollar that flows through healthcare hits an 835.

Key loop structure:
    Loop 1000A  Payer identification
    Loop 1000B  Payee (provider) identification
    Loop 2000   Header number (one per payment batch)
      Loop 2100  Claim payment (CLP — one per claim)
        Loop 2110  Service payment (SVC — one per service line)

Output: one row per service line adjustment.
Key columns:
    claim_id, patient_control_number, patient_name,
    claim_status, claim_status_description,
    claim_charge, claim_payment, claim_adjustment_total,
    procedure_code, modifier, line_charge, line_payment,
    adjustment_group, adjustment_reason, adjustment_amount,
    payer_name, payee_name, payee_npi,
    check_number, check_date, check_amount

Adjustment group codes (CAS):
    CO  Contractual obligation  (write-off — provider cannot bill patient)
    PR  Patient responsibility  (deductible / copay / coinsurance)
    OA  Other adjustment
    PI  Payer-initiated reduction
    CR  Correction / reversal

Common claim status codes (CLP02):
    1   Processed as primary
    2   Processed as secondary
    3   Processed as tertiary
    4   Denied
    19  Processed as primary, forwarded to additional payer
    20  Not our claim
    22  Reversal of prior payment
"""

import logging
from typing import Any

import pandas as pd

from .base_parser import BaseParser, ParserError
from .edi_utils import EDIEnvelopeParser, safe_float, safe_date, e

logger = logging.getLogger(__name__)

CLAIM_STATUS = {
    "1":  "processed_primary",
    "2":  "processed_secondary",
    "3":  "processed_tertiary",
    "4":  "denied",
    "19": "processed_primary_forwarded",
    "20": "not_our_claim",
    "22": "reversal",
    "23": "not_our_claim_forwarded",
}

ADJUSTMENT_GROUPS = {
    "CO": "contractual_obligation",
    "PR": "patient_responsibility",
    "OA": "other_adjustment",
    "PI": "payer_initiated",
    "CR": "correction_reversal",
}


class EDI835Parser(BaseParser):
    """Parses EDI 835 Remittance Advice files into a service-line payment DataFrame."""

    @property
    def format_name(self) -> str:
        return "EDI X12 5010 835 Remittance Advice"

    def parse(self, source: Any) -> pd.DataFrame:
        env = EDIEnvelopeParser(source)
        transactions = env.extract_transactions(["835"])
        if not transactions:
            raise ParserError("No 835 transactions found.")

        all_rows = []
        for tx in transactions:
            rows = self._parse_835(tx["segments"], tx["isa"], tx["gs"])
            all_rows.extend(rows)

        if not all_rows:
            raise ParserError("835 parsed but produced no remittance rows.")

        df = pd.DataFrame(all_rows)
        logger.info(
            f"835 parse complete — {len(df)} service lines across "
            f"{df['claim_id'].nunique()} claims, "
            f"total paid: ${df['line_payment'].sum():,.2f}"
        )
        return df

    def _parse_835(self, segments, isa, gs):
        rows = []

        payer  = {"payer_name":  "", "payer_id":  ""}
        payee  = {"payee_name":  "", "payee_npi": "", "payee_tax_id": ""}
        check  = {"check_number": "", "check_date": None, "check_amount": None,
                  "payment_method": "", "eft_trace": ""}
        claim  = {}
        claim_adjustments = []
        service_lines     = []

        def _flush_claim():
            if not claim:
                return
            base = {
                **isa, **gs,
                "transaction_type": "835",
                **payer, **payee, **check,
                **claim,
                "claim_adjustment_groups": _summarise_adjustments(claim_adjustments),
                "claim_adjustment_total":  sum(a["adjustment_amount"] or 0
                                               for a in claim_adjustments),
            }
            if service_lines:
                for sl in service_lines:
                    rows.append({**base, **sl})
            else:
                rows.append({**base,
                             "procedure_code": "", "modifier": "",
                             "revenue_code": "",
                             "line_charge": None, "line_payment": None,
                             "adjustment_group": "", "adjustment_reason": "",
                             "adjustment_amount": None,
                             "service_date": None, "service_date_end": None,
                             "allowed_amount": None, "units": None})

        def _summarise_adjustments(adjs):
            parts = []
            for a in adjs:
                parts.append(
                    f"{a['adjustment_group_code']}:{a['adjustment_reason_code']}"
                    f"={a['adjustment_amount']}"
                )
            return "|".join(parts)

        loop_ctx = None

        for seg in segments:
            sid = seg[0].strip()

            # ── Financial info (BPR) — check / EFT details ──────────────────
            if sid == "BPR":
                check["payment_method"] = e(seg, 4)
                check["check_amount"]   = safe_float(e(seg, 2))
                check["check_date"]     = safe_date(e(seg, 16))
                check["eft_trace"]      = e(seg, 9)

            # ── Trace number (TRN) — check number ───────────────────────────
            elif sid == "TRN":
                if e(seg, 1) == "1":
                    check["check_number"] = e(seg, 2)

            # ── Payer ────────────────────────────────────────────────────────
            elif sid == "N1" and e(seg, 1) == "PR":
                payer["payer_name"] = e(seg, 2)
                payer["payer_id"]   = e(seg, 4)
                loop_ctx = "1000A"

            # ── Payee ────────────────────────────────────────────────────────
            elif sid == "N1" and e(seg, 1) == "PE":
                payee["payee_name"]   = e(seg, 2)
                payee["payee_npi"]    = e(seg, 4) if e(seg, 3) == "XX" else ""
                payee["payee_tax_id"] = e(seg, 4) if e(seg, 3) == "FI" else ""
                loop_ctx = "1000B"

            elif sid == "REF" and loop_ctx == "1000B":
                if e(seg, 1) in ("TJ", "EI"):
                    payee["payee_tax_id"] = e(seg, 2)
                elif e(seg, 1) == "NF":
                    payee["payee_npi"] = payee["payee_npi"] or e(seg, 2)

            # ── Claim payment (CLP) — one per claim ──────────────────────────
            elif sid == "CLP":
                _flush_claim()
                service_lines     = []
                claim_adjustments = []
                status_code = e(seg, 2)
                claim = {
                    "claim_id":               e(seg, 1),
                    "patient_control_number": e(seg, 1),
                    "claim_status_code":      status_code,
                    "claim_status":           CLAIM_STATUS.get(status_code, status_code),
                    "claim_charge":           safe_float(e(seg, 3)),
                    "claim_payment":          safe_float(e(seg, 4)),
                    "patient_responsibility": safe_float(e(seg, 5)),
                    "claim_filing_indicator": e(seg, 6),
                    "payer_claim_control":    e(seg, 7),
                    "facility_type":          e(seg, 8),
                    "patient_last":           "",
                    "patient_first":          "",
                    "patient_id":             "",
                    "patient_dob":            None,
                    "patient_gender":         "",
                }
                loop_ctx = "2100"

            # ── Patient name in 2100 ─────────────────────────────────────────
            elif sid == "NM1" and loop_ctx in ("2100", "2110"):
                nm1_qual = e(seg, 1)
                if nm1_qual == "QC":  # patient
                    claim["patient_last"]  = e(seg, 3)
                    claim["patient_first"] = e(seg, 4)
                    claim["patient_id"]    = e(seg, 9)
                elif nm1_qual == "IL":  # insured
                    claim["patient_last"]  = claim["patient_last"]  or e(seg, 3)
                    claim["patient_first"] = claim["patient_first"] or e(seg, 4)

            elif sid == "DMG" and loop_ctx in ("2100", "2110"):
                claim["patient_dob"]    = safe_date(e(seg, 2))
                claim["patient_gender"] = e(seg, 3)

            # ── Claim-level adjustments (CAS on CLP) ────────────────────────
            elif sid == "CAS" and loop_ctx == "2100":
                grp_code = e(seg, 1)
                for i in range(2, len(seg) - 1, 3):
                    reason = e(seg, i)
                    amount = safe_float(e(seg, i + 1))
                    qty    = e(seg, i + 2)
                    if reason:
                        claim_adjustments.append({
                            "adjustment_group_code":  grp_code,
                            "adjustment_group":       ADJUSTMENT_GROUPS.get(grp_code, grp_code),
                            "adjustment_reason_code": reason,
                            "adjustment_amount":      amount,
                            "adjustment_quantity":    qty,
                        })

            # ── Service line (SVC) ───────────────────────────────────────────
            elif sid == "SVC":
                proc_field = e(seg, 1)
                parts = proc_field.split(":") if ":" in proc_field else ["", proc_field]
                procedure_code = parts[1] if len(parts) > 1 else parts[0]
                modifier = ":".join(parts[2:]) if len(parts) > 2 else ""
                revenue_code = parts[0] if parts[0] != "HC" else ""
                svc = {
                    "procedure_code": procedure_code,
                    "modifier":       modifier,
                    "revenue_code":   revenue_code,
                    "line_charge":    safe_float(e(seg, 2)),
                    "line_payment":   safe_float(e(seg, 3)),
                    "units":          safe_float(e(seg, 5)),
                    "allowed_amount": None,
                    "service_date":   None,
                    "service_date_end": None,
                    "adjustment_group":       "",
                    "adjustment_reason":      "",
                    "adjustment_amount":      None,
                    "_adjustments":           [],
                }
                service_lines.append(svc)
                loop_ctx = "2110"

            # ── Service line date ────────────────────────────────────────────
            elif sid == "DTP" and loop_ctx == "2110" and service_lines:
                if e(seg, 1) == "472":
                    val = e(seg, 3)
                    if "-" in val:
                        p = val.split("-")
                        service_lines[-1]["service_date"]     = safe_date(p[0])
                        service_lines[-1]["service_date_end"] = safe_date(p[1])
                    else:
                        service_lines[-1]["service_date"] = safe_date(val)

            # ── Service line adjustment (CAS on SVC) ─────────────────────────
            elif sid == "CAS" and loop_ctx == "2110" and service_lines:
                grp_code = e(seg, 1)
                for i in range(2, len(seg) - 1, 3):
                    reason = e(seg, i)
                    amount = safe_float(e(seg, i + 1))
                    if reason:
                        service_lines[-1]["_adjustments"].append({
                            "adjustment_group_code":  grp_code,
                            "adjustment_group":       ADJUSTMENT_GROUPS.get(grp_code, grp_code),
                            "adjustment_reason_code": reason,
                            "adjustment_amount":      amount,
                        })

            # ── Allowed amount (AMT) ─────────────────────────────────────────
            elif sid == "AMT" and loop_ctx == "2110" and service_lines:
                if e(seg, 1) == "B6":
                    service_lines[-1]["allowed_amount"] = safe_float(e(seg, 2))

        _flush_claim()

        # flatten per-line adjustments — take first adjustment for simplicity
        # (real pipelines often explode to one row per adjustment)
        for row in rows:
            adjs = row.pop("_adjustments", [])
            if adjs:
                row["adjustment_group"]  = adjs[0]["adjustment_group"]
                row["adjustment_reason"] = adjs[0]["adjustment_reason_code"]
                row["adjustment_amount"] = adjs[0]["adjustment_amount"]

        return rows
