"""
EDI 837 — Healthcare Claim (ANSI X12 5010)

Three flavours — same parser handles all:
    837P  Professional   (physician visits, outpatient, labs)  X222A2
    837I  Institutional  (hospital, inpatient, SNF)            X223A2
    837D  Dental                                               X224A3

Direction:  Provider → Payer (insurance company / Medicare / Medicaid)
Purpose:    Bill for services rendered. The most financially significant
            EDI transaction — every claim rejection costs providers money.

Key loop structure:
    Loop 1000A  Submitter
    Loop 1000B  Receiver
    Loop 2000A  Billing provider
      Loop 2000B  Subscriber (patient or policyholder)
        Loop 2000C  Patient (if different from subscriber)
          Loop 2300  Claim information
            Loop 2310  Service facility / rendering provider
            Loop 2400  Service line (one per procedure)
              Loop 2430  Adjudication info (COB)

Output: one row per service line (CLM + SV1/SV2).
Key columns: claim_id, patient_id, patient_name, dob, diagnosis_codes,
             procedure_code, modifier, revenue_code, units, charge_amount,
             place_of_service, provider_npi, provider_name,
             subscriber_id, payer_name, claim_filing_indicator,
             service_date, admit_date, discharge_date
"""

import logging
from typing import Any

import pandas as pd

from .base_parser import BaseParser, ParserError
from .edi_utils import EDIEnvelopeParser, safe_float, safe_date, e

logger = logging.getLogger(__name__)

CLAIM_FILING = {
    "MB": "medicare_part_b", "MC": "medicaid", "CH": "champus_tricare",
    "BL": "blue_cross_blue_shield", "CI": "commercial_insurance",
    "HM": "hmo", "PP": "preferred_provider", "WC": "workers_comp",
    "AM": "automobile_medical", "VA": "veterans_affairs", "OF": "other_federal",
    "ZZ": "other",
}

PLACE_OF_SERVICE = {
    "11": "office", "12": "home", "21": "inpatient_hospital",
    "22": "outpatient_hospital", "23": "emergency_room", "24": "ambulatory_surgical",
    "31": "skilled_nursing", "32": "nursing_facility", "33": "custodial_care",
    "34": "hospice", "41": "ambulance_land", "42": "ambulance_air_water",
    "49": "independent_clinic", "50": "federally_qualified_health_center",
    "51": "inpatient_psychiatric", "52": "psychiatric_facility_partial",
    "53": "community_mental_health", "65": "end_stage_renal",
    "71": "state_local_public_health", "72": "rural_health_clinic",
    "81": "independent_laboratory", "99": "other",
}


class EDI837Parser(BaseParser):
    """
    Parses EDI 837P, 837I, and 837D claim files.
    Auto-detects claim type from GS functional identifier:
        HC = 837P or 837I or 837D
    Distinguishes P/I/D from CLM05-2 (facility type code).
    """

    @property
    def format_name(self) -> str:
        return "EDI X12 5010 837 Healthcare Claim"

    def parse(self, source: Any) -> pd.DataFrame:
        env = EDIEnvelopeParser(source)
        transactions = env.extract_transactions(["837"])
        if not transactions:
            raise ParserError("No 837 transactions found.")

        all_rows = []
        for tx in transactions:
            rows = self._parse_837(tx["segments"], tx["isa"], tx["gs"])
            all_rows.extend(rows)

        if not all_rows:
            raise ParserError("837 parsed but produced no claim rows.")

        df = pd.DataFrame(all_rows)
        logger.info(f"837 parse complete — {len(df)} service lines, "
                    f"{df['claim_id'].nunique()} unique claims")
        return df

    def _parse_837(self, segments, isa, gs):
        rows = []

        submitter  = {"submitter_name": "", "submitter_id": ""}
        receiver   = {"receiver_name":  "", "receiver_id":  ""}
        billing_prov = {"billing_provider_npi": "", "billing_provider_name": "",
                        "billing_provider_tax_id": ""}
        subscriber = {}
        patient    = {}
        claim      = {}
        service_lines = []
        diagnoses  = []

        # context flags
        loop_ctx = None  # "1000A","1000B","2000A","2000B","2000C","2300","2400"
        nm1_ctx  = None

        def _flush_claim():
            if not claim or not claim.get("claim_id"):
                return
            base = {
                **isa, **gs,
                "transaction_type": "837",
                **submitter, **receiver, **billing_prov,
                **subscriber, **patient,
                **{k: v for k, v in claim.items()},
                "diagnosis_codes": "|".join(diagnoses),
            }
            if service_lines:
                for sl in service_lines:
                    rows.append({**base, **sl})
            else:
                rows.append({**base,
                             "procedure_code": "", "modifier": "",
                             "revenue_code": "", "units": None,
                             "charge_amount": None, "service_date": None,
                             "service_date_end": None})

        for seg in segments:
            sid = seg[0].strip()

            # ── Loop context tracking via NM1 qualifier ─────────────────────
            if sid == "NM1":
                nm1_qual = e(seg, 1)
                nm1_ctx  = nm1_qual

                if nm1_qual == "41":   # submitter
                    submitter["submitter_name"] = e(seg, 3)
                    submitter["submitter_id"]   = e(seg, 9)
                    loop_ctx = "1000A"

                elif nm1_qual == "40":  # receiver
                    receiver["receiver_name"] = e(seg, 3)
                    receiver["receiver_id"]   = e(seg, 9)
                    loop_ctx = "1000B"

                elif nm1_qual == "85":  # billing provider
                    billing_prov["billing_provider_name"] = (
                        e(seg, 3) if e(seg, 2) == "2"
                        else f"{e(seg, 3)}, {e(seg, 4)}"
                    )
                    billing_prov["billing_provider_npi"]  = e(seg, 9)
                    loop_ctx = "2000A"

                elif nm1_qual == "IL":  # subscriber — new subscriber loop, flush pending claim
                    _flush_claim()
                    claim = {}
                    service_lines = []
                    diagnoses = []
                    subscriber = {
                        "subscriber_last":  e(seg, 3),
                        "subscriber_first": e(seg, 4),
                        "subscriber_id":    e(seg, 9),
                        "subscriber_id_qualifier": e(seg, 8),
                    }
                    # default patient to subscriber — overridden by QC if present
                    patient = {
                        "patient_last":  e(seg, 3),
                        "patient_first": e(seg, 4),
                        "patient_id":    e(seg, 9),
                        "patient_dob":   None,
                        "patient_gender": "",
                        "patient_ssn":   "",
                    }
                    loop_ctx = "2000B"

                elif nm1_qual == "QC":  # patient (different from subscriber)
                    patient = {
                        "patient_last":  e(seg, 3),
                        "patient_first": e(seg, 4),
                        "patient_id":    e(seg, 9),
                        "patient_dob":   None,
                        "patient_gender": "",
                    }
                    loop_ctx = "2000C"

                elif nm1_qual == "77":  # service facility
                    claim["service_facility_name"] = e(seg, 3)
                    claim["service_facility_npi"]  = e(seg, 9)

                elif nm1_qual == "82":  # rendering provider
                    claim["rendering_provider_npi"]  = e(seg, 9)
                    claim["rendering_provider_name"] = (
                        f"{e(seg, 3)}, {e(seg, 4)}" if e(seg, 4)
                        else e(seg, 3)
                    )
                elif nm1_qual == "PR":  # payer
                    claim["payer_name"] = e(seg, 3)
                    claim["payer_id"]   = e(seg, 9)

            # ── REF segments for IDs ─────────────────────────────────────────
            elif sid == "REF":
                ref_qual = e(seg, 1)
                ref_val  = e(seg, 2)
                if ref_qual == "EI":
                    billing_prov["billing_provider_tax_id"] = ref_val
                elif ref_qual == "SY" and loop_ctx in ("2000B", "2000C"):
                    patient["patient_ssn"] = ref_val
                elif ref_qual == "1W" and loop_ctx == "2000B":
                    subscriber["subscriber_member_id"] = ref_val

            # ── Demographics ─────────────────────────────────────────────────
            elif sid == "DMG":
                dob = safe_date(e(seg, 2))
                gender = e(seg, 3)
                if loop_ctx == "2000B":
                    subscriber["subscriber_dob"]    = dob
                    subscriber["subscriber_gender"] = gender
                    patient["patient_dob"]    = dob
                    patient["patient_gender"] = gender
                elif loop_ctx == "2000C":
                    patient["patient_dob"]    = dob
                    patient["patient_gender"] = gender

            # ── Claim header (CLM) ───────────────────────────────────────────
            elif sid == "CLM":
                _flush_claim()
                service_lines = []
                diagnoses     = []
                pos_code = e(seg, 5).split(":")[0] if ":" in e(seg, 5) else e(seg, 5)
                claim = {
                    "claim_id":              e(seg, 1),
                    "charge_total":          safe_float(e(seg, 2)),
                    "place_of_service_code": pos_code,
                    "place_of_service":      PLACE_OF_SERVICE.get(pos_code, pos_code),
                    "claim_frequency":       e(seg, 5).split(":")[2] if e(seg, 5).count(":") >= 2 else "",
                    "provider_signature":    e(seg, 6),
                    "assignment_of_benefits": e(seg, 7),
                    "release_of_info":       e(seg, 8),
                    "claim_filing_indicator": CLAIM_FILING.get(e(seg, 9), e(seg, 9)),
                    "admit_date":            None,
                    "discharge_date":        None,
                    "patient_control_number": e(seg, 1),
                    "payer_name":            claim.get("payer_name", ""),
                    "payer_id":              claim.get("payer_id",   ""),
                    "service_facility_name": claim.get("service_facility_name", ""),
                    "service_facility_npi":  claim.get("service_facility_npi",  ""),
                    "rendering_provider_npi":  claim.get("rendering_provider_npi",  ""),
                    "rendering_provider_name": claim.get("rendering_provider_name", ""),
                }
                loop_ctx = "2300"

            # ── Diagnosis codes (HI) ─────────────────────────────────────────
            elif sid == "HI" and loop_ctx in ("2300", "2400"):
                for i in range(1, len(seg)):
                    pair = e(seg, i)
                    if ":" in pair:
                        code = pair.split(":")[1]
                        if code:
                            diagnoses.append(code)
                    elif pair:
                        diagnoses.append(pair)

            # ── Dates (DTP) ──────────────────────────────────────────────────
            elif sid == "DTP" and loop_ctx in ("2300", "2400"):
                dtp_qual = e(seg, 1)
                dtp_val  = e(seg, 3)
                if dtp_qual == "435":
                    claim["admit_date"]     = safe_date(dtp_val)
                elif dtp_qual == "096":
                    claim["discharge_date"] = safe_date(dtp_val)

            # ── Service line (SV1 = professional, SV2 = institutional) ──────
            elif sid in ("SV1", "SV2") and loop_ctx in ("2300", "2400"):
                loop_ctx = "2400"
                if sid == "SV1":
                    # SV1*HC:99213:25**50*UN*1***1:2~
                    proc_field = e(seg, 1)
                    parts = proc_field.split(":") if ":" in proc_field else ["", proc_field]
                    procedure_code = parts[1] if len(parts) > 1 else parts[0]
                    modifiers = ":".join(parts[2:]) if len(parts) > 2 else ""
                    sl = {
                        "procedure_code": procedure_code,
                        "modifier":       modifiers,
                        "revenue_code":   "",
                        "charge_amount":  safe_float(e(seg, 2)),
                        "unit_of_measure": e(seg, 3),
                        "units":          safe_float(e(seg, 4)),
                        "diagnosis_pointer": e(seg, 7),
                    }
                else:  # SV2 — institutional
                    sl = {
                        "revenue_code":   e(seg, 1),
                        "procedure_code": e(seg, 2).split(":")[1] if ":" in e(seg, 2) else e(seg, 2),
                        "modifier":       "",
                        "charge_amount":  safe_float(e(seg, 3)),
                        "unit_of_measure": e(seg, 4),
                        "units":          safe_float(e(seg, 5)),
                        "diagnosis_pointer": "",
                    }
                service_lines.append({**sl, "service_date": None, "service_date_end": None})

            # ── Service line date ────────────────────────────────────────────
            elif sid == "DTP" and loop_ctx == "2400":
                if e(seg, 1) == "472":
                    date_val = e(seg, 3)
                    if service_lines:
                        if "-" in date_val:
                            parts = date_val.split("-")
                            service_lines[-1]["service_date"]     = safe_date(parts[0])
                            service_lines[-1]["service_date_end"] = safe_date(parts[1])
                        else:
                            service_lines[-1]["service_date"] = safe_date(date_val)

            # ── Line-level procedure (LX marks new service line segment) ────
            elif sid == "LX":
                loop_ctx = "2400"

        _flush_claim()
        return rows
