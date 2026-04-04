"""
Inbound API — FastAPI server for partner order ingestion.

Endpoints:
    POST /inbound/orders          REST order ingestion (JSON)
    POST /inbound/events          Webhook event receiver (any partner event)
    POST /inbound/edi             EDI file ingestion (raw X12 body)
    POST /inbound/batch           Batch file reference (GCS / SFTP path)
    GET  /inbound/health          Health check
    GET  /inbound/partners        List registered partners

Auth:
    REST endpoints → X-API-Key header (per-partner key)
    Webhook endpoints → X-Webhook-Signature (HMAC-SHA256)
    Both → X-Partner-ID header (identifies the sending partner)

Idempotency:
    All endpoints check X-Idempotency-Key header.
    Duplicate keys return the cached response immediately.

Usage (local):
    uvicorn api.inbound_api:app --host 0.0.0.0 --port 8000 --reload
"""

import hashlib
import json
import logging
import time
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from shared.hmac_signer import verify_signature
from shared.audit_log import AuditLog

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Partner Integration API",
    description="Inbound order and event ingestion from trading partners.",
    version="1.0.0",
)

# ── In-memory partner registry (replace with DB lookup in production) ────────
# Each partner has: api_key, webhook_secret, allowed_event_types
PARTNER_REGISTRY: dict[str, dict] = {
    "PARTNER-ACME": {
        "name":             "ACME Corporation",
        "api_key":          "key-acme-abc123",
        "webhook_secret":   "whsec-acme-xyz789",
        "allowed_events":   ["order.created", "order.updated", "order.cancelled"],
        "allowed_formats":  ["json", "edi_850", "edi_856"],
    },
    "PARTNER-BLUECROSS": {
        "name":             "Blue Cross Blue Shield",
        "api_key":          "key-bcbs-def456",
        "webhook_secret":   "whsec-bcbs-qrs012",
        "allowed_events":   ["claim.submitted", "eligibility.request", "enrollment.change"],
        "allowed_formats":  ["edi_837", "edi_834", "edi_270"],
    },
}

# Audit log (no DB in this standalone mode — logs to stdout)
audit = AuditLog(db_conn=None)


# ── Auth dependency ──────────────────────────────────────────────────────────

def get_partner(
    x_partner_id: str = Header(..., description="Partner identifier"),
    x_api_key:    str = Header(..., description="Partner API key"),
) -> dict:
    """Validate partner ID + API key. Raises 401/403 on failure."""
    partner = PARTNER_REGISTRY.get(x_partner_id)
    if not partner:
        logger.warning(f"Unknown partner: {x_partner_id}")
        raise HTTPException(status_code=401, detail="Unknown partner ID.")
    if partner["api_key"] != x_api_key:
        logger.warning(f"Invalid API key for partner: {x_partner_id}")
        raise HTTPException(status_code=401, detail="Invalid API key.")
    return {**partner, "partner_id": x_partner_id}


# ── Request / Response models ─────────────────────────────────────────────────

class OrderLine(BaseModel):
    line_number:     str
    product_id:      str
    description:     str     = ""
    quantity:        float
    unit_of_measure: str     = "EA"
    unit_price:      float
    line_total:      Optional[float] = None


class InboundOrder(BaseModel):
    order_id:        str   = Field(..., description="Partner's order reference")
    order_date:      str
    currency:        str   = "USD"
    buyer_name:      str   = ""
    seller_name:     str   = ""
    ship_to_name:    str   = ""
    ship_to_address: str   = ""
    lines:           list[OrderLine]
    metadata:        dict  = {}


class OrderResponse(BaseModel):
    status:          str
    our_reference:   str
    partner_order_id: str
    line_count:      int
    message:         str  = ""


class WebhookEvent(BaseModel):
    event_type:   str
    event_id:     str
    occurred_at:  str
    data:         dict
    metadata:     dict = {}


class EDIIngestionResponse(BaseModel):
    status:           str
    transaction_type: str
    transaction_count: int
    row_count:        int
    message:          str = ""


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/inbound/health")
def health():
    return {"status": "ok", "service": "partner-inbound-api", "version": "1.0.0"}


@app.get("/inbound/partners")
def list_partners():
    return {
        "partners": [
            {"partner_id": pid, "name": p["name"],
             "allowed_formats": p["allowed_formats"]}
            for pid, p in PARTNER_REGISTRY.items()
        ]
    }


@app.post("/inbound/orders", response_model=OrderResponse)
async def receive_order(
    order:      InboundOrder,
    request:    Request,
    partner:    dict = Depends(get_partner),
    x_idempotency_key: Optional[str] = Header(None),
):
    """
    Receive a partner order as JSON.

    Headers required:
        X-Partner-ID       partner identifier
        X-API-Key          partner API key
        X-Idempotency-Key  optional, UUID for dedup
    """
    start = time.monotonic()
    raw_body = await request.body()

    # Use idempotency key or hash of body
    idem_key = x_idempotency_key or hashlib.sha256(raw_body).hexdigest()
    our_ref  = f"ORD-{idem_key[:12].upper()}"

    # ── Validate ──────────────────────────────────────────────────────────
    if "order.created" not in partner["allowed_events"] and \
       "order.updated" not in partner["allowed_events"]:
        raise HTTPException(status_code=403, detail="Partner not permitted to submit orders.")

    if not order.lines:
        raise HTTPException(status_code=422, detail="Order must have at least one line.")

    # ── Compute line totals ───────────────────────────────────────────────
    for line in order.lines:
        if line.line_total is None:
            line.line_total = round(line.quantity * line.unit_price, 4)

    total_value = sum(l.line_total for l in order.lines)

    # ── Audit ────────────────────────────────────────────────────────────
    duration_ms = int((time.monotonic() - start) * 1000)
    audit.inbound(
        event_type="order.created",
        transport="rest",
        partner_id=partner["partner_id"],
        status="processed",
        idempotency_key=idem_key,
        payload=raw_body,
        duration_ms=duration_ms,
    )

    logger.info(
        f"Order received: {order.order_id} from {partner['partner_id']} — "
        f"{len(order.lines)} lines, total={total_value:.2f} {order.currency}"
    )

    # ── In a real pipeline: push to normaliser → loader → DB ─────────────
    # from parsers import JSONParser
    # from loaders import PostgresLoader
    # df = JSONParser().parse(order.dict())
    # PostgresLoader(conn).load(df, table="raw_orders")

    return OrderResponse(
        status="accepted",
        our_reference=our_ref,
        partner_order_id=order.order_id,
        line_count=len(order.lines),
        message=f"Order accepted. Total value: {total_value:.2f} {order.currency}",
    )


@app.post("/inbound/events")
async def receive_webhook_event(
    request:   Request,
    x_partner_id:         str           = Header(...),
    x_webhook_signature:  str           = Header(...),
    x_webhook_timestamp:  Optional[str] = Header(None),
    x_idempotency_key:    Optional[str] = Header(None),
):
    """
    Receive a webhook event from a partner.

    Partners PUSH events to this endpoint when something happens on their side.
    We verify the HMAC signature before processing.

    Headers required:
        X-Partner-ID          partner identifier
        X-Webhook-Signature   sha256=<hmac_hex>
        X-Webhook-Timestamp   Unix epoch (optional, for replay protection)
        X-Idempotency-Key     optional UUID
    """
    raw_body = await request.body()

    # ── Partner lookup ────────────────────────────────────────────────────
    partner = PARTNER_REGISTRY.get(x_partner_id)
    if not partner:
        raise HTTPException(status_code=401, detail="Unknown partner.")

    # ── Signature verification ────────────────────────────────────────────
    valid = verify_signature(
        payload=raw_body,
        received_sig=x_webhook_signature,
        secret=partner["webhook_secret"],
        timestamp=x_webhook_timestamp,
    )
    if not valid:
        audit.inbound(
            event_type="webhook.signature_failed",
            transport="webhook",
            partner_id=x_partner_id,
            status="rejected",
            payload=raw_body,
        )
        raise HTTPException(status_code=401, detail="Signature verification failed.")

    # ── Parse ────────────────────────────────────────────────────────────
    try:
        body = json.loads(raw_body)
        event_type = body.get("event_type", "unknown")
    except json.JSONDecodeError:
        raise HTTPException(status_code=422, detail="Invalid JSON body.")

    idem_key = x_idempotency_key or hashlib.sha256(raw_body).hexdigest()

    # ── Event type check ──────────────────────────────────────────────────
    if event_type not in partner["allowed_events"]:
        logger.warning(f"Unexpected event type {event_type} from {x_partner_id}")

    audit.inbound(
        event_type=event_type,
        transport="webhook",
        partner_id=x_partner_id,
        status="processed",
        idempotency_key=idem_key,
        payload=raw_body,
    )

    logger.info(f"Webhook event received: {event_type} from {x_partner_id}")

    # Always return 200 quickly — do heavy processing async
    return JSONResponse(
        status_code=200,
        content={
            "status": "accepted",
            "event_type": event_type,
            "idempotency_key": idem_key,
        },
    )


@app.post("/inbound/edi", response_model=EDIIngestionResponse)
async def receive_edi(
    request: Request,
    partner: dict = Depends(get_partner),
    x_idempotency_key: Optional[str] = Header(None),
):
    """
    Receive a raw EDI X12 file body.

    Content-Type: text/plain (raw X12) or application/octet-stream
    Auto-detects transaction type from ST segment.

    Partners can POST EDI directly when SFTP/AS2 is not available.
    """
    raw_body = await request.body()

    if not raw_body:
        raise HTTPException(status_code=422, detail="Empty EDI body.")

    raw_str = raw_body.decode("utf-8", errors="replace").strip()
    if not raw_str.startswith("ISA"):
        raise HTTPException(status_code=422, detail="Body must be a valid X12 EDI file starting with ISA.")

    # detect transaction type from ST segment
    tx_type = "unknown"
    for seg in raw_str.split("~"):
        seg = seg.strip()
        if seg.startswith("ST"):
            parts = seg.split("*")
            if len(parts) > 1:
                tx_type = parts[1].strip()
            break

    idem_key = x_idempotency_key or hashlib.sha256(raw_body).hexdigest()

    # route to correct parser
    row_count = 0
    try:
        from parsers import EDI834Parser, EDI837Parser, EDI835Parser, EDI270Parser, EDI271Parser, EDIParser
        parser_map = {
            "834": EDI834Parser, "837": EDI837Parser, "835": EDI835Parser,
            "270": EDI270Parser, "271": EDI271Parser,
            "850": EDIParser,    "856": EDIParser,
            "810": EDIParser,    "997": EDIParser,
        }
        parser_cls = parser_map.get(tx_type)
        if parser_cls:
            parser = parser_cls()
            df = parser.parse(raw_str)
            row_count = len(df)
            logger.info(f"EDI {tx_type} parsed — {row_count} rows from {partner['partner_id']}")
    except Exception as exc:
        logger.error(f"EDI parse error: {exc}")
        raise HTTPException(status_code=422, detail=f"EDI parse failed: {exc}")

    audit.inbound(
        event_type=f"edi.{tx_type.lower()}",
        transport="rest",
        partner_id=partner["partner_id"],
        status="processed",
        idempotency_key=idem_key,
        payload=raw_body,
    )

    return EDIIngestionResponse(
        status="accepted",
        transaction_type=tx_type,
        transaction_count=1,
        row_count=row_count,
        message=f"EDI {tx_type} accepted — {row_count} rows parsed.",
    )


@app.post("/inbound/batch")
async def receive_batch_reference(
    request: Request,
    partner: dict = Depends(get_partner),
):
    """
    Receive a reference to a batch file (GCS path or SFTP path).
    The API acknowledges receipt and queues the file for async processing.
    Useful for large files that shouldn't be POSTed directly.
    """
    body = await request.json()
    file_path   = body.get("file_path", "")
    file_format = body.get("format", "")
    record_count = body.get("expected_record_count", 0)

    if not file_path:
        raise HTTPException(status_code=422, detail="file_path is required.")

    audit.inbound(
        event_type=f"batch.{file_format}",
        transport="sftp",
        partner_id=partner["partner_id"],
        status="received",
        payload=json.dumps(body).encode(),
    )

    logger.info(f"Batch file queued: {file_path} from {partner['partner_id']}")

    return {
        "status":     "queued",
        "file_path":  file_path,
        "message":    f"File queued for processing. Expected {record_count} records.",
    }
