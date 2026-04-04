"""
Integration tests — inbound API, outbound, EDI round-trip.
Run: python tests/test_integrations.py
"""
import sys, os, json, time, hashlib
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date

DIVIDER = "=" * 60
passed_count = failed_count = 0

def header(name):   print(f"\n{DIVIDER}\n{name}\n{DIVIDER}")
def ok(msg):
    global passed_count
    passed_count += 1
    print(f"  PASS  {msg}")
def fail(msg, err):
    global failed_count
    failed_count += 1
    print(f"  FAIL  {msg}: {err}")


# ── HMAC signing ─────────────────────────────────────────────────────────────
def test_hmac():
    header("HMAC signing + verification")
    from shared.hmac_signer import sign_payload, verify_signature, generate_secret

    secret  = generate_secret()
    payload = b'{"event_type":"order.created","order_id":"ORD-001"}'
    sig     = sign_payload(payload, secret)

    assert sig.startswith("sha256="),           "wrong prefix"
    assert verify_signature(payload, sig, secret), "valid sig rejected"
    assert not verify_signature(payload, sig, "wrong-secret"), "wrong secret accepted"
    assert not verify_signature(b"tampered", sig, secret),     "tampered payload accepted"

    # replay attack — old timestamp
    old_ts = str(int(time.time()) - 600)
    assert not verify_signature(payload, sig, secret,
                                tolerance_seconds=300,
                                timestamp=old_ts), "stale timestamp accepted"
    ok("HMAC sign / verify / replay protection all correct")


# ── Idempotency (in-memory mock) ─────────────────────────────────────────────
def test_idempotency_key_generation():
    header("Idempotency key from payload")
    from shared.idempotency import key_from_payload

    payload = b'{"order_id":"ORD-001"}'
    key1    = key_from_payload(payload)
    key2    = key_from_payload(payload)
    key3    = key_from_payload(b'{"order_id":"ORD-002"}')

    assert key1 == key2,  "same payload → different key"
    assert key1 != key3,  "different payload → same key"
    assert len(key1) == 64, "expected SHA-256 hex digest (64 chars)"
    ok(f"Deterministic key generation correct: {key1[:16]}…")


# ── Audit log (stdout-only mode, no DB) ────────────────────────────────────
def test_audit_log():
    header("Audit log — stdout mode")
    from shared.audit_log import AuditLog

    audit = AuditLog(db_conn=None)
    eid   = audit.inbound(
        event_type="order.created",
        transport="rest",
        partner_id="PARTNER-ACME",
        status="processed",
        payload=b'{"order_id":"ORD-001"}',
        duration_ms=42,
    )
    assert eid and len(eid) == 36, f"expected UUID, got: {eid}"

    eid2 = audit.outbound(
        event_type="order.confirmed",
        transport="webhook",
        partner_id="PARTNER-ACME",
        status="sent",
        http_status_code=200,
        target_url="https://acme.example.com/webhooks",
    )
    assert eid2 and eid != eid2, "duplicate event IDs"
    ok(f"Audit log generated IDs: {eid[:8]}… / {eid2[:8]}…")


# ── Inbound API ──────────────────────────────────────────────────────────────
def test_inbound_api():
    header("Inbound API — FastAPI TestClient")
    try:
        from fastapi.testclient import TestClient
        from api.inbound_api import app

        client = TestClient(app, raise_server_exceptions=True)

        # health check
        r = client.get("/inbound/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"
        ok("GET /inbound/health → 200")

        # partner list
        r = client.get("/inbound/partners")
        assert r.status_code == 200
        assert len(r.json()["partners"]) >= 2
        ok(f"GET /inbound/partners → {len(r.json()['partners'])} partners")

        # REST order ingestion — happy path
        order_payload = {
            "order_id":    "ORD-TEST-001",
            "order_date":  "2024-01-15",
            "buyer_name":  "ACME CORP",
            "seller_name": "OUR COMPANY",
            "lines": [
                {"line_number": "1", "product_id": "SKU-A",
                 "description": "Widget A", "quantity": 10,
                 "unit_of_measure": "EA", "unit_price": 9.99},
                {"line_number": "2", "product_id": "SKU-B",
                 "description": "Widget B", "quantity": 5,
                 "unit_of_measure": "EA", "unit_price": 19.99},
            ]
        }
        r = client.post(
            "/inbound/orders",
            json=order_payload,
            headers={
                "X-Partner-ID":       "PARTNER-ACME",
                "X-API-Key":          "key-acme-abc123",
                "X-Idempotency-Key":  "idem-test-001",
            }
        )
        assert r.status_code == 200, f"Got {r.status_code}: {r.text}"
        body = r.json()
        assert body["status"] == "accepted"
        assert body["line_count"] == 2
        assert "1444" not in body["message"]  # not old test bleed-through
        ok(f"POST /inbound/orders → accepted, ref={body['our_reference']}")

        # Wrong API key → 401
        r = client.post(
            "/inbound/orders",
            json=order_payload,
            headers={"X-Partner-ID": "PARTNER-ACME", "X-API-Key": "wrong-key"},
        )
        assert r.status_code == 401
        ok("Wrong API key → 401 Unauthorized")

        # Unknown partner → 401
        r = client.post(
            "/inbound/orders",
            json=order_payload,
            headers={"X-Partner-ID": "PARTNER-UNKNOWN", "X-API-Key": "any"},
        )
        assert r.status_code == 401
        ok("Unknown partner → 401 Unauthorized")

        # Webhook receiver — valid HMAC
        from shared.hmac_signer import sign_payload
        event_body = json.dumps({
            "event_type": "order.created",
            "event_id":   "evt-001",
            "occurred_at": "2024-01-15T10:00:00Z",
            "data":       {"order_id": "ORD-999"},
        }).encode()
        ts  = str(int(time.time()))
        sig = sign_payload(event_body, "whsec-acme-xyz789")

        r = client.post(
            "/inbound/events",
            content=event_body,
            headers={
                "X-Partner-ID":          "PARTNER-ACME",
                "X-Webhook-Signature":   sig,
                "X-Webhook-Timestamp":   ts,
                "Content-Type":          "application/json",
            }
        )
        assert r.status_code == 200, f"Got {r.status_code}: {r.text}"
        assert r.json()["status"] == "accepted"
        ok("POST /inbound/events with valid HMAC → 200 accepted")

        # Webhook receiver — invalid HMAC → 401
        r = client.post(
            "/inbound/events",
            content=event_body,
            headers={
                "X-Partner-ID":        "PARTNER-ACME",
                "X-Webhook-Signature": "sha256=badhash",
                "X-Webhook-Timestamp": ts,
                "Content-Type":        "application/json",
            }
        )
        assert r.status_code == 401
        ok("POST /inbound/events with bad HMAC → 401 rejected")

        # EDI inbound — 834
        edi_body = open("sample_data/healthcare/sample_834.edi", "rb").read()
        r = client.post(
            "/inbound/edi",
            content=edi_body,
            headers={
                "X-Partner-ID": "PARTNER-BLUECROSS",
                "X-API-Key":    "key-bcbs-def456",
                "Content-Type": "text/plain",
            }
        )
        assert r.status_code == 200, f"Got {r.status_code}: {r.text}"
        body = r.json()
        assert body["status"] == "accepted"
        assert body["transaction_type"] == "834"
        assert body["row_count"] > 0
        ok(f"POST /inbound/edi (834) → {body['row_count']} rows parsed")

    except ImportError as e:
        print(f"  SKIP  FastAPI not installed ({e}) — install with: pip install fastapi httpx")
        return


# ── Outbound webhook dispatcher (mock mode) ──────────────────────────────────
def test_outbound_webhook_mock():
    header("Outbound webhook dispatcher — mock partner")
    import unittest.mock as mock

    from outbound.webhook_dispatcher import WebhookDispatcher

    dispatcher = WebhookDispatcher(max_attempts=2)

    # Mock successful delivery
    mock_resp = mock.MagicMock()
    mock_resp.status_code = 200

    with mock.patch("httpx.Client") as mock_client:
        mock_client.return_value.__enter__.return_value.post.return_value = mock_resp
        result = dispatcher.send(
            partner_id="PARTNER-ACME",
            event_type="order.confirmed",
            payload={"order_id": "ORD-001", "status": "confirmed"},
            idempotency_key="idem-webhook-001",
        )

    assert result.success,              f"Expected success, got: {result.error}"
    assert result.http_status == 200,   f"Expected 200, got: {result.http_status}"
    assert result.partner_id == "PARTNER-ACME"
    ok(f"Webhook delivered successfully: delivery_id={result.delivery_id[:8]}…")

    # Mock failed delivery (500) → exhausts retries
    mock_resp_500 = mock.MagicMock()
    mock_resp_500.status_code = 500
    mock_resp_500.text = "Internal Server Error"

    with mock.patch("httpx.Client") as mock_client, \
         mock.patch("time.sleep"):  # don't actually wait
        mock_client.return_value.__enter__.return_value.post.return_value = mock_resp_500
        result = dispatcher.send(
            partner_id="PARTNER-ACME",
            event_type="order.confirmed",
            payload={"order_id": "ORD-002"},
            idempotency_key="idem-webhook-002",
        )

    assert not result.success,   "Expected failure on 500s"
    assert result.attempt == 2,  f"Expected 2 attempts, got {result.attempt}"
    ok(f"500 responses → exhausted {result.attempt} attempts, result=failed ✓")

    # Unknown partner
    result = dispatcher.send(
        partner_id="PARTNER-UNKNOWN",
        event_type="test.event",
        payload={},
    )
    assert not result.success
    assert "No webhook config" in result.error
    ok("Unknown partner → graceful failure, no exception")


# ── EDI generator round-trip ──────────────────────────────────────────────────
def test_edi_generator_roundtrip():
    header("EDI generator — generate → parse round-trip")
    from outbound.edi_generator import EDIGenerator
    from parsers import EDIParser, EDI834Parser

    gen = EDIGenerator(sender_id="OURCO001", receiver_id="PARTNERX")

    # 850 round-trip
    order = {
        "po_number":      "PO-ROUNDTRIP-001",
        "po_date":        date(2024, 2, 1),
        "buyer_name":     "OUR COMPANY",
        "buyer_id":       "OURCO001",
        "seller_name":    "SUPPLIER INC",
        "seller_id":      "SUPP001",
        "ship_to_name":   "OUR WAREHOUSE",
        "ship_to_address": "999 WAREHOUSE RD",
        "ship_to_city":   "DALLAS",
        "ship_to_state":  "TX",
        "ship_to_zip":    "75201",
        "lines": [
            {"line_number": "1", "product_id": "ITEM-A",
             "description": "Round trip item A",
             "quantity": 25, "unit_of_measure": "EA", "unit_price": 12.50},
            {"line_number": "2", "product_id": "ITEM-B",
             "description": "Round trip item B",
             "quantity": 10, "unit_of_measure": "CS", "unit_price": 75.00},
        ]
    }
    edi_850 = gen.generate_850(order)
    assert edi_850.startswith("ISA"),       "generated EDI must start with ISA"
    assert "ST*850" in edi_850,             "missing ST*850 segment"
    assert "PO-ROUNDTRIP-001" in edi_850,   "missing PO number"
    assert "ITEM-A" in edi_850,             "missing product ID"
    ok("850 generated — ISA/GS/ST/PO1/SE/GE/IEA all present")

    df = EDIParser().parse(edi_850)
    assert not df.empty,                         "parse returned empty"
    assert df["po_number"].iloc[0] == "PO-ROUNDTRIP-001", \
        f"PO mismatch: {df['po_number'].iloc[0]}"
    assert len(df) == 2,                         f"expected 2 lines, got {len(df)}"
    assert df["quantity"].iloc[0] == 25.0,       "quantity mismatch"
    assert df["unit_price"].iloc[0] == 12.50,    "price mismatch"
    ok(f"850 round-trip: generate → parse → {len(df)} lines verified ✓")

    # 856 round-trip
    shipment = {
        "shipment_id":   "SHIP-ROUNDTRIP-001",
        "ship_date":     date(2024, 2, 2),
        "po_number":     "PO-ROUNDTRIP-001",
        "carrier_code":  "FEDEX",
        "tracking_number": "9876543210",
        "ship_to_name":  "OUR WAREHOUSE",
        "ship_to_id":    "WH001",
        "items": [
            {"item_id": "ITEM-A", "quantity": 25, "unit_of_measure": "EA"},
            {"item_id": "ITEM-B", "quantity": 10, "unit_of_measure": "CS"},
        ]
    }
    edi_856 = gen.generate_856(shipment)
    assert "ST*856" in edi_856,                  "missing ST*856"
    assert "SHIP-ROUNDTRIP-001" in edi_856,      "missing shipment ID"
    assert "9876543210" in edi_856,              "missing tracking number"

    df = EDIParser().parse(edi_856)
    assert not df.empty
    items = df[df["hl_level"] == "item"]
    assert len(items) == 2,                      f"expected 2 items, got {len(items)}"
    ok(f"856 round-trip: generate → parse → {len(items)} items verified ✓")

    # 834 round-trip
    from datetime import date as d
    enrollment = {
        "reference_id":  "ENROLL-2024-001",
        "employer_name": "TEST EMPLOYER INC",
        "employer_id":   "EMP001",
        "payer_name":    "TEST HEALTH PLAN",
        "payer_id":      "PLAN001",
        "members": [
            {
                "member_id": "MBR-001", "subscriber_id": "SUB-001",
                "first_name": "ALICE", "last_name": "WONDER",
                "dob": d(1990, 5, 20), "gender": "F",
                "relationship_code": "18",
                "maintenance_type_code": "021",
                "plan_id": "GOLD-HMO",
                "coverage_type_code": "HLT",
                "effective_date": d(2024, 1, 1),
            }
        ]
    }
    edi_834 = gen.generate_834(enrollment)
    assert "ST*834" in edi_834,                  "missing ST*834"
    assert "ALICE" in edi_834,                    "missing member name"
    assert "GOLD-HMO" in edi_834,                "missing plan ID"

    df = EDI834Parser().parse(edi_834)
    assert not df.empty
    assert df["first_name"].iloc[0] == "ALICE",  f"name mismatch: {df['first_name'].iloc[0]}"
    assert df["plan_id"].iloc[0] == "GOLD-HMO",  f"plan mismatch: {df['plan_id'].iloc[0]}"
    ok(f"834 round-trip: generate → parse → member={df['first_name'].iloc[0]} plan={df['plan_id'].iloc[0]} ✓")


# ── Runner ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import traceback
    tests = [
        test_hmac,
        test_idempotency_key_generation,
        test_audit_log,
        test_inbound_api,
        test_outbound_webhook_mock,
        test_edi_generator_roundtrip,
    ]
    for t in tests:
        try:
            t()
        except Exception:
            failed_count += 1
            print(f"\n  FAILED {t.__name__}:")
            traceback.print_exc()

    print(f"\n{DIVIDER}")
    print(f"Results: {passed_count} passed, {failed_count} failed")
    print(DIVIDER)
