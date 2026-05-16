"""
Tests for EDIParser — run with:  python -m pytest tests/ -v
or directly:                      python tests/test_edi_parser.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from parsers.edi_parser import EDIParser
from parsers.base_parser import ParserError

parser = EDIParser()


def test_850_from_file():
    df = parser.parse("sample_data/sample_850.edi")
    assert not df.empty
    assert "850" in df["transaction_type"].values
    po = df[df["transaction_type"] == "850"]
    assert len(po) == 3, f"Expected 3 line items, got {len(po)}"
    assert po["po_number"].iloc[0] == "PO-98765"
    assert po["quantity"].iloc[0] == 50.0
    assert po["unit_price"].iloc[0] == 9.99
    assert po["line_total"].iloc[0] == 499.5
    assert po["buyer_name"].iloc[0] == "ACME CORP"
    assert po["seller_name"].iloc[0] == "WIDGETS INC"
    print(f"  850 test passed — {len(po)} line items parsed")
    print(po[["po_number","line_number","product_id_1","quantity",
              "unit_price","line_total","description"]].to_string(index=False))


def test_856_from_file():
    df = parser.parse("sample_data/sample_856.edi")
    assert not df.empty
    asn = df[df["transaction_type"] == "856"]
    assert len(asn) > 0
    assert asn["shipment_id"].iloc[0] == "SHIP-20240102-001"
    items = asn[asn["hl_level"] == "item"]
    assert len(items) == 2, f"Expected 2 items, got {len(items)}"
    print(f"\n  856 test passed — {len(asn)} HL segments, {len(items)} items")
    print(asn[["shipment_id","hl_level","item_id",
               "quantity_shipped","tracking_number"]].to_string(index=False))


def test_810_from_file():
    df = parser.parse("sample_data/sample_810_997.edi")
    inv = df[df["transaction_type"] == "810"]
    assert not inv.empty
    assert inv["invoice_number"].iloc[0] == "INV-2024-0042"
    assert inv["po_number"].iloc[0] == "PO-98765"
    assert len(inv) == 3, f"Expected 3 invoice lines, got {len(inv)}"
    line_total = inv["line_total"].sum()
    tds_total = inv["total_amount"].iloc[0]
    assert abs(tds_total - 1444.0) < 0.01, f"Expected TDS=1444.0, got {tds_total}"
    print(f"\n  810 test passed — {len(inv)} invoice lines, line sum={line_total:.2f}, TDS={tds_total:.2f}")
    print(inv[["invoice_number","line_number","product_id",
               "quantity","unit_price","line_total"]].to_string(index=False))


def test_997_from_file():
    df = parser.parse("sample_data/sample_810_997.edi")
    ack = df[df["transaction_type"] == "997"]
    assert not ack.empty
    assert ack["ack_status"].iloc[0] == "accepted"
    assert ack["ack_transaction_type"].iloc[0] == "810"
    print(f"\n  997 test passed — status: {ack['ack_status'].iloc[0]}")
    print(ack[["ack_transaction_type","ack_control_number",
               "ack_status_code","ack_status"]].to_string(index=False))


def test_multiple_transactions_in_one_file():
    df = parser.parse("sample_data/sample_810_997.edi")
    types = set(df["transaction_type"].unique())
    assert "810" in types
    assert "997" in types
    print(f"\n  Multi-transaction test passed — found: {types}")


def test_raw_string_input():
    raw = (
        "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       "
        "*240101*1200*^*00501*000000099*0*P*:~"
        "GS*PO*SENDER*RECEIVER*20240101*1200*1*X*005010~"
        "ST*850*0001~"
        "BEG*00*SA*PO-STRING-TEST**20240101~"
        "N1*BY*TEST BUYER~"
        "N1*SE*TEST SELLER~"
        "PO1*1*5*EA*19.99*PE*IN*TEST-SKU-001~"
        "SE*7*0001~"
        "GE*1*1~"
        "IEA*1*000000099~"
    )
    df = parser.parse(raw)
    assert df["po_number"].iloc[0] == "PO-STRING-TEST"
    assert df["quantity"].iloc[0] == 5.0
    print(f"\n  Raw string input test passed")


def test_envelope_metadata_attached():
    df = parser.parse("sample_data/sample_850.edi")
    assert "sender_id" in df.columns
    assert "receiver_id" in df.columns
    assert "interchange_date" in df.columns
    assert df["sender_id"].iloc[0] == "BUYER123"
    print(f"\n  Envelope metadata test passed — sender: {df['sender_id'].iloc[0]}")


def test_bad_input_raises():
    try:
        parser.parse("this is not EDI at all")
        assert False, "Should have raised ParserError"
    except ParserError as e:
        print(f"\n  Error handling test passed — caught: {e}")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    tests = [
        test_850_from_file,
        test_856_from_file,
        test_810_from_file,
        test_997_from_file,
        test_multiple_transactions_in_one_file,
        test_raw_string_input,
        test_envelope_metadata_attached,
        test_bad_input_raises,
    ]
    passed = 0
    failed = 0
    print("=" * 60)
    print("EDI Parser Test Suite")
    print("=" * 60)
    for t in tests:
        try:
            print(f"\nRunning {t.__name__} ...")
            t()
            passed += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            failed += 1
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
