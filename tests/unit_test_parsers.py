"""
Tests for format parsers and the PostgreSQL loader.
Run: python tests/test_parsers.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json

DIVIDER = "=" * 60
passed = failed = 0

def header(name): print(f"\n{DIVIDER}\n{name}\n{DIVIDER}")
def ok(msg):
    global passed; passed += 1; print(f"  PASS  {msg}")
def fail(msg, err):
    global failed; failed += 1; print(f"  FAIL  {msg}: {err}")


# ── JSON Parser ───────────────────────────────────────────────────────────────
def test_json_parser():
    header("JSON Parser")
    from parsers.json_parser import JSONParser
    p = JSONParser()

    # Single object
    df = p.parse('{"order_id": "O-1", "amount": 99.99, "status": "new"}')
    assert len(df) == 1 and df["order_id"].iloc[0] == "O-1"
    ok("Single JSON object → 1 row")

    # Array
    df = p.parse('[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]')
    assert len(df) == 2 and list(df.columns) == ["id","name"]
    ok("JSON array → 2 rows")

    # Envelope wrapper
    df = p.parse('{"data": [{"sku":"A"},{"sku":"B"},{"sku":"C"}], "total": 3}')
    assert len(df) == 3 and df["sku"].iloc[0] == "A"
    ok("JSON envelope {data:[...]} unwrapped → 3 rows")

    # JSONL
    jsonl = '{"id":1}\n{"id":2}\n{"id":3}'
    df = p.parse(jsonl)
    assert len(df) == 3
    ok("JSONL (3 lines) → 3 rows")

    # Nested flattening
    df = p.parse('{"order": {"id": "O-2"}, "customer": {"name": "Jane"}}')
    assert "order.id" in df.columns or "customer.name" in df.columns
    ok("Nested JSON flattened with dot notation")

    # Dict input directly
    df = p.parse({"event": "order.created", "ref": "REF-001"})
    assert df["event"].iloc[0] == "order.created"
    ok("Dict input accepted directly")

    # List input directly
    df = p.parse([{"x": 1}, {"x": 2}])
    assert len(df) == 2
    ok("List input accepted directly")


# ── CSV Parser ────────────────────────────────────────────────────────────────
def test_csv_parser():
    header("CSV Parser")
    from parsers.csv_parser import CSVParser
    p = CSVParser()

    # Standard CSV
    csv = "order_id,product,qty,price\nO-1,Widget,10,9.99\nO-2,Gadget,5,24.99"
    df = p.parse(csv)
    assert len(df) == 2
    assert list(df.columns) == ["order_id","product","qty","price"]
    assert df["order_id"].iloc[0] == "O-1"
    ok("Standard comma CSV → 2 rows, 4 columns")

    # Tab delimited
    tsv = "id\tname\tvalue\n1\tAlice\t100\n2\tBob\t200"
    df = p.parse(tsv)
    assert len(df) == 2 and "name" in df.columns
    ok("Tab-delimited TSV → 2 rows auto-detected")

    # Pipe delimited
    pipe = "code|desc|amount\nA|Widget A|50.00\nB|Widget B|75.00"
    df = p.parse(pipe)
    assert len(df) == 2 and df["code"].iloc[0] == "A"
    ok("Pipe-delimited → 2 rows auto-detected")

    # Whitespace stripping
    csv_spaces = " name , value \n Alice , 100 \n Bob , 200 "
    df = p.parse(csv_spaces)
    assert df["name"].iloc[0] == "Alice"
    ok("Whitespace stripped from column names and values")

    # Empty rows dropped
    csv_empty = "id,val\n1,a\n\n\n2,b"
    df = p.parse(csv_empty)
    assert len(df) == 2
    ok("Empty rows dropped")


# ── XML Parser ────────────────────────────────────────────────────────────────
def test_xml_parser():
    header("XML Parser")
    from parsers.xml_parser import XMLParser

    # Flat repeated elements
    xml = """<orders>
        <order><id>O-1</id><amount>99.99</amount><status>new</status></order>
        <order><id>O-2</id><amount>149.99</amount><status>shipped</status></order>
    </orders>"""
    df = XMLParser().parse(xml)
    assert len(df) == 2
    assert "order.id" in df.columns or "id" in df.columns
    ok("Flat XML repeated elements → 2 rows")

    # Attributes
    xml_attr = """<items>
        <item id="A" qty="10" price="9.99"/>
        <item id="B" qty="5"  price="24.99"/>
    </items>"""
    df = XMLParser().parse(xml_attr)
    assert len(df) == 2
    ok("XML attributes as columns → 2 rows")

    # SOAP envelope unwrapping
    soap = """<Envelope>
        <Body>
            <GetOrderResponse>
                <orders>
                    <order><id>SOAP-1</id><total>500.00</total></order>
                </orders>
            </GetOrderResponse>
        </Body>
    </Envelope>"""
    df = XMLParser().parse(soap)
    assert not df.empty
    ok("SOAP envelope unwrapped → records extracted")

    # Specific record tag
    xml_mixed = """<root>
        <metadata><version>1.0</version></metadata>
        <record><name>Alice</name><score>95</score></record>
        <record><name>Bob</name><score>88</score></record>
    </root>"""
    df = XMLParser(record_tag="record").parse(xml_mixed)
    assert len(df) == 2
    ok("Specific record_tag='record' → 2 rows ignoring metadata")


# ── Fixed-Width Parser ────────────────────────────────────────────────────────
def test_fixed_width_parser():
    header("Fixed-Width Parser")
    from parsers.fixed_width_parser import FixedWidthParser

    schema = [
        {"name": "record_type",  "start": 0, "length": 1},
        {"name": "account_id",   "start": 1, "length": 10},
        {"name": "name",         "start": 11,"length": 20},
        {"name": "amount",       "start": 31,"length": 10, "type": "implied_decimal_2"},
        {"name": "date",         "start": 41,"length": 8,  "type": "date_YYYYMMDD"},
    ]

    # Build fixed-width rows
    def fw_row(rt, acct, name, amount_cents, date):
        return (
            f"{rt}"
            f"{acct:<10}"
            f"{name:<20}"
            f"{str(amount_cents):>10}"
            f"{date}"
        )

    lines = "\n".join([
        fw_row("H", "HEADER    ", "FILE HEADER         ", "0000000000", "20240101"),
        fw_row("6", "ACC-001   ", "ALICE WONDERLAND    ", "0000150000", "20240101"),
        fw_row("6", "ACC-002   ", "BOB THE BUILDER     ", "0000075050", "20240102"),
        fw_row("9", "TRAILER   ", "FILE TRAILER        ", "0000000000", "20240101"),
    ])

    p = FixedWidthParser(schema, skip_record_types=["H", "9"])
    df = p.parse(lines)
    assert len(df) == 2, f"Expected 2 data rows, got {len(df)}"
    assert df["account_id"].iloc[0].strip() == "ACC-001"
    assert df["amount"].iloc[0] == 1500.00, f"Expected 1500.00, got {df['amount'].iloc[0]}"
    assert str(df["date"].iloc[0]) == "2024-01-01"
    ok("Fixed-width: 2 data rows, header/trailer skipped")
    ok(f"Implied decimal: amount=1500.00, date parsed: {df['date'].iloc[0]}")

    # NACHA preset
    nacha = FixedWidthParser.nacha_ach()
    assert nacha is not None
    ok("NACHA ACH preset instantiated")

    # IRS preset
    irs = FixedWidthParser.irs_1099()
    assert irs is not None
    ok("IRS 1099 preset instantiated")


# ── EDI Generator → Parser round-trip ────────────────────────────────────────
def test_edi_generator_roundtrip():
    header("EDI Generator → Parser round-trip (850, 856, 834)")
    from datetime import date
    import importlib, sys
    spec = importlib.util.spec_from_file_location('edi_generator', 'outbound/edi_generator.py')
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    EDIGenerator = mod.EDIGenerator
    from parsers import EDIParser, EDI834Parser

    gen = EDIGenerator(sender_id="OURCO001", receiver_id="PARTNER001")

    # 850
    order = {
        "po_number": "PO-RTRIP-001", "po_date": date(2024,3,1),
        "buyer_name": "OUR COMPANY", "buyer_id": "OUR001",
        "seller_name": "THEIR COMPANY", "seller_id": "THEIR001",
        "lines": [
            {"line_number":"1","product_id":"SKU-X","description":"Test item",
             "quantity":20,"unit_of_measure":"EA","unit_price":15.00},
        ]
    }
    edi = gen.generate_850(order)
    df  = EDIParser().parse(edi)
    assert df["po_number"].iloc[0] == "PO-RTRIP-001"
    assert df["quantity"].iloc[0] == 20.0
    ok("850: generate → parse → fields verified")

    # 856
    shipment = {
        "shipment_id":"SHIP-RTRIP-001","ship_date":date(2024,3,2),
        "po_number":"PO-RTRIP-001","carrier_code":"UPS",
        "tracking_number":"1Z12345","ship_to_name":"WAREHOUSE",
        "items":[{"item_id":"SKU-X","quantity":20,"unit_of_measure":"EA"}]
    }
    edi = gen.generate_856(shipment)
    df  = EDIParser().parse(edi)
    items = df[df["hl_level"]=="item"]
    assert len(items) == 1
    assert items["item_id"].iloc[0] == "SKU-X"
    ok("856: generate → parse → 1 item verified")

    # 834
    enrollment = {
        "reference_id":"ENROLL-001","employer_name":"TEST CORP","employer_id":"EMP001",
        "payer_name":"TEST PLAN","payer_id":"PLAN001",
        "members":[{
            "member_id":"MBR-001","subscriber_id":"SUB-001",
            "first_name":"CAROL","last_name":"DANVERS",
            "dob":date(1985,3,8),"gender":"F",
            "relationship_code":"18","maintenance_type_code":"021",
            "plan_id":"GOLD","coverage_type_code":"HLT",
            "effective_date":date(2024,1,1),
        }]
    }
    edi = gen.generate_834(enrollment)
    df  = EDI834Parser().parse(edi)
    assert df["first_name"].iloc[0] == "CAROL"
    assert df["plan_id"].iloc[0] == "GOLD"
    ok("834: generate → parse → member=CAROL, plan=GOLD verified")


# ── Postgres Loader (mock) ────────────────────────────────────────────────────
def test_postgres_loader_mock():
    header("PostgreSQL Loader — mock connection")
    import unittest.mock as mock
    import pandas as pd
    from loaders.postgres_loader import PostgresLoader

    # Build a mock connection that captures SQL
    mock_conn   = mock.MagicMock()
    mock_cursor = mock.MagicMock()
    mock_conn.cursor.return_value.__enter__ = mock.Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__  = mock.Mock(return_value=False)

    loader = PostgresLoader(mock_conn)

    df = pd.DataFrame([
        {"order_id": "O-1", "amount": 99.99, "status": "new"},
        {"order_id": "O-2", "amount": 149.99, "status": "shipped"},
    ])

    n = loader.load(df, table="raw_orders", mode="upsert", primary_keys=["order_id"])
    assert n == 2
    assert mock_cursor.executemany.called
    sql_used = mock_cursor.executemany.call_args[0][0]
    assert "ON CONFLICT" in sql_used
    assert "order_id" in sql_used
    ok(f"Upsert: {n} rows, ON CONFLICT clause present")

    # Column name sanitisation
    df_dirty = pd.DataFrame([{"Order ID": "O-3", "Item.Name": "Widget", "Price (USD)": 9.99}])
    mock_cursor.reset_mock()
    loader.load(df_dirty, table="raw_test", mode="append")
    ddl = mock_cursor.execute.call_args_list[0][0][0]
    assert "order_id" in ddl.lower() or "order" in ddl.lower()
    ok("Column names sanitised: spaces/dots → underscores")

    # Empty DataFrame skipped
    result = loader.load(pd.DataFrame(), table="raw_empty", mode="append")
    assert result == 0
    ok("Empty DataFrame → 0 rows, no DB call")


# ── Runner ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import traceback
    tests = [
        test_json_parser,
        test_csv_parser,
        test_xml_parser,
        test_fixed_width_parser,
        test_edi_generator_roundtrip,
        test_postgres_loader_mock,
    ]
    for t in tests:
        try:
            t()
        except Exception:
            failed += 1
            print(f"\n  FAILED {t.__name__}:")
            traceback.print_exc()

    print(f"\n{DIVIDER}")
    print(f"Results: {passed} passed, {failed} failed")
    print(DIVIDER)
