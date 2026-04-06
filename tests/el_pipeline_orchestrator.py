from integration_test_parsers import * 
from test_load_db import load_to_db

sources = [
    (test_csv_weather_stations, "weather_stations",    ["station_id"],            "csv",         "sample_data/csv/weather_stations.csv"),
    (test_csv_partner_orders,   "partner_orders",      ["order_id","product_id"], "csv",         "sample_data/csv/partner_orders.csv"),
    (test_csv_member_eligibility,"member_eligibility", ["member_id"],             "csv",         "sample_data/csv/member_eligibility.csv"),
    (test_json_weather,         "weather_json",        [],                        "json",        "sample_data/json/weather_response.json"),
    (test_json_okta_users,      "okta_users",          ["id"],                    "json",        "sample_data/json/okta_users.json"),
    (test_json_order_webhook,   "order_webhooks",      ["event_id"],              "json",        "sample_data/json/order_webhook_event.json"),
    (test_xml_product_catalog,  "product_catalog",     ["product_id"],                        "xml",         "sample_data/xml/product_catalog.xml"),
    (test_xml_soap_orders,      "soap_orders",         ["order_orderid"],                        "xml",         "sample_data/xml/order_status_soap.xml"),
    (test_fixed_width_nacha,    "nacha_payments",      ["account_number","trace_number"],          "fixed_width", "sample_data/text/nacha_payroll.ach"),
    (test_fixed_width_members,  "fw_members",          ["member_id"],             "fixed_width", "sample_data/text/member_dump.txt"),
    (test_edi_850,              "edi_850",             ["po_number","line_number"],"edi",        "sample_data/edi/sample_850.edi"),
    (test_edi_856,              "edi_856",             ["shipment_id","hl_number"],"edi",        "sample_data/edi/sample_856.edi"),
    (test_edi_834,              "edi_834",             ["member_id"],             "edi_834",     "sample_data/healthcare/health_edi/sample_834.edi"),
    (test_edi_837,              "edi_837",             ["claim_id","procedure_code"],"edi_837",  "sample_data/healthcare/health_edi/sample_837p.edi"),
    (test_edi_835,              "edi_835",             ["claim_id","procedure_code"],"edi_835",  "sample_data/healthcare/health_edi/sample_835.edi"),
    (test_edi_270,              "edi_270",             ["subscriber_id"],         "edi_270",     "sample_data/healthcare/health_edi/sample_270_271.edi"),
    (test_edi_271,              "edi_271",             ["subscriber_id", "benefit_seq"], "edi_271", "sample_data/healthcare/health_edi/sample_270_271.edi")
]

print(f"Total sources: {len(sources)}")

for fn, table, pkeys, fmt, src_file in sources:
    print(f"\n--- {table} ---")
    try:
        df = fn()
        print(f"Parsed: {len(df)} rows, {len(df.columns)} columns")
        mode = "append" if not pkeys else "upsert"
        load_to_db(df,
            table=table,
            primary_keys=pkeys if pkeys else None,
            mode=mode,
            source_file=src_file,
            source_format=fmt,
            source_system="local_file"
        )
    except Exception as e:
        import traceback
        print(f"FAILED: {e}")
        traceback.print_exc()