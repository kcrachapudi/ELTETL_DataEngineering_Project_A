from parsers.csv_parser import CSVParser
from parsers.json_parser import JSONParser
from parsers.xml_parser import XMLParser
from parsers.fixed_width_parser import FixedWidthParser
from parsers.edi_parser import EDIParser
from parsers.edi_834_parser import EDI834Parser
from parsers.edi_837_parser import EDI837Parser
from parsers.edi_835_parser import EDI835Parser
from parsers.edi_270_parser import EDI270Parser
from parsers.edi_271_parser import EDI271Parser

def test_csv_weather_stations():
    return CSVParser().parse("sample_data/csv/weather_stations.csv")

def test_csv_partner_orders():
    return CSVParser().parse("sample_data/csv/partner_orders.csv")

def test_csv_member_eligibility():
    return CSVParser().parse("sample_data/csv/member_eligibility.csv")

def test_json_weather():
    import json
    import pandas as pd
    with open("sample_data/json/weather_response.json") as f:
        data = json.load(f)
    hourly = data["hourly"]
    df = pd.DataFrame(hourly)
    df["latitude"]  = data["latitude"]
    df["longitude"] = data["longitude"]
    df["timezone"]  = data["time"]
    return df

def test_json_okta_users():
    return JSONParser().parse("sample_data/json/okta_users.json")

def test_json_order_webhook():
    return JSONParser().parse("sample_data/json/order_webhook_event.json")

def test_xml_product_catalog():
    return XMLParser(record_tag="Product").parse("sample_data/xml/product_catalog.xml")

def test_xml_soap_orders():
    return XMLParser(record_tag="Order").parse("sample_data/xml/order_status_soap.xml")

def test_fixed_width_nacha():
    return FixedWidthParser.nacha_ach().parse("sample_data/text/nacha_payroll.ach")

def test_fixed_width_members():
    schema = [
        {"name": "record_type", "start": 0,  "length": 1},
        {"name": "member_id",   "start": 1,  "length": 10},
        {"name": "sub_id",      "start": 11, "length": 10},
        {"name": "last_name",   "start": 21, "length": 20},
        {"name": "first_name",  "start": 41, "length": 20},
        {"name": "dob",         "start": 61, "length": 8,  "type": "date_YYYYMMDD"},
        {"name": "gender",      "start": 69, "length": 1},
        {"name": "plan_id",     "start": 71, "length": 10},
        {"name": "group_id",    "start": 81, "length": 10},
        {"name": "cov_type",    "start": 91, "length": 10},
        {"name": "eff_date",    "start": 101,"length": 8,  "type": "date_YYYYMMDD"},
    ]
    return FixedWidthParser(schema, skip_record_types=["H","T"]).parse("sample_data/text/member_dump.txt")

def test_edi_850():
    return EDIParser().parse("sample_data/edi/sample_850.edi")

def test_edi_856():
    return EDIParser().parse("sample_data/edi/sample_856.edi")

def test_edi_834():
    return EDI834Parser().parse("sample_data/healthcare/health_edi/sample_834.edi")

def test_edi_837():
    return EDI837Parser().parse("sample_data/healthcare/health_edi/sample_837p.edi")

def test_edi_835():
    return EDI835Parser().parse("sample_data/healthcare/health_edi/sample_835.edi")

def test_edi_270():
    return EDI270Parser().parse("sample_data/healthcare/health_edi/sample_270_271.edi")

def test_edi_271():
    from parsers.edi_271_parser import EDI271Parser
    df = EDI271Parser().parse("sample_data/healthcare/health_edi/sample_270_271.edi")
    df = df.reset_index(drop=True)
    df["benefit_seq"] = df.index
    return df