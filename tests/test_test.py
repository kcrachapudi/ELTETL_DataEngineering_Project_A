import sys
from integration_test_parsers import test_edi_271, test_fixed_width_nacha
from integration_test_parsers import test_xml_product_catalog, test_xml_soap_orders, test_json_weather
import json
import pandas as pd


def test_json_weather():
    import json
    import pandas as pd
    with open("sample_data/json/weather_response.json") as f:
        data = json.load(f)
    df = pd.DataFrame(data["hourly"])
    df["latitude"]  = data["latitude"]
    df["longitude"] = data["longitude"]
    df["time"] = df["time"].str.strip()
    return df
 
df = test_json_weather()
print(len(df))
print(df.columns.tolist())
print(df["time"].head(3).tolist())

sys.exit(0)

print("product_catalog:", test_xml_product_catalog().columns.tolist())
print("soap_orders:", test_xml_soap_orders().columns.tolist())
print("weather_json:", test_json_weather().columns.tolist())


df = test_edi_271()
print(df[["subscriber_id", "eligibility_code", "service_type_code"]].to_string())

df = test_fixed_width_nacha()
print(len(df), 'rows')
print(df.to_string())

df = test_fixed_width_nacha()
print(df["trace_number"].tolist())

df = test_edi_271()
print(len(df))
print(df["benefit_seq"].tolist())

