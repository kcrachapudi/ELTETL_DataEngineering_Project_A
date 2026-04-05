from tests.integration_test_parsers import test_csv_parser
from test_load_db import load_to_db

# Run the parse test to get a DataFrame
df = test_csv_parser()
print('Rows:', len(df))
print('Columns:', list(df.columns))
print()
print(df.head(3).to_string())

# Add source metadata columns
db_table = "weather_stations"
db_primary_keys = ["station_id"]
db_mode = "upsert"
db_source_file="sample_data/csv/weather_stations.csv"
db_source_format="csv"
db_source_system="local_file"

# Load the DataFrame into the database
load_to_db(df,
    table=db_table,
    primary_keys=db_primary_keys,
    mode=db_mode,
    source_file=db_source_file,
    source_format=db_source_format,
    source_system=db_source_system
)