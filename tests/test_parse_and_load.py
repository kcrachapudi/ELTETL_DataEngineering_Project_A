from test_parse import test_csv_parser
from test_load_db import load_to_db


# Run the parse test to get a DataFrame
df = test_csv_parser()

# Load the DataFrame into the database
load_to_db(df, table="weather_stations", primary_keys=["station_id"])