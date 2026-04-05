from parsers.csv_parser import CSVParser

def test_csv_parser():
    parser = CSVParser()
    df = parser.parse('sample_data/csv/weather_stations.csv')

    print('Rows:', len(df))
    print('Columns:', list(df.columns))
    print()
    print(df.head(3).to_string())

    return df