import psycopg2
from datetime import datetime, timezone
from loaders.postgres_loader import PostgresLoader
import json

def get_connection():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname="pipeline_db",
        user="pipeline_user",
        password="changeme"
    )

def load_to_db(df, table, primary_keys=None, mode="upsert",
               source_file="", source_format="", source_system="local_file"):
    now = datetime.now(timezone.utc).isoformat()

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    df = df.assign(
        _source_file=source_file,
        _source_format=source_format,
        _source_system=source_system,
        _ingested_at=now
    )
    conn = get_connection()
    try:
        loader = PostgresLoader(conn)
        n = loader.load(df, table=table, mode=mode, primary_keys=primary_keys)
        print(f"Loaded {n} rows into raw.{table}")
        return n
    finally:
        conn.close()