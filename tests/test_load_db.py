import psycopg2
from loaders.postgres_loader import PostgresLoader


def get_connection():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname="pipeline_db",
        user="pipeline_user",
        password="changeme"
    )


def load_to_db(df, table, primary_keys=None, mode="upsert"):
    conn = get_connection()
    try:
        loader = PostgresLoader(conn)
        n = loader.load(df, table=table, mode=mode, primary_keys=primary_keys)
        print(f"Loaded {n} rows into raw.{table}")
        return n
    finally:
        conn.close()