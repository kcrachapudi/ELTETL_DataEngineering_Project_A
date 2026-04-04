"""
PostgreSQL Loader — writes normalised DataFrames to PostgreSQL.

Supports:
    append      — INSERT, fail on duplicate primary key
    upsert      — INSERT ... ON CONFLICT DO UPDATE (idempotent)
    replace     — TRUNCATE then INSERT (full reload)
    create      — CREATE TABLE from DataFrame schema if not exists

Idempotency:
    The upsert mode is the default and most important.
    Run the same pipeline 10 times — same result every time.
    No duplicate rows, no errors on re-runs.

Usage:
    conn   = psycopg2.connect(...)
    loader = PostgresLoader(conn)
    loader.load(df, table="raw_orders", mode="upsert", primary_keys=["order_id"])
    loader.load(df, table="raw_members", mode="upsert", primary_keys=["member_id", "plan_id"])
"""

import logging
from typing import Any, Literal

import pandas as pd

logger = logging.getLogger(__name__)

LoadMode = Literal["append", "upsert", "replace", "create"]

# PostgreSQL type mapping from pandas dtype
DTYPE_MAP = {
    "int64":          "BIGINT",
    "int32":          "INTEGER",
    "float64":        "DOUBLE PRECISION",
    "float32":        "REAL",
    "bool":           "BOOLEAN",
    "object":         "TEXT",
    "datetime64[ns]": "TIMESTAMPTZ",
    "date":           "DATE",
}


class PostgresLoader:

    def __init__(self, conn, schema: str = "public"):
        self._conn   = conn
        self._schema = schema

    def load(
        self,
        df:           pd.DataFrame,
        table:        str,
        mode:         LoadMode = "upsert",
        primary_keys: list[str] = None,
        chunk_size:   int = 1000,
    ) -> int:
        """
        Write a DataFrame to PostgreSQL.

        Args:
            df:           the normalised DataFrame to write
            table:        target table name (will be created if not exists)
            mode:         'upsert' (default), 'append', 'replace', 'create'
            primary_keys: columns used for conflict detection in upsert mode
            chunk_size:   rows per INSERT batch (default 1000)

        Returns:
            Number of rows written.
        """
        if df.empty:
            logger.warning(f"Empty DataFrame — nothing to load into {table}")
            return 0

        df = self._prepare(df)
        qualified = f"{self._schema}.{table}"

        self._ensure_table(df, qualified)

        if mode == "replace":
            self._truncate(qualified)
            mode = "append"

        if mode == "upsert" and not primary_keys:
            logger.warning("Upsert mode requested but no primary_keys given — falling back to append.")
            mode = "append"

        total = 0
        for chunk_df in self._chunks(df, chunk_size):
            if mode == "upsert":
                n = self._upsert(chunk_df, qualified, primary_keys)
            else:
                n = self._insert(chunk_df, qualified)
            total += n

        self._conn.commit()
        logger.info(f"Loaded {total} rows into {qualified} (mode={mode})")
        return total

    def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up DataFrame before writing — handle NaN, types, column names."""
        df = df.copy()
        # sanitise column names — lowercase, spaces to underscores
        df.columns = [
            c.lower().strip().replace(" ", "_").replace(".", "_").replace("-", "_")
            for c in df.columns
        ]
        # convert None / NaN to None (psycopg2 handles it as NULL)
        df = df.where(pd.notnull(df), None)
        return df

    def _ensure_table(self, df: pd.DataFrame, table: str):
        """Create the table if it doesn't exist, inferred from DataFrame schema."""
        cols = []
        for col, dtype in df.dtypes.items():
            pg_type = DTYPE_MAP.get(str(dtype), "TEXT")
            cols.append(f'"{col}" {pg_type}')
        ddl = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(cols)});"
        with self._conn.cursor() as cur:
            cur.execute(ddl)
        self._conn.commit()

    def _insert(self, df: pd.DataFrame, table: str) -> int:
        columns = list(df.columns)
        col_str = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
        rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
        with self._conn.cursor() as cur:
            cur.executemany(sql, rows)
        return len(rows)

    def _upsert(self, df: pd.DataFrame, table: str, primary_keys: list[str]) -> int:
        """INSERT ... ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col"""
        columns  = list(df.columns)
        non_pks  = [c for c in columns if c not in primary_keys]
        col_str  = ", ".join(f'"{c}"' for c in columns)
        ph       = ", ".join(["%s"] * len(columns))
        pk_str   = ", ".join(f'"{k}"' for k in primary_keys)

        if non_pks:
            update_str = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pks)
            conflict   = f"ON CONFLICT ({pk_str}) DO UPDATE SET {update_str}"
        else:
            conflict   = f"ON CONFLICT ({pk_str}) DO NOTHING"

        sql  = f"INSERT INTO {table} ({col_str}) VALUES ({ph}) {conflict}"
        rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
        with self._conn.cursor() as cur:
            cur.executemany(sql, rows)
        return len(rows)

    def _truncate(self, table: str):
        with self._conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")
        self._conn.commit()

    def _chunks(self, df: pd.DataFrame, size: int):
        for i in range(0, len(df), size):
            yield df.iloc[i:i + size]
