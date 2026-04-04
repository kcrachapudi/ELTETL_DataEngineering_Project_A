"""
Database Extractor — pulls data from a SQL database.

Useful for:
    - Extracting from a source OLTP database into the pipeline
    - Change Data Capture (CDC) — only new/modified rows since last run
    - Full table dumps for reference data (lookup tables, config)
    - Cross-database replication

Supports two extract modes:
    full        — SELECT * FROM table (or custom query)
    incremental — SELECT * WHERE updated_at > last_run_timestamp

Usage:
    import psycopg2
    conn = psycopg2.connect(dsn)

    # Full extract
    ext = DBExtractor(conn, query="SELECT * FROM orders")
    df  = ext.fetch()   # returns DataFrame directly — no parser needed

    # Incremental extract — only rows updated since last run
    ext = DBExtractor(
        conn,
        query="SELECT * FROM orders WHERE updated_at > %(since)s",
        incremental=True,
        watermark_table="pipeline_watermarks",
        watermark_key="orders",
    )
    df = ext.fetch()
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd

from .base_extractor import BaseExtractor, ExtractorError

logger = logging.getLogger(__name__)

WATERMARK_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    source_key   TEXT PRIMARY KEY,
    last_run_at  TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01',
    row_count    INTEGER,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class DBExtractor(BaseExtractor):
    """
    Extracts data from a SQL database via psycopg2 connection.
    Returns a DataFrame directly — no parser step needed.
    """

    def __init__(
        self,
        conn,
        query:             str,
        params:            Optional[dict]  = None,
        incremental:       bool            = False,
        watermark_table:   str             = "pipeline_watermarks",
        watermark_key:     str             = "",
        chunk_size:        int             = 10000,
    ):
        self._conn       = conn
        self._query      = query
        self._params     = params or {}
        self._incremental = incremental
        self._wm_table   = watermark_table
        self._wm_key     = watermark_key
        self._chunk_size = chunk_size

    @property
    def source_name(self) -> str:
        preview = self._query[:60].replace("\n", " ").strip()
        return f"DB: {preview}..."

    def fetch(self) -> pd.DataFrame:
        """
        Execute the query and return a DataFrame.
        If incremental=True, injects {since} param from watermark table.
        """
        params = {**self._params}

        if self._incremental:
            since = self._get_watermark()
            params["since"] = since
            logger.info(f"Incremental extract: {self._wm_key} since {since}")

        start = datetime.now(timezone.utc)
        try:
            df = pd.read_sql(self._query, self._conn, params=params)
        except Exception as exc:
            raise ExtractorError(f"DB extract failed: {exc}")

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(
            f"DB extract complete — {len(df)} rows, "
            f"{len(df.columns)} columns, {elapsed:.2f}s"
        )

        if self._incremental and self._wm_key:
            self._update_watermark(len(df))

        return df

    def _get_watermark(self) -> datetime:
        """Get the last successful run timestamp for this source."""
        self._ensure_watermark_table()
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT last_run_at FROM {self._wm_table} WHERE source_key = %s",
                (self._wm_key,),
            )
            row = cur.fetchone()
        if row:
            return row[0]
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _update_watermark(self, row_count: int):
        """Update the watermark to now after a successful extract."""
        self._ensure_watermark_table()
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._wm_table} (source_key, last_run_at, row_count)
                VALUES (%s, NOW(), %s)
                ON CONFLICT (source_key)
                DO UPDATE SET last_run_at = NOW(), row_count = %s, updated_at = NOW()
                """,
                (self._wm_key, row_count, row_count),
            )
        self._conn.commit()
        logger.info(f"Watermark updated: {self._wm_key} → NOW() ({row_count} rows)")

    def _ensure_watermark_table(self):
        with self._conn.cursor() as cur:
            cur.execute(WATERMARK_DDL)
        self._conn.commit()
