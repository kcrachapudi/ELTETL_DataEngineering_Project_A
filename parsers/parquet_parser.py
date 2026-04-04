"""
Parquet / Avro Parser — columnar formats used in data lakes and warehouses.

Parquet:
    The standard format for BigQuery exports, Spark output, dbt seeds,
    and most modern data lake storage. Highly compressed, typed, fast.

Avro:
    Schema-embedded binary format, common in Kafka messages and
    data streaming pipelines. Schema travels with the data.

Usage:
    parser = ParquetParser()
    df = parser.parse("path/to/file.parquet")
    df = parser.parse("path/to/file.snappy.parquet")

    parser = AvroParser()
    df = parser.parse("path/to/file.avro")

Dependencies:
    pip install pyarrow       (Parquet)
    pip install fastavro      (Avro)
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .base_parser import BaseParser, ParserError

logger = logging.getLogger(__name__)


class ParquetParser(BaseParser):

    @property
    def format_name(self) -> str:
        return "Parquet"

    def parse(self, source: Any) -> pd.DataFrame:
        """
        Args:
            source: file path string, Path object, or file-like object.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError:
            raise ParserError(
                "pyarrow is required for Parquet parsing. "
                "Install with: pip install pyarrow"
            )

        target = self._resolve(source)
        try:
            table = pq.read_table(target)
            df    = table.to_pandas()
            logger.info(
                f"Parquet parse complete — {len(df)} rows, "
                f"{len(df.columns)} columns, "
                f"schema: {[f.name for f in table.schema]}"
            )
            return df
        except Exception as exc:
            raise ParserError(f"Parquet read failed: {exc}")

    def parse_with_filters(
        self,
        source: Any,
        columns: list[str] = None,
        filters: list = None,
    ) -> pd.DataFrame:
        """
        Read only specific columns or apply predicate pushdown filters.
        Filters format: [("column", "op", value)] e.g. [("amount", ">", 0)]
        Much faster than reading full file then filtering in pandas.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError:
            raise ParserError("pyarrow required. pip install pyarrow")

        target = self._resolve(source)
        try:
            table = pq.read_table(target, columns=columns, filters=filters)
            df    = table.to_pandas()
            logger.info(f"Parquet filtered read — {len(df)} rows, {len(df.columns)} cols")
            return df
        except Exception as exc:
            raise ParserError(f"Parquet filtered read failed: {exc}")

    def _resolve(self, source: Any):
        if isinstance(source, (str, Path)):
            path = Path(source)
            if path.exists():
                return str(path)
            raise ParserError(f"File not found: {source}")
        return source  # file-like object


class AvroParser(BaseParser):

    @property
    def format_name(self) -> str:
        return "Avro"

    def parse(self, source: Any) -> pd.DataFrame:
        try:
            import fastavro
        except ImportError:
            raise ParserError(
                "fastavro is required for Avro parsing. "
                "Install with: pip install fastavro"
            )

        if isinstance(source, (str, Path)):
            path = Path(source)
            if not path.exists():
                raise ParserError(f"File not found: {source}")
            with open(path, "rb") as f:
                return self._read_avro(f, fastavro)

        # file-like object
        return self._read_avro(source, fastavro)

    def _read_avro(self, f, fastavro) -> pd.DataFrame:
        try:
            reader  = fastavro.reader(f)
            records = list(reader)
            if not records:
                raise ParserError("Avro file is empty.")
            df = pd.DataFrame(records)
            logger.info(f"Avro parse complete — {len(df)} rows, schema: {list(df.columns)}")
            return df
        except Exception as exc:
            raise ParserError(f"Avro read failed: {exc}")
