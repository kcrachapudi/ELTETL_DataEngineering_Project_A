"""
CSV / Excel Parser — handles flat file formats.

Supports:
    .csv   — comma, tab, pipe, semicolon delimited (auto-detected)
    .tsv   — tab delimited
    .txt   — any delimiter
    .xlsx  — Excel workbook (first sheet or named sheet)
    .xls   — Legacy Excel

Auto-detects:
    - Delimiter (uses csv.Sniffer)
    - Encoding (tries UTF-8, then latin-1)
    - Header row
    - Numeric columns

Usage:
    parser = CSVParser()
    df = parser.parse("path/to/file.csv")
    df = parser.parse("path/to/file.xlsx", sheet_name="Orders")
    df = parser.parse(csv_string)
"""

import csv
import io
import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from .base_parser import BaseParser, ParserError

logger = logging.getLogger(__name__)


class CSVParser(BaseParser):

    def __init__(self, sheet_name: Optional[str] = 0):
        self._sheet = sheet_name  # for Excel

    @property
    def format_name(self) -> str:
        return "CSV/Excel"

    def parse(self, source: Any) -> pd.DataFrame:
        if isinstance(source, bytes):
            source = source.decode("utf-8", errors="replace")

        if isinstance(source, str) and len(source) < 512:
            path = Path(source)
            if path.exists():
                return self._parse_file(path)

        if isinstance(source, str):
            return self._parse_csv_string(source)

        if isinstance(source, Path):
            return self._parse_file(source)

        raise ParserError(f"Unsupported source type: {type(source)}")

    def _parse_file(self, path: Path) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix in (".xlsx", ".xls", ".xlsm"):
            return self._parse_excel(path)
        return self._parse_csv_path(path)

    def _parse_excel(self, path: Path) -> pd.DataFrame:
        try:
            df = pd.read_excel(path, sheet_name=self._sheet, dtype=str)
            df = self._clean(df)
            logger.info(f"Excel parse complete — {len(df)} rows from {path.name}")
            return df
        except Exception as exc:
            raise ParserError(f"Excel parse failed: {exc}")

    def _parse_csv_path(self, path: Path) -> pd.DataFrame:
        # try UTF-8 first, fall back to latin-1
        for encoding in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                text = path.read_text(encoding=encoding)
                return self._parse_csv_string(text, hint=path.name)
            except UnicodeDecodeError:
                continue
        raise ParserError(f"Could not decode {path.name} with any supported encoding.")

    def _parse_csv_string(self, text: str, hint: str = "") -> pd.DataFrame:
        text = text.strip()
        if not text:
            raise ParserError("Empty CSV input.")

        delimiter = self._detect_delimiter(text)
        try:
            df = pd.read_csv(
                io.StringIO(text),
                sep=delimiter,
                dtype=str,
                skip_blank_lines=True,
                on_bad_lines="warn",
            )
            df = self._clean(df)
            logger.info(
                f"CSV parse complete — {len(df)} rows, "
                f"delimiter={repr(delimiter)}"
                + (f", file={hint}" if hint else "")
            )
            return df
        except Exception as exc:
            raise ParserError(f"CSV parse failed: {exc}")

    def _detect_delimiter(self, text: str) -> str:
        """Use csv.Sniffer on first 4096 chars, fall back to comma."""
        sample = text[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
            return dialect.delimiter
        except csv.Error:
            # manual fallback — count occurrences of each candidate
            first_line = sample.split("\n")[0]
            counts = {d: first_line.count(d) for d in [",", "\t", "|", ";"]}
            best = max(counts, key=counts.__getitem__)
            return best if counts[best] > 0 else ","

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip whitespace from column names and string values."""
        df.columns = [str(c).strip() for c in df.columns]
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()
        # drop fully empty rows
        df = df.dropna(how="all").reset_index(drop=True)
        return df
