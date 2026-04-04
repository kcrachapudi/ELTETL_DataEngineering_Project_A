"""
Base extractor — abstract interface every extractor must implement.

An extractor knows WHERE to get data.
A parser knows HOW to read the format.
They are deliberately separate so you can mix and match:
    HTTP extractor + JSON parser
    SFTP extractor + EDI parser
    DB extractor  + any parser (or no parser — already structured)

Contract: fetch() returns raw bytes or a string.
The caller then passes that to the appropriate parser.

Usage:
    extractor = OpenMeteoExtractor(latitude=32.77, longitude=-96.79)
    raw = extractor.fetch()
    df  = JSONParser().parse(raw)
    PostgresLoader(conn).load(df, table="raw_weather")
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseExtractor(ABC):

    @abstractmethod
    def fetch(self) -> Any:
        """
        Fetch raw data from the source.
        Returns bytes, str, file path, or file-like object
        depending on the source type. Each extractor documents its own.
        Raises ExtractorError on failure.
        """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable source name shown in logs."""

    def __repr__(self):
        return f"<{self.__class__.__name__} source={self.source_name}>"


class ExtractorError(Exception):
    """Raised when an extractor cannot retrieve data from its source."""
