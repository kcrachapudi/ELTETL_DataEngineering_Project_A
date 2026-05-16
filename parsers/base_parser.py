from abc import ABC, abstractmethod
from typing import Any
import pandas as pd


class BaseParser(ABC):
    """
    Contract every format parser must honour.
    One rule: take a source, return a clean DataFrame.
    Format complexity lives here and nowhere else.
    """

    @abstractmethod
    def parse(self, source: Any) -> pd.DataFrame:
        """
        Parse the source into a normalised DataFrame.

        Args:
            source: file path (str), raw string, bytes, or file-like object
                    depending on the format. Each parser documents its own.

        Returns:
            pd.DataFrame — clean, typed, ready for the load layer.

        Raises:
            ParserError: if the source is malformed or unrecognised.
        """

    @property
    @abstractmethod
    def format_name(self) -> str:
        """Human-readable name shown in logs. E.g. 'EDI X12 850'."""

    def __repr__(self):
        return f"<{self.__class__.__name__} format={self.format_name}>"


class ParserError(Exception):
    """Raised when a parser cannot process its input."""
