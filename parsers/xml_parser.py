"""
XML Parser — handles SOAP envelopes, REST XML responses, custom feeds.

Supports:
    - Simple flat XML          <orders><order><id>1</id></order></orders>
    - SOAP envelopes           extracts Body content automatically
    - Namespaced XML           strips namespaces for clean column names
    - Attributes as columns    <item id="1" qty="5"/> → id, qty columns
    - Nested → flattened       child elements become dot-notation columns

Usage:
    parser = XMLParser()
    df = parser.parse("path/to/file.xml")
    df = parser.parse(xml_string)
    df = parser.parse(xml_bytes)

    # specify the repeating record element if auto-detection misses it
    parser = XMLParser(record_tag="Order")
    df = parser.parse(xml_string)
"""

import logging
import re
from pathlib import Path
from typing import Any, Optional
from xml.etree import ElementTree as ET

import pandas as pd

from .base_parser import BaseParser, ParserError

logger = logging.getLogger(__name__)

SOAP_BODY_TAGS = {
    "{http://schemas.xmlsoap.org/soap/envelope/}Body",
    "{http://www.w3.org/2003/05/soap-envelope}Body",
    "Body", "body",
}


class XMLParser(BaseParser):

    def __init__(self, record_tag: Optional[str] = None):
        self._record_tag = record_tag  # force a specific repeating element

    @property
    def format_name(self) -> str:
        return "XML"

    def parse(self, source: Any) -> pd.DataFrame:
        text = self._load(source)
        root = self._parse_xml(text)
        root = self._unwrap_soap(root)
        records = self._extract_records(root)

        if not records:
            raise ParserError("XML parsed but produced no records.")

        df = pd.DataFrame(records)
        df = df.dropna(how="all").reset_index(drop=True)
        logger.info(f"XML parse complete — {len(df)} rows, {len(df.columns)} columns")
        return df

    def _load(self, source: Any) -> str:
        if isinstance(source, bytes):
            return source.decode("utf-8", errors="replace")
        if isinstance(source, Path):
            return source.read_text(encoding="utf-8")
        if isinstance(source, str):
            if len(source) < 512 and not source.strip().startswith("<"):
                path = Path(source)
                if path.exists():
                    return path.read_text(encoding="utf-8")
            return source
        raise ParserError(f"Unsupported source type: {type(source)}")

    def _parse_xml(self, text: str) -> ET.Element:
        # strip all namespace declarations and prefixes for simpler parsing
        text = re.sub(r'\s+xmlns(?::[a-zA-Z0-9_]+)?="[^"]*"', "", text)
        text = re.sub(r'\s+xmlns(?::[a-zA-Z0-9_]+)?=\'[^\']*\'', "", text)
        # strip namespace prefixes from element names e.g. soap:Envelope -> Envelope
        text = re.sub(r'<(/?)([a-zA-Z0-9_]+):([a-zA-Z0-9_]+)', r'<\1\3', text)
        try:
            return ET.fromstring(text)
        except ET.ParseError as exc:
            raise ParserError(f"XML parse error: {exc}")

    def _unwrap_soap(self, root: ET.Element) -> ET.Element:
        """If root is a SOAP envelope, drill into the Body."""
        tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag
        if tag in ("Envelope", "envelope"):
            for child in root:
                ctag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if ctag in ("Body", "body"):
                    # return first child of Body
                    children = list(child)
                    if children:
                        logger.debug("SOAP envelope detected — unwrapped Body")
                        return children[0]
        return root

    def _extract_records(self, root: ET.Element) -> list[dict]:
        # if record_tag specified, find all matching elements
        if self._record_tag:
            elements = root.findall(f".//{self._record_tag}")
            if not elements:
                elements = root.findall(self._record_tag)
            return [self._element_to_dict(el) for el in elements]

        # auto-detect: find the most frequent child tag at depth 1
        children = list(root)
        if not children:
            return [self._element_to_dict(root)]

        tag_counts: dict[str, int] = {}
        for child in children:
            t = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            tag_counts[t] = tag_counts.get(t, 0) + 1

        most_common = max(tag_counts, key=tag_counts.__getitem__)
        if tag_counts[most_common] > 1:
            # repeating elements at root level — each is a record
            records = []
            for child in children:
                t = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if t == most_common:
                    records.append(self._element_to_dict(child))
            return records

        # single wrapper element — go one level deeper
        if len(children) == 1:
            return self._extract_records(children[0])

        # fall back — treat root as single record
        return [self._element_to_dict(root)]

    def _element_to_dict(self, el: ET.Element, prefix: str = "") -> dict:
        """Recursively flatten an XML element into a flat dict."""
        result = {}
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
        key = f"{prefix}.{tag}" if prefix else tag

        # attributes become columns
        for attr_name, attr_val in el.attrib.items():
            result[f"{key}.{attr_name}"] = attr_val

        children = list(el)
        if not children:
            result[key] = el.text.strip() if el.text and el.text.strip() else None
        else:
            for child in children:
                result.update(self._element_to_dict(child, prefix=key))

        return result
