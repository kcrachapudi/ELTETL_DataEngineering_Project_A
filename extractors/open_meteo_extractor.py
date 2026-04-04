"""
Open Meteo Extractor — free weather API, no key required.

This is the primary data source for Project 1.
Pulls hourly weather data for any location on earth.
Pairs with JSONParser to produce a clean DataFrame.

API docs: https://open-meteo.com/en/docs

Usage:
    # Dallas, TX — default
    ext = OpenMeteoExtractor()
    raw = ext.fetch()
    df  = JSONParser().parse(raw)

    # Custom location + variables
    ext = OpenMeteoExtractor(
        latitude=40.7128,
        longitude=-74.0060,
        hourly=["temperature_2m", "precipitation", "windspeed_10m"],
        days_back=7,
    )
    df = JSONParser().parse(ext.fetch())

Output columns (depends on variables requested):
    time, temperature_2m, precipitation, windspeed_10m,
    weathercode, relativehumidity_2m, etc.
"""

import json
import logging
from datetime import date, timedelta
from typing import Optional

from .http_extractor import HTTPExtractor
from .base_extractor import BaseExtractor, ExtractorError

logger = logging.getLogger(__name__)

BASE_URL = "https://api.open-meteo.com/v1/forecast"

DEFAULT_HOURLY = [
    "temperature_2m",
    "relativehumidity_2m",
    "precipitation",
    "weathercode",
    "windspeed_10m",
    "winddirection_10m",
    "apparent_temperature",
    "surface_pressure",
]


class OpenMeteoExtractor(BaseExtractor):
    """
    Fetches hourly weather forecast + historical data from Open Meteo.
    Free, no API key, no rate limit for reasonable use.
    """

    def __init__(
        self,
        latitude:    float          = 32.7767,   # Dallas, TX
        longitude:   float          = -96.7970,
        hourly:      list[str]      = None,
        days_back:   int            = 7,
        days_forward: int           = 3,
        timezone:    str            = "America/Chicago",
        temperature_unit: str       = "fahrenheit",
    ):
        self._lat     = latitude
        self._lon     = longitude
        self._hourly  = hourly or DEFAULT_HOURLY
        self._back    = days_back
        self._forward = days_forward
        self._tz      = timezone
        self._temp_unit = temperature_unit

    @property
    def source_name(self) -> str:
        return f"Open Meteo ({self._lat},{self._lon})"

    def fetch(self) -> str:
        """
        Fetch hourly weather data.
        Returns JSON string — pass directly to JSONParser().parse().
        """
        start = (date.today() - timedelta(days=self._back)).isoformat()
        end   = (date.today() + timedelta(days=self._forward)).isoformat()

        params = {
            "latitude":         self._lat,
            "longitude":        self._lon,
            "hourly":           ",".join(self._hourly),
            "start_date":       start,
            "end_date":         end,
            "timezone":         self._tz,
            "temperature_unit": self._temp_unit,
        }

        logger.info(
            f"Fetching Open Meteo: lat={self._lat} lon={self._lon} "
            f"{start} → {end} variables={self._hourly}"
        )

        ext = HTTPExtractor(BASE_URL, params=params, timeout=15)
        raw = ext.fetch()

        # Flatten the response: {hourly: {time: [...], temp: [...]}}
        # into [{time: t, temp: v}, ...] for the JSON parser
        data    = json.loads(raw)
        hourly  = data.get("hourly", {})
        times   = hourly.get("time", [])

        if not times:
            raise ExtractorError("Open Meteo returned no hourly data.")

        records = []
        for i, ts in enumerate(times):
            row = {
                "time":      ts,
                "latitude":  self._lat,
                "longitude": self._lon,
                "timezone":  self._tz,
            }
            for var in self._hourly:
                values = hourly.get(var, [])
                row[var] = values[i] if i < len(values) else None
            records.append(row)

        logger.info(f"Open Meteo: {len(records)} hourly records fetched")
        return json.dumps(records)
