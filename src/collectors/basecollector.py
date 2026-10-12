"""Data collectors.

Each collector implements the template method ``BaseCollector.execute``:

    fetch_raw_data()  ->  parse_data(raw)  ->  List[FuelStationData]

Any exception is routed through ``handle_errors`` so that one failing source
never takes down the collection cycle. Places where you inject your own
reverse-engineered request/response logic are marked ``# TODO(inject)``.
"""
from __future__ import annotations

import csv
import io
import logging
import math
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import cloudscraper
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from models import FuelStationData

logger = logging.getLogger(__name__)

EARTH_RADIUS_KM = 6371.0


class BaseCollector(ABC):
    """Abstract collector: subclasses implement fetch + parse."""

    def __init__(
        self,
        source_name: str,
        api_url: str,
        timeout: float = 15.0,
        max_retries: int = 3,
    ) -> None:
        self._source_name = source_name
        self._api_url = api_url
        self._timeout = timeout
        self._session = self._build_session(max_retries)

    # -- properties kept read-only for the controller / logging ---------- #
    @property
    def source_name(self) -> str:
        return self._source_name

    @property
    def api_url(self) -> str:
        return self._api_url

    # -- template method -------------------------------------------------- #
    def execute(self) -> List[FuelStationData]:
        """Run one collection pass. Never raises; returns [] on failure."""
        try:
            raw = self.fetch_raw_data()
            stations = self.parse_data(raw)
            logger.info("[%s] collected %d stations", self._source_name, len(stations))
            return stations
        except Exception as exc:  # noqa: BLE001 - isolate source failures
            self.handle_errors(exc)
            return []

    # -- abstract steps --------------------------------------------------- #
    @abstractmethod
    def fetch_raw_data(self) -> Dict[str, Any]:
        """Fetch the raw payload from the source."""

    @abstractmethod
    def parse_data(self, raw_data: Dict[str, Any]) -> List[FuelStationData]:
        """Convert the raw payload into ``FuelStationData`` objects."""

    # -- shared behaviour ------------------------------------------------- #
    def handle_errors(self, exc: Optional[BaseException] = None) -> None:
        """Central error logging. Override for source-specific recovery."""
        if isinstance(exc, requests.HTTPError):
            status = exc.response.status_code if exc.response is not None else "?"
            logger.error("[%s] HTTP error (status=%s): %s", self._source_name, status, exc)
        elif isinstance(exc, (requests.ConnectionError, requests.Timeout)):
            logger.error("[%s] network error: %s", self._source_name, exc)
        else:
            logger.exception("[%s] unexpected error: %s", self._source_name, exc)

    @staticmethod
    def _build_session(max_retries: int) -> requests.Session:
        retry = Retry(
            total=max_retries,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
        )
        session = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        """Lenient float conversion ('7.12', '7,12', 7 -> float; junk -> None)."""
        if value is None or value == "":
            return None
        try:
            return float(str(value).strip().replace(",", "."))
        except (TypeError, ValueError):
            return None



