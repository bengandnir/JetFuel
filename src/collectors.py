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
        session = requests.Session()
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


# ====================================================================== #
# Waze
# ====================================================================== #
class WazeCollector(BaseCollector):
    """Collects stations inside a bounding box around a centre point."""

    def __init__(
        self,
        api_url: str,
        center_lat: float,
        center_lon: float,
        radius_km: float = 10.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_name="waze", api_url=api_url, **kwargs)
        self._box_lat_min = 0.0
        self._box_lon_min = 0.0
        self._box_lat_max = 0.0
        self._box_lon_max = 0.0
        self._calculate_bounding_box(center_lat, center_lon, radius_km)

    def _calculate_bounding_box(
        self, center_lat: float, center_lon: float, radius: float
    ) -> Tuple[float, float, float, float]:
        """Compute (lat_min, lon_min, lat_max, lon_max) for ``radius`` km."""
        d_lat = math.degrees(radius / EARTH_RADIUS_KM)
        d_lon = math.degrees(
            radius / (EARTH_RADIUS_KM * max(math.cos(math.radians(center_lat)), 1e-6))
        )
        self._box_lat_min = center_lat - d_lat
        self._box_lat_max = center_lat + d_lat
        self._box_lon_min = center_lon - d_lon
        self._box_lon_max = center_lon + d_lon
        return (
            self._box_lat_min,
            self._box_lon_min,
            self._box_lat_max,
            self._box_lon_max,
        )

    def fetch_raw_data(self) -> Dict[str, Any]:
        # TODO(inject): replace params/headers with your reverse-engineered request.
        params = {
            "latMin": self._box_lat_min,
            "lonMin": self._box_lon_min,
            "latMax": self._box_lat_max,
            "lonMax": self._box_lon_max,
        }
        response = self._session.get(self._api_url, params=params, timeout=self._timeout)
        response.raise_for_status()
        return response.json()

    def parse_data(self, raw_data: Dict[str, Any]) -> List[FuelStationData]:
        stations: List[FuelStationData] = []
        # TODO(inject): adapt the key names below to the real response schema.
        for item in raw_data.get("stations", []):
            try:
                stations.append(
                    FuelStationData(
                        station_id=f"waze:{item['id']}",
                        name=str(item.get("name", "")),
                        brand=str(item.get("brand", "")),
                        price_95=self._to_float(item.get("price95")),
                        price_diesel=self._to_float(item.get("priceDiesel")),
                        lat=self._to_float(item.get("lat")),
                        lon=self._to_float(item.get("lon")),
                        timestamp=datetime.now(timezone.utc),
                    )
                )
            except KeyError as exc:
                logger.warning("[waze] skipping malformed item (missing %s)", exc)
        return stations


# ====================================================================== #
# Mobile fuel app API
# ====================================================================== #
class FuelAppAPICollector(BaseCollector):
    """Collector for a mobile-app backend that needs auth + device headers."""

    def __init__(
        self,
        api_url: str,
        device_id: str,
        auth_token: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(source_name="fuel_app", api_url=api_url, **kwargs)
        self._auth_token = auth_token
        self._device_id = device_id

    def _refresh_auth_token(self) -> None:
        """Obtain a new token and store it in ``self._auth_token``."""
        # TODO(inject): call your auth/refresh endpoint here.
        raise NotImplementedError("refresh_auth_token is not implemented yet")

    def _simulate_mobile_headers(self) -> Dict[str, str]:
        """Headers that mimic the official mobile client."""
        # TODO(inject): set the real User-Agent / app-version / signature headers.
        return {
            "User-Agent": "FuelApp/0.0.0 (placeholder)",
            "X-Device-Id": self._device_id,
            "Authorization": f"Bearer {self._auth_token}",
            "Accept": "application/json",
        }

    def fetch_raw_data(self) -> Dict[str, Any]:
        response = self._session.get(
            self._api_url, headers=self._simulate_mobile_headers(), timeout=self._timeout
        )
        if response.status_code == 401:
            logger.info("[fuel_app] token rejected, refreshing and retrying once")
            self._refresh_auth_token()
            response = self._session.get(
                self._api_url,
                headers=self._simulate_mobile_headers(),
                timeout=self._timeout,
            )
        response.raise_for_status()
        return response.json()

    def parse_data(self, raw_data: Dict[str, Any]) -> List[FuelStationData]:
        stations: List[FuelStationData] = []
        # TODO(inject): adapt the key names below to the real response schema.
        for item in raw_data.get("data", []):
            try:
                stations.append(
                    FuelStationData(
                        station_id=f"app:{item['station_id']}",
                        name=str(item.get("name", "")),
                        brand=str(item.get("brand", "")),
                        price_95=self._to_float(item.get("gasoline_95")),
                        price_diesel=self._to_float(item.get("diesel")),
                        lat=self._to_float(item.get("latitude")),
                        lon=self._to_float(item.get("longitude")),
                        timestamp=datetime.now(timezone.utc),
                    )
                )
            except KeyError as exc:
                logger.warning("[fuel_app] skipping malformed item (missing %s)", exc)
        return stations


# ====================================================================== #
# Government open data (CSV)
# ====================================================================== #
class GovDataScraper(BaseCollector):
    """Downloads a CSV published on a government open-data portal."""

    def __init__(self, data_gov_url: str, **kwargs: Any) -> None:
        super().__init__(source_name="gov_data", api_url=data_gov_url, **kwargs)
        self._data_gov_url = data_gov_url

    def _extract_csv_from_url(self) -> List[Dict[str, str]]:
        """Download the CSV and return its rows as dicts."""
        response = self._session.get(self._data_gov_url, timeout=self._timeout)
        response.raise_for_status()
        text = response.content.decode("utf-8-sig", errors="replace")
        return list(csv.DictReader(io.StringIO(text)))

    def fetch_raw_data(self) -> Dict[str, Any]:
        return {"rows": self._extract_csv_from_url()}

    def parse_data(self, raw_data: Dict[str, Any]) -> List[FuelStationData]:
        stations: List[FuelStationData] = []
        # TODO(inject): map the real CSV column names below.
        for row in raw_data.get("rows", []):
            station_id = row.get("station_id")
            if not station_id:
                continue
            stations.append(
                FuelStationData(
                    station_id=f"gov:{station_id}",
                    name=row.get("name", ""),
                    brand=row.get("brand", ""),
                    price_95=self._to_float(row.get("price_95")),
                    price_diesel=self._to_float(row.get("price_diesel")),
                    lat=self._to_float(row.get("lat")),
                    lon=self._to_float(row.get("lon")),
                    timestamp=datetime.now(timezone.utc),
                )
            )
        return stations
