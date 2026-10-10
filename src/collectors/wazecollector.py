
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
from collectors.basecollector import BaseCollector

logger = logging.getLogger(__name__)

EARTH_RADIUS_KM = 6371.0



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

