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

