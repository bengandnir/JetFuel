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
# Government open data (CSV)
# ====================================================================== #
class GovCollector(BaseCollector):
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
