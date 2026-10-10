"""Normalization pipeline applied to the merged output of all collectors."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from models import FuelStationData

logger = logging.getLogger(__name__)


class DataNormalizer:
    """Cleans a list of ``FuelStationData`` before it is sent to Splunk.

    Pipeline: enrich coordinates -> validate prices -> remove duplicates.
    Prices are expected in the local currency per litre (default sanity
    window 3.0-15.0, configurable).
    """

    def __init__(self, min_price: float = 3.0, max_price: float = 15.0) -> None:
        self._min_price = min_price
        self._max_price = max_price

    def normalize_data(self, stations: List[FuelStationData]) -> List[FuelStationData]:
        before = len(stations)
        self._enrich_missing_coordinates(stations)
        stations = self._validate_prices(stations)
        stations = self._remove_duplicates(stations)
        logger.info("normalized %d -> %d records", before, len(stations))
        return stations

    # ------------------------------------------------------------------ #
    def _remove_duplicates(self, stations: List[FuelStationData]) -> List[FuelStationData]:
        """Keep one record per station_id: the one with the newest timestamp."""
        latest: Dict[str, FuelStationData] = {}
        for s in stations:
            current = latest.get(s.station_id)
            if current is None or self._ts(s) >= self._ts(current):
                latest[s.station_id] = s
        return list(latest.values())

    def _validate_prices(self, stations: List[FuelStationData]) -> List[FuelStationData]:
        """Null out-of-range prices; drop records left with no valid price."""
        valid: List[FuelStationData] = []
        for s in stations:
            s.price_95 = self._clean_price(s.price_95)
            s.price_diesel = self._clean_price(s.price_diesel)
            if s.price_95 is None and s.price_diesel is None:
                logger.debug("dropping %s: no valid prices", s.station_id)
                continue
            valid.append(s)
        return valid

    def _enrich_missing_coordinates(self, stations: List[FuelStationData]) -> None:
        """Fill missing lat/lon from another record of the same station (in place).

        Hook: replace/extend with a geocoding lookup by name if needed.
        """
        known: Dict[str, Tuple[float, float]] = {
            s.station_id: (s.lat, s.lon)
            for s in stations
            if s.lat is not None and s.lon is not None
        }
        for s in stations:
            if (s.lat is None or s.lon is None) and s.station_id in known:
                s.lat, s.lon = known[s.station_id]

    # ------------------------------------------------------------------ #
    def _clean_price(self, price: Optional[float]) -> Optional[float]:
        if price is None:
            return None
        if self._min_price <= price <= self._max_price:
            return round(price, 3)
        return None

    @staticmethod
    def _ts(s: FuelStationData):
        return s._aware_timestamp()
