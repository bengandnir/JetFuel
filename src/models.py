"""Domain model for the fuel price ingestion worker.

``FuelStationData`` is the single normalized record that flows through the
whole pipeline: collectors create it, ``DataNormalizer`` cleans it and
``SplunkHECClient`` ships it.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class FuelStationData:
    """A single price observation for one gas station."""

    station_id: str
    name: str
    brand: str
    price_95: Optional[float] = None
    price_diesel: Optional[float] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    timestamp: datetime = field(default_factory=_utcnow)

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict (timestamp rendered as ISO-8601)."""
        data = asdict(self)
        data["timestamp"] = self._aware_timestamp().isoformat()
        return data

    def to_splunk_json(
        self,
        index: Optional[str] = None,
        sourcetype: str = "fuel:station:price",
        source: str = "fuel-price-worker",
        host: Optional[str] = None,
    ) -> str:
        """Serialize as a single HEC event envelope (JSON string).

        The envelope's ``time`` field is the observation timestamp in epoch
        seconds, so Splunk indexes the event at the moment the price was
        observed rather than when it was received.
        """
        envelope: Dict[str, Any] = {
            "time": round(self._aware_timestamp().timestamp(), 3),
            "sourcetype": sourcetype,
            "source": source,
            "event": self.to_dict(),
        }
        if index:
            envelope["index"] = index
        if host:
            envelope["host"] = host
        return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _aware_timestamp(self) -> datetime:
        """Naive timestamps are assumed to be UTC."""
        if self.timestamp.tzinfo is None:
            return self.timestamp.replace(tzinfo=timezone.utc)
        return self.timestamp
