"""Splunk HTTP Event Collector client with batching and failure spooling."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from models import FuelStationData

logger = logging.getLogger(__name__)


class HECTransientError(Exception):
    """Splunk unreachable or temporarily unavailable - safe to retry later."""


class HECRejectedError(Exception):
    """Splunk rejected the payload (bad token, bad data) - retrying won't help."""


class SplunkHECClient:
    """Sends events to HEC in batches.

    Batches that fail for transient reasons (connection errors, 5xx) are
    spooled to a JSON-lines file and replayed on the next ``send_events``.
    """

    def __init__(
        self,
        hec_url: str,
        hec_token: str,
        batch_size: int = 100,
        index: Optional[str] = None,
        verify_ssl: bool = False,
        timeout: float = 10.0,
        max_retries: int = 3,
        spool_path: Optional[str] = "spool/failed_hec.jsonl",
        max_spooled_batches: int = 1000,
    ) -> None:
        self._hec_url = hec_url
        self._hec_token = hec_token
        self._batch_size = max(1, int(batch_size))
        self._index = index
        self._verify_ssl = verify_ssl
        self._timeout = timeout
        self._spool_path = spool_path
        self._max_spooled = max_spooled_batches
        self._session = self._build_session(max_retries)
        if not verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        if spool_path:
            os.makedirs(os.path.dirname(spool_path) or ".", exist_ok=True)

    # ------------------------------------------------------------------ #
    def send_events(self, events: List[FuelStationData]) -> bool:
        """Send all events (plus any spooled backlog). True if everything landed."""
        all_ok = self._replay_spool()
        for start in range(0, len(events), self._batch_size):
            batch = events[start : start + self._batch_size]
            payload = self._format_payload(batch)
            try:
                self._post_to_hec(payload)
                logger.info("sent batch of %d events to Splunk", payload["count"])
            except HECTransientError as exc:
                logger.error("HEC unavailable, spooling batch: %s", exc)
                self._handle_failed_requests(payload)
                all_ok = False
            except HECRejectedError as exc:
                logger.error("HEC rejected batch, discarding: %s", exc)
                all_ok = False
        return all_ok

    # ------------------------------------------------------------------ #
    def _format_payload(self, events: List[FuelStationData]) -> Dict[str, Any]:
        """HEC accepts concatenated JSON envelopes in the request body."""
        body = "\n".join(e.to_splunk_json(index=self._index) for e in events)
        return {"data": body, "count": len(events)}

    def _post_to_hec(self, payload: Dict[str, Any]) -> None:
        try:
            response = self._session.post(
                self._hec_url,
                data=payload["data"].encode("utf-8"),
                headers={
                    "Authorization": f"Splunk {self._hec_token}",
                    "Content-Type": "application/json",
                },
                timeout=self._timeout,
                verify=self._verify_ssl,
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            raise HECTransientError(str(exc)) from exc
        except requests.RequestException as exc:
            raise HECTransientError(str(exc)) from exc

        if response.status_code == 200:
            return
        detail = f"status={response.status_code} body={response.text[:200]}"
        if response.status_code >= 500 or response.status_code in (408, 429):
            raise HECTransientError(detail)
        raise HECRejectedError(detail)

    def _handle_failed_requests(self, payload: Dict[str, Any]) -> None:
        """Persist a failed batch so it can be replayed later."""
        if not self._spool_path:
            logger.warning("no spool configured; dropping %d events", payload["count"])
            return
        try:
            with open(self._spool_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except OSError as exc:
            logger.error("could not write spool file: %s", exc)

    # ------------------------------------------------------------------ #
    def _replay_spool(self) -> bool:
        """Retry spooled batches; keep the ones that still fail."""
        if not self._spool_path or not os.path.exists(self._spool_path):
            return True
        try:
            with open(self._spool_path, "r", encoding="utf-8") as fh:
                lines = [ln for ln in fh.read().splitlines() if ln.strip()]
        except OSError as exc:
            logger.error("could not read spool file: %s", exc)
            return False
        if not lines:
            return True

        lines = lines[-self._max_spooled :]  # drop oldest beyond the cap
        logger.info("replaying %d spooled batches", len(lines))
        remaining: List[str] = []
        for i, line in enumerate(lines):
            try:
                self._post_to_hec(json.loads(line))
            except HECTransientError:
                remaining.extend(lines[i:])  # Splunk still down; stop trying
                break
            except (HECRejectedError, json.JSONDecodeError) as exc:
                logger.error("discarding bad spooled batch: %s", exc)

        try:
            if remaining:
                with open(self._spool_path, "w", encoding="utf-8") as fh:
                    fh.write("\n".join(remaining) + "\n")
            else:
                os.remove(self._spool_path)
        except OSError as exc:
            logger.error("could not update spool file: %s", exc)
        return not remaining

    @staticmethod
    def _build_session(max_retries: int) -> requests.Session:
        retry = Retry(
            total=max_retries,
            backoff_factor=0.5,
            status_forcelist=(502, 503, 504),
            allowed_methods=frozenset({"POST"}),
        )
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
