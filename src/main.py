"""Entry point: MainController wires collectors -> normalizer -> Splunk HEC."""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional

import schedule

from collectors import (
    BaseCollector,
    FuelAppAPICollector,
    GovDataScraper,
    WazeCollector,
)
from models import FuelStationData
from processing import DataNormalizer
from splunk_client import SplunkHECClient

logger = logging.getLogger("fuel_worker")

DEFAULT_CONFIG: Dict[str, Any] = {
    "interval_minutes": 5,
    "splunk": {
        "hec_url": "https://splunk:8088/services/collector/event",
        "hec_token": "11111111-2222-3333-4444-555555555555",
        "index": "main",
        "batch_size": 100,
        "verify_ssl": False,
    },
    "collectors": {
        # A collector is only started when its URL is set.
        "waze": {"api_url": "", "center_lat": 32.0853, "center_lon": 34.7818, "radius_km": 10.0},
        "fuel_app": {"api_url": "", "device_id": "", "auth_token": ""},
        "gov_data": {"data_gov_url": ""},
    },
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _env_bool(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "on")


class MainController:
    """Orchestrates the whole pipeline and the scheduled loop."""

    def __init__(self, config_path: str = "config.json") -> None:
        self._config_path = config_path
        self._collectors: List[BaseCollector] = []
        self._splunk_client: Optional[SplunkHECClient] = None
        self._normalizer = DataNormalizer()
        self._config: Dict[str, Any] = {}
        self._running = True

    # ------------------------------------------------------------------ #
    def init_system(self) -> None:
        """Load config (defaults < JSON file < env vars) and build components."""
        self._config = self._load_config()
        spl = self._config["splunk"]
        if not spl["hec_token"]:
            raise RuntimeError("Splunk HEC token missing (set SPLUNK_HEC_TOKEN)")

        self._splunk_client = SplunkHECClient(
            hec_url=spl["hec_url"],
            hec_token=spl["hec_token"],
            batch_size=int(spl["batch_size"]),
            index=spl["index"] or None,
            verify_ssl=bool(spl["verify_ssl"]),
            spool_path=os.getenv("HEC_SPOOL_PATH", "spool/failed_hec.jsonl"),
        )

        cols = self._config["collectors"]
        if cols["waze"]["api_url"]:
            self._collectors.append(WazeCollector(**cols["waze"]))
        if cols["fuel_app"]["api_url"]:
            self._collectors.append(FuelAppAPICollector(**cols["fuel_app"]))
        if cols["gov_data"]["data_gov_url"]:
            self._collectors.append(GovDataScraper(**cols["gov_data"]))

        if not self._collectors:
            logger.warning("no collectors configured - set WAZE_API_URL / FUEL_APP_API_URL / GOV_DATA_URL")
        logger.info(
            "system initialised: %d collectors (%s)",
            len(self._collectors),
            ", ".join(c.source_name for c in self._collectors) or "none",
        )

    def run_collection_cycle(self) -> None:
        """One full pass: collect -> normalize -> send. Never raises."""
        if self._splunk_client is None:
            raise RuntimeError("init_system() must be called first")
        started = time.monotonic()
        collected: List[FuelStationData] = []
        for collector in self._collectors:
            collected.extend(collector.execute())  # execute() isolates its own errors

        try:
            normalized = self._normalizer.normalize_data(collected)
            ok = self._splunk_client.send_events(normalized)
            logger.info(
                "cycle done in %.1fs: %d collected, %d sent, all_ok=%s",
                time.monotonic() - started, len(collected), len(normalized), ok,
            )
        except Exception:  # noqa: BLE001 - keep the scheduler alive
            logger.exception("collection cycle failed")

    def schedule_jobs(self, interval_minutes: Optional[int] = None) -> None:
        """Run one cycle immediately, then every ``interval_minutes`` until stopped."""
        interval = interval_minutes or int(self._config["interval_minutes"])
        schedule.every(interval).minutes.do(self.run_collection_cycle)
        logger.info("scheduled collection every %d minutes", interval)

        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT, self._stop)

        self.run_collection_cycle()
        while self._running:
            schedule.run_pending()
            time.sleep(1)
        logger.info("worker stopped")

    # ------------------------------------------------------------------ #
    def _stop(self, *_: Any) -> None:
        logger.info("shutdown signal received")
        self._running = False

    def _load_config(self) -> Dict[str, Any]:
        config = DEFAULT_CONFIG
        if os.path.exists(self._config_path):
            with open(self._config_path, "r", encoding="utf-8") as fh:
                config = _deep_merge(config, json.load(fh))
            logger.info("loaded config file %s", self._config_path)

        env = os.environ
        spl, cols = config["splunk"], config["collectors"]
        if env.get("SPLUNK_HEC_URL"):
            spl["hec_url"] = env["SPLUNK_HEC_URL"]
        if env.get("SPLUNK_HEC_TOKEN"):
            spl["hec_token"] = env["SPLUNK_HEC_TOKEN"]
        if env.get("SPLUNK_INDEX"):
            spl["index"] = env["SPLUNK_INDEX"]
        if env.get("SPLUNK_HEC_BATCH_SIZE"):
            spl["batch_size"] = int(env["SPLUNK_HEC_BATCH_SIZE"])
        if env.get("SPLUNK_HEC_VERIFY_SSL"):
            spl["verify_ssl"] = _env_bool(env["SPLUNK_HEC_VERIFY_SSL"])
        if env.get("COLLECTION_INTERVAL_MINUTES"):
            config["interval_minutes"] = int(env["COLLECTION_INTERVAL_MINUTES"])
        if env.get("WAZE_API_URL"):
            cols["waze"]["api_url"] = env["WAZE_API_URL"]
        if env.get("FUEL_APP_API_URL"):
            cols["fuel_app"]["api_url"] = env["FUEL_APP_API_URL"]
        if env.get("FUEL_APP_DEVICE_ID"):
            cols["fuel_app"]["device_id"] = env["FUEL_APP_DEVICE_ID"]
        if env.get("FUEL_APP_AUTH_TOKEN"):
            cols["fuel_app"]["auth_token"] = env["FUEL_APP_AUTH_TOKEN"]
        if env.get("GOV_DATA_URL"):
            cols["gov_data"]["data_gov_url"] = env["GOV_DATA_URL"]
        return config


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    controller = MainController(config_path=os.getenv("CONFIG_PATH", "config.json"))
    try:
        controller.init_system()
    except Exception:
        logger.exception("failed to initialise system")
        return 1
    controller.schedule_jobs()
    return 0


if __name__ == "__main__":
    sys.exit(main())
