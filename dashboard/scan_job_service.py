from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.main import build_runner
from config.settings import AppSettings


@dataclass
class ScanJobService:
    """Runs scanner jobs in a single background thread with live status."""

    settings: AppSettings
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _thread: threading.Thread | None = field(default=None, init=False)
    _status: str = field(default="idle", init=False)
    _phase: str = field(default="idle", init=False)
    _progress_current: int = field(default=0, init=False)
    _progress_total: int = field(default=0, init=False)
    _current_ticker: str | None = field(default=None, init=False)
    _last_run_id: str | None = field(default=None, init=False)
    _error: str | None = field(default=None, init=False)
    _started_at: str | None = field(default=None, init=False)
    _finished_at: str | None = field(default=None, init=False)
    _logs: deque[dict[str, str]] = field(default_factory=lambda: deque(maxlen=300), init=False)

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

    def start(self) -> tuple[bool, str]:
        """Start an async scan job if none is currently running."""
        with self._lock:
            if self._status == "running" and self._thread is not None and self._thread.is_alive():
                return False, "Job is already running"
            self._status = "running"
            self._phase = "starting"
            self._progress_current = 0
            self._progress_total = 0
            self._current_ticker = None
            self._error = None
            self._started_at = datetime.now(UTC).isoformat()
            self._finished_at = None
            self._logs.clear()

            self._thread = threading.Thread(target=self._run, name="scanner-job", daemon=True)
            self._thread.start()
            return True, "Job started"

    def status(self) -> dict[str, Any]:
        """Return current state for UI polling."""
        with self._lock:
            return {
                "status": self._status,
                "phase": self._phase,
                "progress_current": self._progress_current,
                "progress_total": self._progress_total,
                "current_ticker": self._current_ticker,
                "last_run_id": self._last_run_id,
                "error": self._error,
                "started_at": self._started_at,
                "finished_at": self._finished_at,
                "is_running": self._status == "running",
                "logs": list(self._logs),
            }

    def _run(self) -> None:
        try:
            self._add_log("Scan job started")
            runner = build_runner(self.settings)
            runner.progress_callback = self._on_progress
            runner.log_callback = self._add_log
            runner.max_workers = max(1, self.settings.scan_workers)
            result = runner.run()
            with self._lock:
                self._status = "completed"
                self._phase = "done"
                self._last_run_id = result.run_id
                self._finished_at = datetime.now(UTC).isoformat()
            self._add_log(f"Scan completed successfully (run_id={result.run_id})")
        except Exception as exc:  # noqa: BLE001
            self._logger.exception("Background scan job failed")
            with self._lock:
                self._status = "failed"
                self._phase = "failed"
                self._error = str(exc)
                self._finished_at = datetime.now(UTC).isoformat()
            self._add_log(f"Scan failed: {exc}")

    def _on_progress(self, phase: str, current: int, total: int, ticker: str) -> None:
        with self._lock:
            self._phase = phase
            self._progress_current = current
            self._progress_total = total
            self._current_ticker = ticker
        if current == 1 or current == total or current % 25 == 0:
            self._add_log(f"{phase} {current}/{total} ({ticker})")

    def _add_log(self, message: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._logs.append({"time": now, "message": message})

