from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

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
    _cache_only: bool = field(default=False, init=False)
    _selected_phase: str | None = field(default=None, init=False)
    _decision_message: str = field(default="", init=False)
    _phase_fetch_log: dict[str, set[str]] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._market_tz = ZoneInfo("America/New_York")

    def start(self, cache_only: bool = False) -> tuple[bool, str]:
        """Start an async scan job if none is currently running."""
        with self._lock:
            if self._status == "running" and self._thread is not None and self._thread.is_alive():
                return False, "Job is already running"
            decided_cache_only, selected_phase, decision_message = self._decide_cache_mode(manual_cache_only=cache_only)
            self._status = "running"
            self._phase = "starting"
            self._progress_current = 0
            self._progress_total = 0
            self._current_ticker = None
            self._error = None
            self._started_at = datetime.now(UTC).isoformat()
            self._finished_at = None
            self._cache_only = decided_cache_only
            self._selected_phase = selected_phase
            self._decision_message = decision_message
            self._logs.clear()

            self._thread = threading.Thread(target=self._run, name="scanner-job", daemon=True)
            self._thread.start()
            return True, decision_message

    def status(self) -> dict[str, Any]:
        """Return current state for UI polling."""
        now_et = datetime.now(self._market_tz)
        current_phase = self._current_fetch_phase(now_et)
        today_key = now_et.date().isoformat()
        completed = sorted(self._phase_fetch_log.get(today_key, set()))
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
                "cache_only": self._cache_only,
                "selected_fetch_phase": self._selected_phase,
                "decision_message": self._decision_message,
                "market_open": self._is_market_open(now_et),
                "current_fetch_phase": current_phase,
                "completed_fetch_phases_today": completed,
                "logs": list(self._logs),
            }

    def _run(self) -> None:
        try:
            self._add_log("Scan job started")
            if self._cache_only:
                self._add_log("Cache-only mode enabled")
            if self._decision_message:
                self._add_log(self._decision_message)
            runner = build_runner(self.settings)
            runner.progress_callback = self._on_progress
            runner.log_callback = self._add_log
            runner.max_workers = max(1, self.settings.scan_workers)
            result = runner.run(use_cache_only=self._cache_only)
            if not self._cache_only and self._selected_phase is not None:
                now_et = datetime.now(self._market_tz)
                today_key = now_et.date().isoformat()
                with self._lock:
                    self._phase_fetch_log.setdefault(today_key, set()).add(self._selected_phase)
                self._add_log(f"Marked fetch phase completed: {self._selected_phase}")
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

    def _decide_cache_mode(self, manual_cache_only: bool) -> tuple[bool, str | None, str]:
        now_et = datetime.now(self._market_tz)
        if manual_cache_only:
            return True, None, "Manual cache-only mode selected"
        if not self._is_market_open(now_et):
            return True, None, "Market is closed. Running cache-only (no remote fetch)."
        phase = self._current_fetch_phase(now_et)
        if phase is None:
            return True, None, "Outside scheduled fetch windows. Running cache-only."
        today_key = now_et.date().isoformat()
        completed = self._phase_fetch_log.get(today_key, set())
        if phase in completed:
            return True, phase, f"Fetch window {phase} already completed today. Running cache-only."
        return False, phase, f"Fetch window {phase} is active and missing today. Running with remote fetch."

    def _current_fetch_phase(self, now_et: datetime) -> str | None:
        t = now_et.time()
        phase_windows = {
            "OPEN_20M": (time(hour=9, minute=50), time(hour=10, minute=40)),
            "PLUS_3H": (time(hour=12, minute=30), time(hour=13, minute=20)),
            "LAST_40M": (time(hour=15, minute=20), time(hour=16, minute=0)),
        }
        for phase, (start_t, end_t) in phase_windows.items():
            if start_t <= t <= end_t:
                return phase
        return None

    @staticmethod
    def _is_market_open(now_et: datetime) -> bool:
        if now_et.weekday() >= 5:
            return False
        return time(hour=9, minute=30) <= now_et.time() <= time(hour=16, minute=0)
