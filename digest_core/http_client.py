"""Polite, retry-aware HTTP transport for heterogeneous research sources."""

from __future__ import annotations

import datetime as dt
import email.utils
import threading
import time
import urllib.parse
from typing import Any

import requests


RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


def retry_after_seconds(value: str | None, *, now: dt.datetime | None = None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    current = now or dt.datetime.now(dt.timezone.utc)
    return max(0.0, (parsed - current).total_seconds())


class PoliteHttpClient:
    def __init__(self, settings) -> None:
        self.settings = settings
        self._local = threading.local()
        self._lock = threading.Lock()
        self._host_locks: dict[str, threading.Lock] = {}
        self._next_allowed: dict[str, float] = {}

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._local.session = session
        return session

    def _host_lock(self, host: str) -> threading.Lock:
        with self._lock:
            return self._host_locks.setdefault(host, threading.Lock())

    def _wait_for_host(self, host: str) -> None:
        lock = self._host_lock(host)
        with lock:
            wait_for = self._next_allowed.get(host, 0.0) - time.monotonic()
            if wait_for > 0:
                time.sleep(wait_for)
            self._next_allowed[host] = time.monotonic() + self.settings.min_host_interval

    def request(
        self,
        method: str,
        url: str,
        *,
        timeout: float | None = None,
        retries: int = 1,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        host = (urllib.parse.urlparse(url).hostname or "").lower()
        request_headers = {"User-Agent": self.settings.user_agent}
        request_headers.update(headers or {})
        last_error: Exception | None = None
        for attempt in range(max(0, retries) + 1):
            self._wait_for_host(host)
            try:
                response = self._session().request(
                    method,
                    url,
                    headers=request_headers,
                    timeout=timeout or self.settings.request_timeout,
                    verify=self.settings.verify_tls,
                    **kwargs,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= retries:
                    raise
                time.sleep(min(4.0, 0.75 * (2**attempt)))
                continue
            if response.status_code not in RETRYABLE_STATUS_CODES or attempt >= retries:
                return response
            delay = retry_after_seconds(response.headers.get("Retry-After"))
            if delay is None:
                delay = min(8.0, 1.0 * (2**attempt))
            time.sleep(min(30.0, max(0.25, delay)))
        if last_error:
            raise last_error
        raise RuntimeError("HTTP request failed without a response")

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)
