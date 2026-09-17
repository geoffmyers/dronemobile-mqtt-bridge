"""Phase A event-timeline streams + Phase C StatusPage client.

`IotLogsStream` and `AlertEventStream` mirror the Tractive bridge's
`EventStream` design — `seen_ids` set scoped to SEEN_CAP, paginate via
`next` cursor for startup backfill, single page-1 fetch for incremental
poll, callback-driven for publish flexibility.

`StatusPageClient` is a thin GET wrapper for the three Atlassian
StatusPage endpoints DroneMobile exposes (incidents/unresolved +
scheduled-maintenances/{active,upcoming}). No auth required.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Callable
from urllib.parse import urlparse, parse_qs

from ha_mqtt_bridge import request_with_backoff

from parsers import (
    AlertEvent,
    IotLog,
    StatusPageSnapshot,
    parse_alert_events,
    parse_iot_logs,
    parse_statuspage,
)


DM_BASE = "https://api.dronemobile.com/api/v1/"
STATUSPAGE_BASE = "https://status.dronemobile.com/api/v2/"

SEEN_CAP = 2000  # ~1 year of events at this account's volume (~1500 records/yr)


def _auth_headers(id_token: str) -> dict:
    return {"Authorization": f"Bearer {id_token}", "Accept": "application/json"}


def _get(url: str, headers: dict, params: dict | None = None, *, max_attempts: int = 4):
    """GET with exponential backoff on 429/5xx (shared toolkit helper —
    also used by main.py's own `_get`, so there is exactly one retry
    loop in this codebase). Returns parsed JSON or None on empty body.

    Raises `PermissionError` on 401 and `ha_mqtt_bridge.RetryExhaustedError`
    (a `RuntimeError` subclass) if every attempt is rate-limited/erroring —
    the caller's `except Exception` per-cycle guard in main.py turns that
    into a logged, backed-off retry next cycle instead of a crash.
    """
    r = request_with_backoff(
        "GET", url, headers=headers, params=params, timeout=20, max_attempts=max_attempts,
    )
    if r.status_code == 401:
        raise PermissionError(f"401 from {url}")
    r.raise_for_status()
    if not r.content:
        return None
    try:
        return r.json()
    except ValueError:
        return r.text


# ============================================================== IoT logs stream


class IotLogsStream:
    """Incremental + backfill poller for /api/v1/iot/logs."""

    def __init__(self, vehicle_id: str, device_key: str | None = None):
        self.vehicle_id = vehicle_id
        self.device_key = device_key
        self.seen_ids: set[int] = set()
        self._seen_order: list[int] = []

    def _remember(self, event_id: int) -> None:
        if event_id in self.seen_ids:
            return
        self.seen_ids.add(event_id)
        self._seen_order.append(event_id)
        if len(self._seen_order) > SEEN_CAP:
            drop = self._seen_order[: len(self._seen_order) - SEEN_CAP]
            for d in drop:
                self.seen_ids.discard(d)
            self._seen_order = self._seen_order[-SEEN_CAP:]

    def fetch_window(
        self, id_token: str, from_date: str, to_date: str, max_pages: int = 1,
    ) -> list[IotLog]:
        """Paginated fetch — walks `next` cursor up to `max_pages`."""
        params: dict = {
            "from_date": from_date, "to_date": to_date,
            "exclude_failures": "true", "limit": 100, "offset": 0, "sort": "desc",
        }
        url = f"{DM_BASE}iot/logs"
        all_records: list[IotLog] = []
        for _ in range(max_pages):
            body = _get(url, _auth_headers(id_token), params=params)
            records, next_url = parse_iot_logs(body or {})
            if not records:
                break
            all_records.extend(records)
            if not next_url:
                break
            parsed = urlparse(next_url)
            url = f"https://api.dronemobile.com{parsed.path}"
            params = {k: v[0] if len(v) == 1 else v
                      for k, v in parse_qs(parsed.query).items()}
        # Filter to this vehicle (the API doesn't take a vehicle_id filter —
        # logs are account-scoped — so we filter client-side).
        try:
            vid = int(self.vehicle_id)
        except (TypeError, ValueError):
            vid = None
        if vid is None:
            return all_records
        return [r for r in all_records if r.vehicle_id == vid]

    def backfill(
        self, id_token: str, days: int,
        on_event: Callable[[IotLog], None],
        log: logging.Logger,
    ) -> int:
        if days <= 0:
            return 0
        today = dt.date.today()
        from_d = (today - dt.timedelta(days=days)).isoformat() + "T00:00:00"
        to_d = today.isoformat() + "T23:59:59"
        # The API caps a single response at 100 records — bound max_pages so
        # a runaway query can't drain rate-limit budget. ~1500 records / 100
        # = 15 pages at most for a full year of history.
        max_pages = max(2, (days // 30) + 4)
        try:
            records = self.fetch_window(id_token, from_d, to_d, max_pages=max_pages)
        except Exception:
            log.exception("iot/logs backfill failed (non-fatal)")
            return 0
        # Sort oldest-first for chronological publish order.
        records.sort(key=lambda r: r.create_date or r.timestamp or "")
        for r in records:
            if r.id and r.id not in self.seen_ids:
                on_event(r)
                self._remember(r.id)
        log.info("iot/logs backfill: %d records over last %dd", len(records), days)
        return len(records)

    def poll_incremental(
        self, id_token: str,
        on_event: Callable[[IotLog], None],
        log: logging.Logger,
    ) -> int:
        # Fetch the most-recent 7-day window, page 1 only. Anything newer
        # than the newest seen_id gets emitted.
        today = dt.date.today()
        from_d = (today - dt.timedelta(days=7)).isoformat() + "T00:00:00"
        to_d = today.isoformat() + "T23:59:59"
        try:
            records = self.fetch_window(id_token, from_d, to_d, max_pages=1)
        except Exception:
            log.exception("iot/logs poll failed (non-fatal)")
            return 0
        new = [r for r in records if r.id and r.id not in self.seen_ids]
        new.sort(key=lambda r: r.create_date or r.timestamp or "")
        for r in new:
            on_event(r)
            self._remember(r.id)
        return len(new)


# ============================================================== Alert events


class AlertEventStream:
    """Incremental + backfill poller for /api/v1/alert/event."""

    def __init__(self, vehicle_id: str):
        self.vehicle_id = vehicle_id
        self.seen_ids: set[int] = set()
        self._seen_order: list[int] = []

    def _remember(self, event_id: int) -> None:
        if event_id in self.seen_ids:
            return
        self.seen_ids.add(event_id)
        self._seen_order.append(event_id)
        if len(self._seen_order) > SEEN_CAP:
            drop = self._seen_order[: len(self._seen_order) - SEEN_CAP]
            for d in drop:
                self.seen_ids.discard(d)
            self._seen_order = self._seen_order[-SEEN_CAP:]

    def fetch_window(
        self, id_token: str, from_date: str, to_date: str, max_pages: int = 1,
    ) -> list[AlertEvent]:
        params: dict = {
            "from_date": from_date, "to_date": to_date,
            "exclude_failures": "true", "limit": 100, "offset": 0, "sort": "desc",
        }
        url = f"{DM_BASE}alert/event"
        all_events: list[AlertEvent] = []
        for _ in range(max_pages):
            body = _get(url, _auth_headers(id_token), params=params)
            events, next_url = parse_alert_events(body or {})
            if not events:
                break
            all_events.extend(events)
            if not next_url:
                break
            parsed = urlparse(next_url)
            url = f"https://api.dronemobile.com{parsed.path}"
            params = {k: v[0] if len(v) == 1 else v
                      for k, v in parse_qs(parsed.query).items()}
        try:
            vid = int(self.vehicle_id)
        except (TypeError, ValueError):
            vid = None
        if vid is None:
            return all_events
        return [e for e in all_events if e.vehicle_id == vid]

    def backfill(
        self, id_token: str, days: int,
        on_event: Callable[[AlertEvent], None],
        log: logging.Logger,
    ) -> int:
        if days <= 0:
            return 0
        today = dt.date.today()
        from_d = (today - dt.timedelta(days=days)).isoformat() + "T00:00:00"
        to_d = today.isoformat() + "T23:59:59"
        max_pages = max(2, (days // 30) + 4)
        try:
            events = self.fetch_window(id_token, from_d, to_d, max_pages=max_pages)
        except Exception:
            log.exception("alert/event backfill failed (non-fatal)")
            return 0
        events.sort(key=lambda e: e.create_date or "")
        for e in events:
            if e.id and e.id not in self.seen_ids:
                on_event(e)
                self._remember(e.id)
        log.info("alert/event backfill: %d events over last %dd", len(events), days)
        return len(events)

    def poll_incremental(
        self, id_token: str,
        on_event: Callable[[AlertEvent], None],
        log: logging.Logger,
    ) -> int:
        today = dt.date.today()
        from_d = (today - dt.timedelta(days=7)).isoformat() + "T00:00:00"
        to_d = today.isoformat() + "T23:59:59"
        try:
            events = self.fetch_window(id_token, from_d, to_d, max_pages=1)
        except Exception:
            log.exception("alert/event poll failed (non-fatal)")
            return 0
        new = [e for e in events if e.id and e.id not in self.seen_ids]
        new.sort(key=lambda e: e.create_date or "")
        for e in new:
            on_event(e)
            self._remember(e.id)
        return len(new)


# ============================================================== StatusPage


def fetch_statuspage(timeout: float = 10.0) -> StatusPageSnapshot:
    """No-auth GET wrapper for status.dronemobile.com (Atlassian StatusPage).
    Returns a populated snapshot even when individual sub-fetches fail —
    a degraded StatusPage shouldn't break the bridge."""
    def _safe_get(path: str) -> dict:
        try:
            r = requests.get(f"{STATUSPAGE_BASE}{path}", timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception:
            return {}
    return parse_statuspage(
        incidents=_safe_get("incidents/unresolved.json"),
        active=_safe_get("scheduled-maintenances/active.json"),
        upcoming=_safe_get("scheduled-maintenances/upcoming.json"),
    )
