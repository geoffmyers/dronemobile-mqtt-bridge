"""Pure-data layer for the DroneMobile bridge.

Holds dataclasses, parse_* helpers, and the small derivation helpers
(`_is_moving`, `_compass_direction`, `_signal_quality`, `_humanize_carrier`).
No network I/O, no MQTT, no env vars — keeps parsing trivially testable
from fixture payloads.

Endpoint shape references (live-probed 2026-05-26 against the API):
  - GET /api/v1/vehicle/{id}    — full vehicle telemetry blob (the
                                   bridge's original endpoint)
  - GET /api/v1/iot/logs        — paginated device-message history
  - GET /api/v1/alert/event     — paginated alert history
  - GET /api/v1/device          — device + plan info
  - GET /api/v1/alert/rule      — configured alert rules
  - GET /api/v1/geofence        — configured geofences
  - GET /api/v1/user            — account users + permissions
  - GET /api/v1/firmware-update — firmware version list
  - GET status.dronemobile.com  — StatusPage incidents + maintenance
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


# ---------------------- Vehicle (existing — kept verbatim) ----------------------


@dataclass
class Vehicle:
    vehicle_id: str
    name: str
    make: str
    model: str
    year: int
    vin: str


def parse_vehicle(payload: dict) -> Vehicle:
    return Vehicle(
        vehicle_id=str(payload.get("id")),
        name=payload.get("vehicle_name") or "DroneMobile Vehicle",
        make=payload.get("vehicle_make") or "",
        model=payload.get("vehicle_model") or "",
        year=int(payload.get("vehicle_year") or 0),
        vin=payload.get("vin") or "",
    )


# ---------------------- Derivation helpers ----------------------


def _get_in(d: dict, path: str) -> Any:
    cur: Any = d
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _bool_onoff(v: Any) -> str | None:
    if v is None:
        return None
    return "ON" if v else "OFF"


def _is_moving(payload: dict) -> str | None:
    speed = _get_in(payload, "last_known_state.speed")
    if speed is None:
        return None
    try:
        return "ON" if float(speed) > 0.0 else "OFF"
    except (TypeError, ValueError):
        return None


def _str_or_none(v: Any) -> str | None:
    if v is None or v == "":
        return None
    return str(v)


# Known brand / carrier names that don't follow Title Case rules. Case-insensitive
# lookup; the value side is the canonical capitalisation.
_BRAND_OVERRIDES = {
    "at&t": "AT&T",
    "t-mobile": "T-Mobile",
    "verizon": "Verizon",
    "sprint": "Sprint",
    "us cellular": "US Cellular",
}


def _humanize_carrier(v: Any) -> str | None:
    """Carrier-name normaliser. `at&t` → `AT&T`; otherwise Title Case."""
    if v is None or v == "":
        return None
    lv = str(v).strip().lower()
    if lv in _BRAND_OVERRIDES:
        return _BRAND_OVERRIDES[lv]
    return str(v).title()


_COMPASS_OCTANTS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def _compass_direction(payload: dict) -> str | None:
    """Convert `last_known_state.gps_degree` (0-360 °) to an 8-way octant."""
    raw = _get_in(payload, "last_known_state.gps_degree")
    if raw is None:
        return None
    try:
        deg = float(raw) % 360
    except (TypeError, ValueError):
        return None
    idx = int((deg + 22.5) // 45) % 8
    return _COMPASS_OCTANTS[idx]


def _signal_quality(payload: dict) -> str | None:
    """Map `last_known_state.cellular_signal_strength` (AT+CSQ 0-31 scale,
    higher is better) to an Excellent/Good/Fair/Poor bucket."""
    raw = _get_in(payload, "last_known_state.cellular_signal_strength")
    if raw is None:
        return None
    try:
        r = int(raw)
    except (TypeError, ValueError):
        return None
    if r >= 24:
        return "Excellent"
    if r >= 19:
        return "Good"
    if r >= 14:
        return "Fair"
    return "Poor"


def _slugify(s: str) -> str:
    s = re.sub(r"[^\w\s-]", "", s.lower())
    s = re.sub(r"[\s_-]+", "_", s).strip("_")
    return s or "unnamed"


# ---------------------- Geofence membership ----------------------


def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters between two lat/lon points."""
    from math import asin, cos, radians, sin, sqrt
    R = 6_371_000  # mean Earth radius (m)
    lat1, lon1, lat2, lon2 = map(radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * R * asin(sqrt(a))


def is_inside_geofence(payload: dict, geofence: dict) -> str | None:
    """ON if the vehicle's current position is within the geofence radius.

    DroneMobile geofences store radius in **miles** (per HAR — e.g., a
    "Home" geofence with radius=1.0 and the polygon centered on the house).
    Returns None when either side of the comparison is missing.
    """
    lat = _get_in(payload, "last_known_state.latitude")
    lon = _get_in(payload, "last_known_state.longitude")
    if lat is None or lon is None:
        return None
    coords = geofence.get("coordinates") or []
    if len(coords) < 2:
        return None
    radius_mi = geofence.get("radius")
    if radius_mi is None:
        return None
    try:
        radius_m = float(radius_mi) * 1609.344
        d = _haversine_meters(float(lat), float(lon), float(coords[0]), float(coords[1]))
    except (TypeError, ValueError):
        return None
    return "ON" if d <= radius_m else "OFF"


# ---------------------- Phase A: iot/logs ----------------------


@dataclass
class IotLog:
    """One row from `GET /api/v1/iot/logs?from_date&to_date&...`"""
    id: int
    command_alias: str | None = None
    type: str | None = None
    create_date: str | None = None
    timestamp: str | None = None
    vehicle_id: int | None = None
    device_key: str | None = None
    address: str | None = None
    cellular_signal_strength: int | None = None
    mileage: int | None = None      # odometer (miles per Basic plan; km if metric)
    speed: float | None = None
    gps_status: int | None = None
    gps_direction: str | None = None
    response_name: str | None = None
    response_received: str | None = None
    dtc_codes: Any = None
    controller: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)


def parse_iot_logs(payload: dict) -> tuple[list[IotLog], str | None]:
    """Returns (records, next_url). `next_url` is the cursor for the next
    (older) page or None when exhausted."""
    if not isinstance(payload, dict):
        return [], None
    out: list[IotLog] = []
    for r in payload.get("results") or []:
        if not isinstance(r, dict):
            continue
        out.append(IotLog(
            id=int(r.get("id") or 0),
            command_alias=r.get("command_alias"),
            type=r.get("type"),
            create_date=r.get("create_date"),
            timestamp=r.get("timestamp"),
            vehicle_id=r.get("vehicle_id"),
            device_key=r.get("device_key"),
            address=r.get("address"),
            cellular_signal_strength=r.get("cellular_signal_strength"),
            mileage=r.get("mileage"),
            speed=r.get("speed"),
            gps_status=r.get("gps_status"),
            gps_direction=r.get("gps_direction"),
            response_name=r.get("response_name"),
            response_received=r.get("response_received"),
            dtc_codes=r.get("dtc_codes"),
            controller=r.get("controller") or {},
            raw=r,
        ))
    return out, payload.get("next")


# ---------------------- Phase A: alert/event ----------------------


@dataclass
class AlertEvent:
    """One row from `GET /api/v1/alert/event?from_date&to_date&...`"""
    id: int
    create_date: str | None = None
    alert_type: str | None = None
    message: str | None = None
    vehicle_id: int | None = None
    user_id: int | None = None
    latitude: float | None = None
    longitude: float | None = None
    receiving_users: list[int] = field(default_factory=list)


def parse_alert_events(payload: dict) -> tuple[list[AlertEvent], str | None]:
    if not isinstance(payload, dict):
        return [], None
    out: list[AlertEvent] = []
    for r in payload.get("results") or []:
        if not isinstance(r, dict):
            continue
        out.append(AlertEvent(
            id=int(r.get("id") or 0),
            create_date=r.get("create_date"),
            alert_type=r.get("alert_type"),
            message=r.get("message"),
            vehicle_id=r.get("vehicle_id"),
            user_id=r.get("user_id"),
            latitude=r.get("latitude"),
            longitude=r.get("longitude"),
            receiving_users=r.get("receiving_users") or [],
        ))
    return out, payload.get("next")


# ---------------------- Phase B: metadata ----------------------


@dataclass
class DeviceInfo:
    """Single device row from `GET /api/v1/device?limit=100`."""
    device_key: str
    product_name: str | None = None
    device_state: str | None = None
    hardwired_mode: bool | None = None
    service_mode: bool | None = None
    plan_id: int | None = None
    plan_name: str | None = None
    plan_description: str | None = None
    plan_price: float | None = None
    plan_billing_model: str | None = None
    plan_activation_date: str | None = None
    plan_renewal_date: str | None = None
    plan_local_events: bool | None = None
    plan_audit_log: bool | None = None
    plan_trip_reporting: bool | None = None
    plan_dtc_events: bool | None = None
    plan_speed_limit_api: bool | None = None
    plan_location_services: bool | None = None
    plan_motion_reporting: bool | None = None
    plan_add_ons: list = field(default_factory=list)


def parse_device(payload: dict) -> list[DeviceInfo]:
    out: list[DeviceInfo] = []
    for d in (payload.get("results") if isinstance(payload, dict) else []) or []:
        if not isinstance(d, dict):
            continue
        pp = d.get("pricing_plan") or {}
        out.append(DeviceInfo(
            device_key=d.get("device_key") or "",
            product_name=d.get("product_name"),
            device_state=d.get("device_state"),
            hardwired_mode=d.get("hardwired_mode"),
            service_mode=d.get("service_mode"),
            plan_id=pp.get("id"),
            plan_name=pp.get("name"),
            plan_description=pp.get("description"),
            plan_price=pp.get("price"),
            plan_billing_model=pp.get("billing_model"),
            plan_activation_date=d.get("plan_activation_date"),
            plan_renewal_date=d.get("plan_renewal_date"),
            plan_local_events=pp.get("local_events"),
            plan_audit_log=pp.get("audit_log"),
            plan_trip_reporting=pp.get("trip_reporting"),
            plan_dtc_events=pp.get("dtc_events"),
            plan_speed_limit_api=pp.get("speed_limit_api"),
            plan_location_services=pp.get("location_services"),
            plan_motion_reporting=pp.get("motion_reporting"),
            plan_add_ons=d.get("plan_add_ons") or [],
        ))
    return out


@dataclass
class AlertRule:
    """One row from `GET /api/v1/alert/rule?limit=100`."""
    id: int
    vehicle_id: int | None = None
    type: str | None = None
    alert_type: list[str] = field(default_factory=list)
    enabled: bool = False
    email: str | None = None
    phone_number: str | None = None
    parameters: dict = field(default_factory=dict)


def parse_alert_rules(payload: dict) -> list[AlertRule]:
    out: list[AlertRule] = []
    for r in (payload.get("results") if isinstance(payload, dict) else []) or []:
        if not isinstance(r, dict):
            continue
        out.append(AlertRule(
            id=int(r.get("id") or 0),
            vehicle_id=r.get("vehicle_id"),
            type=r.get("type"),
            alert_type=r.get("alert_type") or [],
            enabled=bool(r.get("enabled")),
            email=r.get("email"),
            phone_number=r.get("phone_number"),
            parameters=r.get("parameters") or {},
        ))
    return out


@dataclass
class Geofence:
    """One row from `GET /api/v1/geofence?limit=100`."""
    id: int
    name: str
    type: str | None = None         # "radius" / "polygon" / etc.
    coordinates: list = field(default_factory=list)
    radius: float | None = None     # miles per HAR
    address: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    creator_id: int | None = None


def parse_geofences(payload: dict) -> list[Geofence]:
    out: list[Geofence] = []
    for g in (payload.get("results") if isinstance(payload, dict) else []) or []:
        if not isinstance(g, dict):
            continue
        out.append(Geofence(
            id=int(g.get("id") or 0),
            name=g.get("name") or f"geofence_{g.get('id')}",
            type=g.get("type"),
            coordinates=g.get("coordinates") or [],
            radius=g.get("radius"),
            address=g.get("address"),
            city=g.get("city"),
            state=g.get("state"),
            postal_code=g.get("postal_code"),
            creator_id=g.get("creator_id"),
        ))
    return out


@dataclass
class AccountUser:
    id: int
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    role: str | None = None   # owner / shared / etc.
    user_type: list = field(default_factory=list)


@dataclass
class AccountUsers:
    count: int = 0
    owner: AccountUser | None = None
    shared: list[AccountUser] = field(default_factory=list)


def parse_users(payload: dict) -> AccountUsers:
    items = (payload.get("results") if isinstance(payload, dict) else []) or []
    owner = None
    shared: list[AccountUser] = []
    for u in items:
        if not isinstance(u, dict):
            continue
        types = u.get("user_type") or []
        role = None
        for t in types:
            if isinstance(t, dict) and t.get("type"):
                role = t.get("type")
                break
        au = AccountUser(
            id=int(u.get("id") or 0),
            first_name=u.get("first_name"),
            last_name=u.get("last_name"),
            email=u.get("email"),
            role=role,
            user_type=types,
        )
        if role == "owner" and owner is None:
            owner = au
        else:
            shared.append(au)
    return AccountUsers(count=len(items), owner=owner, shared=shared)


@dataclass
class Firmware:
    """One row from `GET /api/v1/firmware-update?limit=100`."""
    firmware_version: str
    supported_hardware: str | None = None
    firmware_version_number: float | None = None
    release_notes: str | None = None


def parse_firmware(payload: dict) -> list[Firmware]:
    out: list[Firmware] = []
    for f in (payload.get("results") if isinstance(payload, dict) else []) or []:
        if not isinstance(f, dict):
            continue
        out.append(Firmware(
            firmware_version=f.get("firmware_version") or "",
            supported_hardware=f.get("supported_hardware"),
            firmware_version_number=f.get("firmware_version_number"),
            release_notes=f.get("release_notes"),
        ))
    return out


def latest_firmware_for_controller(
    firmwares: list[Firmware], controller_model: str | None,
) -> Firmware | None:
    """Pick the highest `firmware_version_number` row whose
    `supported_hardware` matches the vehicle's controller_model.

    DroneMobile controller_model strings (e.g., "DC3") don't always match
    `supported_hardware` strings (e.g., "FT-X1S"). We try an exact-match
    first; if nothing matches, return None so the bridge doesn't lie
    about an "update available" against the wrong hardware family.
    """
    if not controller_model or not firmwares:
        return None
    candidates = [
        f for f in firmwares
        if f.supported_hardware and f.firmware_version_number is not None
        and (controller_model.lower() in f.supported_hardware.lower()
             or f.supported_hardware.lower() in controller_model.lower())
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda f: f.firmware_version_number or 0.0)


# ---------------------- Phase C: StatusPage ----------------------


@dataclass
class StatusPageSnapshot:
    """Summary of DroneMobile's status.dronemobile.com (Atlassian StatusPage)."""
    incidents_open: int = 0
    last_incident_at: str | None = None
    last_incident_name: str | None = None
    active_maintenance_count: int = 0
    next_upcoming_maintenance_at: str | None = None
    next_upcoming_maintenance_name: str | None = None
    updated_at: str | None = None


def parse_statuspage(
    incidents: dict, active: dict, upcoming: dict,
) -> StatusPageSnapshot:
    snap = StatusPageSnapshot()
    if isinstance(incidents, dict):
        page = incidents.get("page") or {}
        snap.updated_at = page.get("updated_at")
        ilist = incidents.get("incidents") or []
        snap.incidents_open = len(ilist)
        if ilist:
            snap.last_incident_at = (ilist[0] or {}).get("created_at")
            snap.last_incident_name = (ilist[0] or {}).get("name")
    if isinstance(active, dict):
        snap.active_maintenance_count = len(active.get("scheduled_maintenances") or [])
    if isinstance(upcoming, dict):
        upc = upcoming.get("scheduled_maintenances") or []
        if upc:
            # Take the soonest (lowest scheduled_for).
            soonest = min(upc, key=lambda m: m.get("scheduled_for") or "9999")
            snap.next_upcoming_maintenance_at = soonest.get("scheduled_for")
            snap.next_upcoming_maintenance_name = soonest.get("name")
    return snap
