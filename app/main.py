"""
DroneMobile Cloud → MQTT bridge — full-coverage extension (2026-05-26+).

Consumes every documented DroneMobile API endpoint this account's plan
supports (verified via HAR + live probe):

  graph: api.dronemobile.com/api/v1/
    - GET /vehicle              (vehicle list — multi-vehicle discovery)
    - GET /vehicle/{id}         (per-vehicle telemetry — fast tier)
    - GET /iot/logs             (device-message firehose — paginated)
    - GET /alert/event          (alert history — paginated)
    - GET /device               (device + pricing-plan metadata)
    - GET /alert/rule           (configured alert rules)
    - GET /geofence             (configured geofences)
    - GET /user                 (account users + permissions)
    - GET /firmware-update      (firmware versions list)

  status.dronemobile.com/api/v2/
    - GET /incidents/unresolved.json
    - GET /scheduled-maintenances/{active,upcoming}.json

  cognito-idp.us-east-1.amazonaws.com/
    - POST InitiateAuth (USER_PASSWORD_AUTH) — 1h IdToken

Endpoint NOT consumed:
  - /vehicle/{id}/obd — plan-gated (the Basic plan returns HTTP 400
    "This device or its plan does not support OBD functions"). Bridge
    will probe + skip cleanly if a future plan upgrade unlocks it.
  - POST /iot/command, POST/PATCH /geofence — mutation endpoints
    deliberately skipped (the official HA `drone_mobile` custom_component
    owns lock/unlock/remote-start; we don't duplicate).

Tiered cadences (each handler retries cleanly on transient 429/5xx):

  FAST  (60 s)  — /vehicle/{id} per vehicle  (~29 entities updated)
  MED  (300 s)  — /iot/logs + /alert/event incremental polls (last-event
                  state + one-shot dronemobile_event publishes for
                  InfluxDB ingest) + StatusPage refresh
  SLOW (3600 s) — /device + /alert/rule + /geofence + /user +
                  /firmware-update (metadata + plan + firmware-available)

Multi-vehicle ready — `discover_vehicles()` returns list[Vehicle];
tier handlers iterate per vehicle; account / service entities publish
once.

Auth: USER_PASSWORD_AUTH grant on every IdToken expiry. The iOS app's
`GetTokensFromRefreshToken` flow requires ConfirmDevice setup the
bridge doesn't implement yet, so the bridge signs in again instead.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import signal
import sys
import time
import uuid
from typing import Any

import requests
from ha_mqtt_bridge import (
    ThreadedPublisher,
    configure_logging,
    now_s,
    register_github_error_reporter,
    request_with_backoff,
    watch_ha_birth,
)

from discovery import (
    ENTITIES,
    PHASE_B_ACCOUNT_ENTITIES,
    discovery_specs_account,
    discovery_specs_service,
    discovery_specs_vehicle,
)
from events import AlertEventStream, IotLogsStream, fetch_statuspage
from parsers import (
    AlertEvent,
    DeviceInfo,
    Geofence,
    IotLog,
    Vehicle,
    _bool_onoff,
    _get_in,
    _slugify,
    is_inside_geofence,
    latest_firmware_for_controller,
    parse_alert_rules,
    parse_device,
    parse_firmware,
    parse_geofences,
    parse_users,
    parse_vehicle,
)


# --- production-error reporter ---------------------------------------
register_github_error_reporter("dronemobile-mqtt-bridge")
# ---------------------------------------------------------------------


COGNITO_URL = "https://cognito-idp.us-east-1.amazonaws.com/"
DRONE_BASE = "https://api.dronemobile.com/api/v1/"

USERNAME = os.environ["DRONEMOBILE_USERNAME"]
PASSWORD = os.environ["DRONEMOBILE_PASSWORD"]
# DRONEMOBILE_VEHICLE_ID is retained as a fallback / hint; the bridge
# now calls /vehicle to discover all vehicles on the account dynamically.
VEHICLE_ID_HINT = os.environ.get("DRONEMOBILE_VEHICLE_ID", "")
COGNITO_CLIENT_ID = os.environ["DRONEMOBILE_COGNITO_CLIENT_ID"]

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ["MQTT_PASSWORD"]
# Off by default (current behaviour) — set MQTT_TLS=1 for a broker that
# requires TLS; MQTT_CA_FILE points at a custom CA bundle (system trust
# store is used when unset).
MQTT_TLS = os.environ.get("MQTT_TLS", "0") != "0"
MQTT_CA_FILE = os.environ.get("MQTT_CA_FILE") or None

# `POLL_INTERVAL` is the legacy single-tier name — kept as fallback for
# FAST_POLL_INTERVAL (default 60 s).
FAST_POLL = int(os.environ.get("FAST_POLL_INTERVAL", os.environ.get("POLL_INTERVAL", "60")))
MED_POLL = int(os.environ.get("MED_POLL_INTERVAL", "300"))
SLOW_POLL = int(os.environ.get("SLOW_POLL_INTERVAL", "3600"))
INTER_CALL_DELAY = float(os.environ.get("INTER_CALL_DELAY", "0.4"))

# Days of historical events to backfill into MQTT (and onward into
# InfluxDB) on bridge startup. API caps at ~365 d for both iot/logs
# (~1500 records) and alert/event (~1410 records). 0 disables.
EVENT_BACKFILL_DAYS = int(os.environ.get("EVENT_BACKFILL_DAYS", "30"))

DISCOVERY_PREFIX = os.environ.get("HA_DISCOVERY_PREFIX", "homeassistant")
TOPIC_PREFIX = os.environ.get("MQTT_TOPIC_PREFIX", "dronemobile")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

BRIDGE_LWT_TOPIC = f"{TOPIC_PREFIX}/bridge/online"

COGNITO_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://accounts.dronemobile.com/",
    "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
    "X-Amz-User-Agent": "aws-amplify/5.0.4 js",
    "Content-Type": "application/x-amz-json-1.1",
}


# -------------------------------------------------------------- Cognito auth


def cognito_initiate(username: str, password: str) -> dict[str, Any]:
    r = requests.post(COGNITO_URL, headers=COGNITO_HEADERS, json={
        "AuthFlow": "USER_PASSWORD_AUTH",
        "ClientId": COGNITO_CLIENT_ID,
        "AuthParameters": {"USERNAME": username, "PASSWORD": password},
        "ClientMetadata": {},
    }, timeout=15)
    r.raise_for_status()
    return r.json()["AuthenticationResult"]


# -------------------------------------------------------------- REST helpers


def _auth_headers(id_token: str) -> dict:
    return {"Authorization": f"Bearer {id_token}", "Accept": "application/json"}


def _get(path: str, id_token: str, *, params: dict | None = None) -> Any:
    """GET with exponential backoff on 429/5xx (shared toolkit helper —
    the same one `events.py` uses). A 429/5xx that survives every retry
    raises `RetryExhaustedError` (a `RuntimeError` subclass), which the
    main loop's per-cycle `except Exception` now catches and logs
    instead of letting it kill the process — previously this raised a
    bare, unguarded `RuntimeError` straight out of vehicle discovery."""
    url = f"{DRONE_BASE}{path}"
    r = request_with_backoff("GET", url, headers=_auth_headers(id_token), params=params, timeout=20)
    if r.status_code == 401:
        raise PermissionError(f"401 from {url}")
    r.raise_for_status()
    if not r.content:
        return None
    return r.json()


def fetch_vehicle_list(id_token: str) -> list[dict]:
    body = _get("vehicle", id_token, params={"limit": 200})
    return (body or {}).get("results") or []


def fetch_vehicle(vehicle_id: str, id_token: str) -> dict:
    return _get(f"vehicle/{vehicle_id}", id_token)


def fetch_device(id_token: str) -> dict:
    return _get("device", id_token, params={"limit": 100})


def fetch_alert_rules(id_token: str) -> dict:
    return _get("alert/rule", id_token, params={"limit": 100})


def fetch_geofences(id_token: str) -> dict:
    return _get("geofence", id_token, params={"limit": 100})


def fetch_users(id_token: str) -> dict:
    return _get("user", id_token, params={"limit": 100})


def fetch_firmware_updates(id_token: str) -> dict:
    return _get("firmware-update", id_token,
                params={"limit": 100, "offset": 0, "sort": "desc"})


# -------------------------------------------------------------- Publish helpers


def publish_vehicle_telemetry(pub: ThreadedPublisher, vehicle_id: str, payload: dict) -> int:
    """The original Phase-0 publish loop — table-driven from ENTITIES."""
    base = f"{TOPIC_PREFIX}/{vehicle_id}"
    n = 0
    for _component, slug, _name, extractor, _extras in ENTITIES:
        value = extractor(payload) if callable(extractor) else _get_in(payload, extractor)
        if value is None:
            continue
        pub.publish_state(f"{base}/{slug}", str(value))
        n += 1
    return n


def publish_geofence_membership(
    pub: ThreadedPublisher, vehicle_id: str, vehicle_payload: dict, geofences: list[Geofence],
) -> None:
    base = f"{TOPIC_PREFIX}/{vehicle_id}"
    for g in geofences:
        state = is_inside_geofence(vehicle_payload, {
            "coordinates": g.coordinates, "radius": g.radius,
        })
        if state is None:
            continue
        pub.publish_state(f"{base}/inside_geofence_{_slugify(g.name)}", state)


def publish_iot_log_event(pub: ThreadedPublisher, vehicle_id: str, log: IotLog) -> None:
    """One-shot JSON event for InfluxDB + retained last_*_at/command/address
    state mirrors."""
    base = f"{TOPIC_PREFIX}/{vehicle_id}"
    ts = log.timestamp or log.create_date
    # Retained state mirrors.
    if ts:
        pub.publish_state(f"{base}/last_iot_log_at", ts)
    if log.command_alias:
        pub.publish_state(f"{base}/last_iot_log_command", log.command_alias)
    if log.address:
        pub.publish_state(f"{base}/last_iot_log_address", log.address)
    if log.mileage is not None:
        pub.publish_state(f"{base}/odometer_miles", str(log.mileage))

    # One-shot event for InfluxDB.
    if ts:
        payload = {
            "ts": ts,
            "id": log.id,
            "command_alias": log.command_alias,
            "type": log.type,
            "vehicle_id": log.vehicle_id,
            "address": log.address,
            "mileage": log.mileage,
            "speed": log.speed,
            "gps_status": log.gps_status,
            "gps_direction": log.gps_direction,
            "cellular_signal_strength": log.cellular_signal_strength,
        }
        # Spread controller booleans as flat fields so InfluxDB can graph
        # them per-event.
        ctrl = log.controller or {}
        for k in ("armed", "door_open", "engine_on", "hood_open", "trunk_open",
                  "ignition_on", "siren_enabled", "drive_lock_enabled",
                  "reservation_status", "valet_mode_enabled"):
            if k in ctrl:
                payload[f"ctrl_{k}"] = bool(ctrl[k])
        pub.publish_event(f"{base}/events/iot_log", payload, retain=False)


def publish_alert_event(pub: ThreadedPublisher, vehicle_id: str, evt: AlertEvent) -> None:
    base = f"{TOPIC_PREFIX}/{vehicle_id}"
    ts = evt.create_date
    if ts:
        pub.publish_state(f"{base}/last_alert_at", ts)
    if evt.alert_type:
        pub.publish_state(f"{base}/last_alert_type", evt.alert_type)
    if evt.message:
        # HA caps state at 255 chars.
        msg = evt.message if len(evt.message) <= 252 else evt.message[:252] + "…"
        pub.publish_state(f"{base}/last_alert_message", msg)
    if ts:
        payload = {
            "ts": ts,
            "id": evt.id,
            "alert_type": evt.alert_type,
            "message": evt.message,
            "vehicle_id": evt.vehicle_id,
            "latitude": evt.latitude,
            "longitude": evt.longitude,
        }
        pub.publish_event(f"{base}/events/alert", payload, retain=False)


def _device_tracker_attrs(payload: dict) -> dict[str, Any] | None:
    """Build the device_tracker attrs payload from a `/vehicle/{id}`
    blob, or None if no position is available yet. Mirrors the
    Tractive bridge's `publish_pos`/`_device_tracker_payload` pattern —
    HA's map panel needs a `device_tracker` entity, not just numeric
    lat/lon sensors."""
    lat = _get_in(payload, "last_known_state.latitude")
    lon = _get_in(payload, "last_known_state.longitude")
    if lat is None or lon is None:
        return None
    try:
        lat_f, lon_f = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    attrs: dict[str, Any] = {
        "latitude": lat_f,
        "longitude": lon_f,
        # DroneMobile's API doesn't expose a GPS-fix-uncertainty field —
        # included (as None) so the attribute key is always present,
        # same convention Tractive's device_tracker uses.
        "gps_accuracy": _get_in(payload, "last_known_state.gps_accuracy"),
        "source_type": "gps",
    }
    speed = _get_in(payload, "last_known_state.speed")
    if speed is not None:
        attrs["speed"] = speed
    bearing = _get_in(payload, "last_known_state.gps_degree")
    if bearing is not None:
        attrs["course"] = bearing
    return attrs


def publish_vehicle_position(pub: ThreadedPublisher, vehicle_id: str, payload: dict) -> None:
    """device_tracker state + attrs so the vehicle shows up on HA's
    Lovelace map, not just as numeric lat/lon sensors (the numeric
    sensors already exist via ENTITIES' speed/gps_degree entries).

    State is `home`/`not_home`, computed from DroneMobile's own
    `in_geofence` flag (ON when inside ANY configured geofence — for
    an account with a single "Home" geofence that's exactly "home",
    same approximation the account-scoped `in_geofence` binary_sensor
    already makes)."""
    base = f"{TOPIC_PREFIX}/{vehicle_id}"
    attrs = _device_tracker_attrs(payload)
    if attrs is None:
        return
    dt_state = "home" if payload.get("in_geofence") else "not_home"
    pub.publish_state(f"{base}/device_tracker/state", dt_state)
    pub.publish_raw(f"{base}/device_tracker/attrs", json.dumps(attrs), retain=True)


def publish_24h_counters(pub: ThreadedPublisher, vehicle_id: str, id_token: str,
                         log: logging.Logger) -> None:
    """Cheap dedicated 24h-count fetch for iot/logs + alert/event."""
    today = dt.date.today()
    from_d = (today - dt.timedelta(days=1)).isoformat() + "T00:00:00"
    to_d = today.isoformat() + "T23:59:59"
    base = f"{TOPIC_PREFIX}/{vehicle_id}"
    try:
        iot = _get("iot/logs", id_token, params={
            "from_date": from_d, "to_date": to_d, "exclude_failures": "true",
            "limit": 1, "offset": 0, "sort": "desc",
        }) or {}
        pub.publish_state(f"{base}/iot_logs_count_24h", str(iot.get("count") or 0))
    except Exception:
        log.exception("iot/logs 24h count failed (non-fatal)")
    time.sleep(INTER_CALL_DELAY)
    try:
        al = _get("alert/event", id_token, params={
            "from_date": from_d, "to_date": to_d, "exclude_failures": "true",
            "limit": 1, "offset": 0, "sort": "desc",
        }) or {}
        pub.publish_state(f"{base}/alerts_count_24h", str(al.get("count") or 0))
    except Exception:
        log.exception("alert/event 24h count failed (non-fatal)")


# Phase B account publishers


def publish_account_metadata(
    pub: ThreadedPublisher, id_token: str, log: logging.Logger,
) -> tuple[list[Geofence], dict[str, str | None]]:
    """Hourly account metadata refresh. Returns (geofences, per-vehicle
    firmware-available-state-map keyed by vehicle_id) so the slow cycle
    can fan out vehicle-specific firmware sensors.
    """
    base = f"{TOPIC_PREFIX}/account"

    # /device — first entry's plan is what the account uses.
    devices: list[DeviceInfo] = []
    try:
        devices = parse_device(fetch_device(id_token) or {})
    except Exception:
        log.exception("/device fetch failed (non-fatal)")
    time.sleep(INTER_CALL_DELAY)
    if devices:
        d = devices[0]
        if d.plan_name: pub.publish_state(f"{base}/plan_name", d.plan_name)
        if d.plan_description: pub.publish_state(f"{base}/plan_description", d.plan_description)
        if d.plan_price is not None: pub.publish_state(f"{base}/plan_price", str(d.plan_price))
        if d.plan_billing_model: pub.publish_state(f"{base}/plan_billing_model", d.plan_billing_model)
        if d.plan_activation_date: pub.publish_state(f"{base}/plan_activation_date", d.plan_activation_date)
        if d.plan_renewal_date: pub.publish_state(f"{base}/plan_renewal_date", d.plan_renewal_date)
        for slug, val in (
            ("plan_local_events", d.plan_local_events),
            ("plan_audit_log", d.plan_audit_log),
            ("plan_trip_reporting", d.plan_trip_reporting),
            ("plan_dtc_events", d.plan_dtc_events),
            ("plan_speed_limit_api", d.plan_speed_limit_api),
            ("plan_location_services", d.plan_location_services),
            ("plan_motion_reporting", d.plan_motion_reporting),
        ):
            on_off = _bool_onoff(val)
            if on_off is not None:
                pub.publish_state(f"{base}/{slug}", on_off)
        if d.product_name: pub.publish_state(f"{base}/device_product_name", d.product_name)
        if d.device_state: pub.publish_state(f"{base}/device_state", d.device_state)
        hw = _bool_onoff(d.hardwired_mode)
        if hw is not None: pub.publish_state(f"{base}/device_hardwired", hw)

    # /alert/rule — count enabled / total.
    try:
        rules = parse_alert_rules(fetch_alert_rules(id_token) or {})
        pub.publish_state(f"{base}/alert_rules_count", str(len(rules)))
        pub.publish_state(f"{base}/alert_rules_enabled_count",
                          str(sum(1 for r in rules if r.enabled)))
    except Exception:
        log.exception("/alert/rule fetch failed (non-fatal)")
    time.sleep(INTER_CALL_DELAY)

    # /geofence — geofence list (returned for vehicle-tier inside-binaries).
    geofences: list[Geofence] = []
    try:
        geofences = parse_geofences(fetch_geofences(id_token) or {})
        pub.publish_state(f"{base}/geofences_count", str(len(geofences)))
    except Exception:
        log.exception("/geofence fetch failed (non-fatal)")
    time.sleep(INTER_CALL_DELAY)

    # /user
    try:
        users = parse_users(fetch_users(id_token) or {})
        pub.publish_state(f"{base}/users_count", str(users.count))
        if users.owner and users.owner.email:
            pub.publish_state(f"{base}/owner_email", users.owner.email)
    except Exception:
        log.exception("/user fetch failed (non-fatal)")

    return geofences, {}


def publish_firmware_for_vehicle(
    pub: ThreadedPublisher, id_token: str, vehicle_id: str,
    vehicle_payload: dict, log: logging.Logger,
) -> None:
    """Fetches /firmware-update once per slow cycle, picks the latest
    version matching this vehicle's controller_model, publishes
    `latest_firmware_version` + `firmware_update_available` binary."""
    try:
        firmwares = parse_firmware(fetch_firmware_updates(id_token) or {})
    except Exception:
        log.exception("/firmware-update fetch failed (non-fatal)")
        return
    controller_model = _get_in(vehicle_payload, "last_known_state.controller_model")
    current = _get_in(vehicle_payload, "last_known_state.firmware_version")
    latest = latest_firmware_for_controller(firmwares, controller_model)
    base = f"{TOPIC_PREFIX}/{vehicle_id}"
    if latest:
        pub.publish_state(f"{base}/latest_firmware_version", latest.firmware_version)
        update_avail = "OFF"
        if current and latest.firmware_version and current != latest.firmware_version:
            update_avail = "ON"
        pub.publish_state(f"{base}/firmware_update_available", update_avail)


def publish_statuspage(pub: ThreadedPublisher, log: logging.Logger) -> None:
    base = f"{TOPIC_PREFIX}/service"
    try:
        snap = fetch_statuspage()
    except Exception:
        log.exception("StatusPage fetch failed (non-fatal)")
        return
    pub.publish_state(f"{base}/service_degraded",
                      "ON" if snap.incidents_open > 0 else "OFF")
    pub.publish_state(f"{base}/open_incidents_count", str(snap.incidents_open))
    if snap.last_incident_at:
        pub.publish_state(f"{base}/last_incident_at", snap.last_incident_at)
    if snap.last_incident_name:
        pub.publish_state(f"{base}/last_incident_name", snap.last_incident_name)
    pub.publish_state(f"{base}/active_maintenance",
                      "ON" if snap.active_maintenance_count > 0 else "OFF")
    if snap.next_upcoming_maintenance_at:
        pub.publish_state(f"{base}/next_maintenance_at",
                          snap.next_upcoming_maintenance_at)
    if snap.next_upcoming_maintenance_name:
        pub.publish_state(f"{base}/next_maintenance_name",
                          snap.next_upcoming_maintenance_name)
    if snap.updated_at:
        pub.publish_state(f"{base}/statuspage_updated_at", snap.updated_at)


# -------------------------------------------------------------- Discovery


def discover_vehicles(id_token: str, log: logging.Logger) -> list[Vehicle]:
    """List every vehicle on the account, parsed into Vehicle records."""
    rows = fetch_vehicle_list(id_token)
    if not rows:
        # Fall back to env-var hint if /vehicle returned empty.
        if VEHICLE_ID_HINT:
            try:
                payload = fetch_vehicle(VEHICLE_ID_HINT, id_token)
                return [parse_vehicle(payload)]
            except Exception:
                log.exception("fallback to VEHICLE_ID_HINT failed")
        return []
    out: list[Vehicle] = []
    for row in rows:
        try:
            out.append(parse_vehicle(row))
        except Exception:
            log.exception("could not parse vehicle row: %s", row.get("id"))
    return out


# -------------------------------------------------------------- Tier runners


def run_fast_cycle(pub, vehicle: Vehicle, id_token: str, log,
                   geofences: list[Geofence]) -> dict:
    """Phase-0 — /vehicle/{id} telemetry. Returns the raw payload so the
    caller can reuse it (e.g. for geofence-membership computation)."""
    payload = fetch_vehicle(vehicle.vehicle_id, id_token)
    n = publish_vehicle_telemetry(pub, vehicle.vehicle_id, payload)
    publish_vehicle_position(pub, vehicle.vehicle_id, payload)
    if geofences:
        publish_geofence_membership(pub, vehicle.vehicle_id, payload, geofences)
    log.debug("fast cycle ok vehicle=%s published=%d", vehicle.vehicle_id, n)
    return payload


def run_med_cycle(pub, vehicle: Vehicle, id_token: str, log,
                  iot_stream: IotLogsStream, alert_stream: AlertEventStream) -> None:
    """Phase A — incremental iot/logs + alert/event poll + 24h counters."""
    new_iot = iot_stream.poll_incremental(
        id_token,
        lambda r: publish_iot_log_event(pub, vehicle.vehicle_id, r),
        log,
    )
    time.sleep(INTER_CALL_DELAY)
    new_alerts = alert_stream.poll_incremental(
        id_token,
        lambda e: publish_alert_event(pub, vehicle.vehicle_id, e),
        log,
    )
    time.sleep(INTER_CALL_DELAY)
    publish_24h_counters(pub, vehicle.vehicle_id, id_token, log)
    time.sleep(INTER_CALL_DELAY)
    # Service-level (StatusPage) refresh — no per-vehicle scope.
    publish_statuspage(pub, log)
    if new_iot or new_alerts:
        log.info("med cycle: %d new iot_log, %d new alerts (vehicle=%s)",
                 new_iot, new_alerts, vehicle.vehicle_id)


def run_slow_cycle(pub, vehicles: list[Vehicle], id_token: str, log) -> list[Geofence]:
    """Phase B — account + plan + alert-rules + geofences + users + per-vehicle
    firmware tracker. Returns the refreshed geofence list so the FAST tier
    can publish `inside_<geofence>` binaries on the next cycle."""
    geofences, _ = publish_account_metadata(pub, id_token, log)
    for v in vehicles:
        try:
            payload = fetch_vehicle(v.vehicle_id, id_token)
            publish_firmware_for_vehicle(pub, id_token, v.vehicle_id, payload, log)
        except Exception:
            log.exception("firmware update check failed for %s", v.vehicle_id)
        time.sleep(INTER_CALL_DELAY)
    return geofences


# -------------------------------------------------------------- Main


def main() -> int:
    log = configure_logging("dronemobile-mqtt-bridge", LOG_LEVEL)
    log.info("starting; fast=%ss med=%ss slow=%ss inter_call=%ss",
             FAST_POLL, MED_POLL, SLOW_POLL, INTER_CALL_DELAY)

    creds = cognito_initiate(USERNAME, PASSWORD)
    id_token = creds["IdToken"]
    token_expires_at = now_s() + int(creds["ExpiresIn"])
    log.info("login ok; token expires in %ds", token_expires_at - now_s())

    pub = ThreadedPublisher(
        host=MQTT_HOST, port=MQTT_PORT, username=MQTT_USER, password=MQTT_PASS,
        client_id=f"dronemobile-mqtt-bridge-{uuid.uuid4().hex[:8]}",
        lwt_topic=BRIDGE_LWT_TOPIC, discovery_prefix=DISCOVERY_PREFIX,
        health_path="/tmp/healthy", tls=MQTT_TLS, ca_file=MQTT_CA_FILE,
    )
    pub.start()

    vehicles: list[Vehicle] = []
    geofences: list[Geofence] = []
    iot_streams: dict[str, IotLogsStream] = {}
    alert_streams: dict[str, AlertEventStream] = {}
    discovery_published = False
    backfill_done = False
    stopping = False
    next_fast = 0.0
    next_med = 0.0
    next_slow = 0.0

    def on_signal(signum, _frame):
        nonlocal stopping
        log.info("signal %s, shutting down", signum)
        stopping = True
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    def _on_ha_birth() -> None:
        # HA republishes nothing on its own restart; retained discovery
        # configs usually survive in the broker, but not always (a
        # broker restart with no persistence, a manual "purge retained
        # messages"). Re-running the same discovery-publish block on
        # HA's birth message and nudging every tier to run on the next
        # loop iteration closes that gap without a bridge restart.
        nonlocal discovery_published, next_fast, next_med, next_slow
        log.info("HA birth message received; re-publishing discovery and refreshing state")
        discovery_published = False
        next_fast = 0.0
        next_med = 0.0
        next_slow = 0.0

    watch_ha_birth(pub, _on_ha_birth, discovery_prefix=DISCOVERY_PREFIX)

    while not stopping:
        try:
            # Re-auth via password grant 5 min before IdToken expiry.
            if now_s() > token_expires_at - 300:
                creds = cognito_initiate(USERNAME, PASSWORD)
                id_token = creds["IdToken"]
                token_expires_at = now_s() + int(creds["ExpiresIn"])
                log.info("token re-issued via password grant; expires in %ds",
                         token_expires_at - now_s())

            if not vehicles:
                vehicles = discover_vehicles(id_token, log)
                if not vehicles:
                    log.warning("no vehicles found, retrying in %ds", FAST_POLL)
                    time.sleep(FAST_POLL)
                    continue
                log.info("discovered %d vehicle(s): %s", len(vehicles),
                         ", ".join(v.name for v in vehicles))
                # Initial geofence fetch so FAST cycle has them on first run.
                try:
                    geofences = parse_geofences(fetch_geofences(id_token) or {})
                    log.info("loaded %d geofence(s) at startup", len(geofences))
                except Exception:
                    log.exception("startup geofence fetch failed (non-fatal)")

            if not discovery_published:
                total = 0
                for v in vehicles:
                    for component, unique_id, payload in discovery_specs_vehicle(
                        v, TOPIC_PREFIX, BRIDGE_LWT_TOPIC, geofences,
                    ):
                        pub.publish_discovery(component=component, unique_id=unique_id, payload=payload)
                        total += 1
                for component, unique_id, payload in discovery_specs_account(
                    TOPIC_PREFIX, BRIDGE_LWT_TOPIC,
                ):
                    pub.publish_discovery(component=component, unique_id=unique_id, payload=payload)
                    total += 1
                for component, unique_id, payload in discovery_specs_service(
                    TOPIC_PREFIX, BRIDGE_LWT_TOPIC,
                ):
                    pub.publish_discovery(component=component, unique_id=unique_id, payload=payload)
                    total += 1
                discovery_published = True
                log.info("discovery published: %d entities across %d vehicle(s) + account + service",
                         total, len(vehicles))

            if not backfill_done:
                # Phase A backfill — per-vehicle iot/logs + alert/event.
                # Independent of discovery_published so an HA-birth
                # re-publish of discovery doesn't also re-run (and
                # re-count against rate limits) a full history backfill.
                for v in vehicles:
                    iot = IotLogsStream(v.vehicle_id)
                    alt = AlertEventStream(v.vehicle_id)
                    iot.backfill(
                        id_token, EVENT_BACKFILL_DAYS,
                        lambda r, vid=v.vehicle_id: publish_iot_log_event(pub, vid, r),
                        log,
                    )
                    alt.backfill(
                        id_token, EVENT_BACKFILL_DAYS,
                        lambda e, vid=v.vehicle_id: publish_alert_event(pub, vid, e),
                        log,
                    )
                    iot_streams[v.vehicle_id] = iot
                    alert_streams[v.vehicle_id] = alt
                backfill_done = True

            now = time.monotonic()
            if now >= next_fast:
                for v in vehicles:
                    try:
                        run_fast_cycle(pub, v, id_token, log, geofences)
                    except PermissionError:
                        raise
                    except Exception as e:
                        log.exception("fast cycle failed for %s: %s", v.name, e)
                next_fast = now + FAST_POLL

            if now >= next_med:
                for v in vehicles:
                    try:
                        run_med_cycle(pub, v, id_token, log,
                                      iot_streams[v.vehicle_id],
                                      alert_streams[v.vehicle_id])
                    except PermissionError:
                        raise
                    except Exception as e:
                        log.exception("med cycle failed for %s: %s", v.name, e)
                next_med = now + MED_POLL

            if now >= next_slow:
                try:
                    geofences = run_slow_cycle(pub, vehicles, id_token, log) or geofences
                except PermissionError:
                    raise
                except Exception as e:
                    log.exception("slow cycle failed: %s", e)
                next_slow = now + SLOW_POLL

        except PermissionError:
            log.warning("auth expired mid-poll; re-authing via password grant")
            try:
                creds = cognito_initiate(USERNAME, PASSWORD)
                id_token = creds["IdToken"]
                token_expires_at = now_s() + int(creds["ExpiresIn"])
            except Exception as e:
                log.error("re-auth failed: %s", e)
                time.sleep(30)
        except requests.RequestException as e:
            log.error("network/HTTP error: %s", e)
        except Exception:
            # Last-resort guard for the main loop itself — e.g. a
            # RetryExhaustedError surfacing from vehicle discovery or
            # the startup geofence fetch, neither of which is wrapped in
            # its own per-call try/except the way the FAST/MED/SLOW tier
            # runners are. Previously this class of error (an unguarded
            # 429/5xx exhausting its retries) propagated straight out of
            # `main()` and killed the process.
            log.exception("poll cycle failed unexpectedly")

        for _ in range(5):
            if stopping:
                break
            time.sleep(1)

    pub.stop()
    log.info("shutdown clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
