"""HA MQTT Discovery payload builder for the DroneMobile bridge.

Three device blocks:
  - Per-vehicle ("DroneMobile Bridge: <name>") — the original 29 telemetry
    entities + new Phase A iot_logs / alert/event last-event sensors +
    Phase B per-geofence inside-binaries + Phase B firmware-update binary.
  - Account ("DroneMobile Account") — Phase B plan + alert rules + user
    diagnostics.
  - Service ("DroneMobile Service") — Phase C StatusPage health.

`ENTITIES` is the existing table-driven definition for vehicle telemetry —
preserved verbatim so all Phase-0 entity unique_ids stay stable. New
entities are added via `PHASE_A_VEHICLE_ENTITIES`, `PHASE_B_ACCOUNT_ENTITIES`,
`PHASE_C_SERVICE_ENTITIES`, and the per-geofence factory.
"""

from __future__ import annotations

from typing import Any

from ha_mqtt_bridge import (
    availability_block,
    build_device_block,
    build_discovery_payload,
)

from parsers import (
    Geofence,
    Vehicle,
    _bool_onoff,
    _compass_direction,
    _get_in,
    _humanize_carrier,
    _is_moving,
    _signal_quality,
    _slugify,
    _str_or_none,
)


# ---------------------- Device blocks ----------------------


def _vehicle_device_block(v: Vehicle) -> dict:
    return build_device_block(
        identifiers=[f"dronemobile_mqtt_bridge_{v.vehicle_id}"],
        name=f"DroneMobile Bridge: {v.name}",
        manufacturer="DroneMobile",
        model=f"{v.year} {v.make} {v.model}".strip(),
    )


def _account_device_block() -> dict:
    return build_device_block(
        identifiers=["dronemobile_mqtt_bridge_account"],
        name="DroneMobile Account",
        manufacturer="DroneMobile",
        model="Cloud account",
    )


def _service_device_block() -> dict:
    return build_device_block(
        identifiers=["dronemobile_mqtt_bridge_service"],
        name="DroneMobile Service",
        manufacturer="DroneMobile",
        model="StatusPage",
        configuration_url="https://status.dronemobile.com",
    )


# `build_discovery_payload` in the shared toolkit accepts only a curated
# subset of HA Discovery fields. `device_tracker` needs `source_type` +
# `payload_home` / `payload_not_home`, which aren't in that curated set —
# so we hand-build the payload here rather than expanding the toolkit
# for one consumer. Mirrors the Tractive bridge's `_device_tracker_payload`.
def _device_tracker_payload(
    *, name: str, unique_id: str, state_topic: str, json_attributes_topic: str,
    device: dict, avail: dict, icon: str,
) -> dict:
    return {
        "name": name,
        "unique_id": unique_id,
        "object_id": unique_id,
        "state_topic": state_topic,
        "json_attributes_topic": json_attributes_topic,
        "payload_home": "home",
        "payload_not_home": "not_home",
        "source_type": "gps",
        "icon": icon,
        "device": device,
        **avail,
    }


# ---------------------- Phase 0 vehicle telemetry (preserved verbatim) ----------------------


# Mapping: (component, slug, name, payload_path_or_callable, extras)
ENTITIES: list[tuple[str, str, str, Any, dict]] = [
    # ---- movement / speed / position ----
    ("binary_sensor", "moving", "Moving", _is_moving,
        {"device_class": "moving", "payload_on": "ON", "payload_off": "OFF", "icon": "mdi:car-traction-control"}),
    ("sensor", "speed", "Speed",
        lambda p: _str_or_none(_get_in(p, "last_known_state.speed")),
        {"unit_of_measurement": "mph", "state_class": "measurement", "icon": "mdi:speedometer"}),
    ("sensor", "gps_degree", "Compass Bearing",
        lambda p: _str_or_none(_get_in(p, "last_known_state.gps_degree")),
        {"unit_of_measurement": "°", "state_class": "measurement", "icon": "mdi:compass"}),
    ("sensor", "compass_direction", "Compass Direction",
        _compass_direction,
        {"icon": "mdi:compass-rose"}),
    ("sensor", "last_movement_timestamp", "Last Movement",
        lambda p: _str_or_none(p.get("last_movement_timestamp")),
        {"device_class": "timestamp", "icon": "mdi:car-clock"}),
    # ---- cellular / connectivity ----
    ("sensor", "cellular_signal_strength", "Cellular Signal Strength",
        lambda p: _str_or_none(_get_in(p, "last_known_state.cellular_signal_strength")),
        {"unit_of_measurement": "dBm", "state_class": "measurement", "icon": "mdi:signal", "entity_category": "diagnostic"}),
    ("sensor", "cellular_signal_quality", "Cellular Signal Quality",
        _signal_quality,
        {"icon": "mdi:signal-cellular-3", "entity_category": "diagnostic"}),
    ("sensor", "carrier", "Carrier",
        lambda p: _humanize_carrier(_get_in(p, "last_known_state.carrier")),
        {"icon": "mdi:radio-tower", "entity_category": "diagnostic"}),
    ("sensor", "cellular_network", "Cellular Network",
        lambda p: _str_or_none(_get_in(p, "last_known_state.current_cellular_network")),
        {"icon": "mdi:antenna", "entity_category": "diagnostic"}),
    # ---- firmware / hardware ----
    ("sensor", "firmware_version", "Module Firmware",
        lambda p: _str_or_none(_get_in(p, "last_known_state.firmware_version")),
        {"icon": "mdi:chip", "entity_category": "diagnostic"}),
    ("sensor", "controller_model", "Controller Model",
        lambda p: _str_or_none(_get_in(p, "last_known_state.controller_model")),
        {"icon": "mdi:car-cog", "entity_category": "diagnostic"}),
    ("sensor", "backup_battery_voltage", "Backup Battery Voltage",
        lambda p: _str_or_none(_get_in(p, "last_known_state.backup_battery_voltage")),
        {"unit_of_measurement": "V", "device_class": "voltage", "state_class": "measurement",
         "icon": "mdi:battery", "entity_category": "diagnostic"}),
    ("sensor", "imei", "IMEI",
        lambda p: _str_or_none(_get_in(p, "last_known_state.imei")),
        {"icon": "mdi:sim", "entity_category": "diagnostic"}),
    ("sensor", "iccid", "ICCID",
        lambda p: _str_or_none(_get_in(p, "last_known_state.iccid")),
        {"icon": "mdi:sim", "entity_category": "diagnostic"}),
    # ---- vehicle-level statuses (binary) ----
    ("binary_sensor", "service_due", "Service Due",
        lambda p: _bool_onoff(p.get("service_due")),
        {"device_class": "problem", "payload_on": "ON", "payload_off": "OFF", "icon": "mdi:car-wrench"}),
    ("binary_sensor", "towing_detected", "Towing Detected",
        lambda p: _bool_onoff(p.get("towing_detected")),
        {"device_class": "problem", "payload_on": "ON", "payload_off": "OFF", "icon": "mdi:tow-truck"}),
    ("binary_sensor", "battery_off", "Battery Disconnected",
        lambda p: _bool_onoff(p.get("battery_off")),
        {"device_class": "problem", "payload_on": "ON", "payload_off": "OFF", "icon": "mdi:battery-off"}),
    ("binary_sensor", "battery_reconnected", "Battery Reconnected",
        lambda p: _bool_onoff(p.get("battery_reconnected")),
        {"payload_on": "ON", "payload_off": "OFF", "icon": "mdi:battery-check"}),
    ("binary_sensor", "low_battery", "Low Battery",
        lambda p: _bool_onoff(p.get("low_battery")),
        {"device_class": "battery", "payload_on": "ON", "payload_off": "OFF"}),
    ("binary_sensor", "in_geofence", "In Geofence",
        lambda p: _bool_onoff(p.get("in_geofence")),
        {"device_class": "presence", "payload_on": "ON", "payload_off": "OFF", "icon": "mdi:map-marker-radius"}),
    ("binary_sensor", "panic_status", "Panic Active",
        lambda p: _bool_onoff(p.get("panic_status")),
        {"device_class": "safety", "payload_on": "ON", "payload_off": "OFF", "icon": "mdi:alarm-light"}),
    # ---- controller security flags ----
    ("binary_sensor", "armed", "Armed",
        lambda p: _bool_onoff(_get_in(p, "last_known_state.controller.armed")),
        {"payload_on": "ON", "payload_off": "OFF", "icon": "mdi:shield-lock", "entity_category": "diagnostic"}),
    ("binary_sensor", "reservation_status", "Reservation Mode",
        lambda p: _bool_onoff(_get_in(p, "last_known_state.controller.reservation_status")),
        {"payload_on": "ON", "payload_off": "OFF", "icon": "mdi:car-key", "entity_category": "diagnostic"}),
    ("binary_sensor", "siren_enabled", "Siren Enabled",
        lambda p: _bool_onoff(_get_in(p, "last_known_state.controller.siren_enabled")),
        {"payload_on": "ON", "payload_off": "OFF", "icon": "mdi:bell-ring", "entity_category": "diagnostic"}),
    ("binary_sensor", "shock_sensor_enabled", "Shock Sensor Enabled",
        lambda p: _bool_onoff(_get_in(p, "last_known_state.controller.shock_sensor_enabled")),
        {"payload_on": "ON", "payload_off": "OFF", "icon": "mdi:vibrate", "entity_category": "diagnostic"}),
    ("binary_sensor", "valet_mode_enabled", "Valet Mode",
        lambda p: _bool_onoff(_get_in(p, "last_known_state.controller.valet_mode_enabled")),
        {"payload_on": "ON", "payload_off": "OFF", "icon": "mdi:account-tie", "entity_category": "diagnostic"}),
    ("binary_sensor", "auto_door_lock_enabled", "Auto Door Lock Enabled",
        lambda p: _bool_onoff(_get_in(p, "last_known_state.controller.auto_door_lock_enabled")),
        {"payload_on": "ON", "payload_off": "OFF", "icon": "mdi:car-door-lock", "entity_category": "diagnostic"}),
    ("binary_sensor", "passive_arming_enabled", "Passive Arming",
        lambda p: _bool_onoff(_get_in(p, "last_known_state.controller.passive_arming_enabled")),
        {"payload_on": "ON", "payload_off": "OFF", "icon": "mdi:shield-half-full", "entity_category": "diagnostic"}),
    ("binary_sensor", "drive_lock_enabled", "Drive Lock Enabled",
        lambda p: _bool_onoff(_get_in(p, "last_known_state.controller.drive_lock_enabled")),
        {"payload_on": "ON", "payload_off": "OFF", "icon": "mdi:car-door-lock", "entity_category": "diagnostic"}),
    # ---- subscription ----
    ("sensor", "subscription_plan", "Subscription Plan",
        lambda p: _str_or_none(_get_in(p, "pricing_plan.name")),
        {"icon": "mdi:credit-card-outline", "entity_category": "diagnostic"}),
    ("sensor", "subscription_renewal", "Subscription Renewal",
        lambda p: _str_or_none(_get_in(p, "pricing_plan.plan_renewal_date")),
        {"device_class": "timestamp", "icon": "mdi:calendar-check", "entity_category": "diagnostic"}),
]


# ---------------------- Phase A — iot/logs + alert/event last-event sensors ----------------------


PHASE_A_VEHICLE_ENTITIES: list[tuple[str, str, str, dict]] = [
    # iot/logs last-event mirror
    ("sensor", "last_iot_log_at", "Last IoT Log",
     {"device_class": "timestamp", "icon": "mdi:message-text-clock"}),
    ("sensor", "last_iot_log_command", "Last IoT Log Command",
     {"icon": "mdi:message-text", "entity_category": "diagnostic"}),
    ("sensor", "last_iot_log_address", "Last IoT Log Location",
     {"icon": "mdi:map-marker", "entity_category": "diagnostic"}),
    ("sensor", "iot_logs_count_24h", "IoT Logs 24 h",
     {"state_class": "measurement", "icon": "mdi:counter"}),
    # alert/event last-event mirror
    ("sensor", "last_alert_at", "Last Alert",
     {"device_class": "timestamp", "icon": "mdi:bell-alert"}),
    ("sensor", "last_alert_type", "Last Alert Type",
     {"icon": "mdi:bell-ring-outline"}),
    ("sensor", "last_alert_message", "Last Alert Message",
     {"icon": "mdi:message-alert"}),
    ("sensor", "alerts_count_24h", "Alerts 24 h",
     {"state_class": "measurement", "icon": "mdi:counter"}),
    # Odometer from iot_logs (the /vehicle/{id} endpoint doesn't surface mileage)
    ("sensor", "odometer_miles", "Odometer",
     {"unit_of_measurement": "mi", "device_class": "distance",
      "state_class": "total_increasing", "icon": "mdi:counter"}),
]


# ---------------------- Phase B — plan / device / firmware metadata ----------------------


# Plan-capability booleans from /api/v1/device.pricing_plan. Useful as
# diagnostic binaries so HA users can see at a glance which features
# their plan unlocks.
PHASE_B_ACCOUNT_ENTITIES: list[tuple[str, str, str, dict]] = [
    ("sensor", "plan_name", "Plan",
     {"icon": "mdi:credit-card-outline", "entity_category": "diagnostic"}),
    ("sensor", "plan_description", "Plan Description",
     {"icon": "mdi:text-box-outline", "entity_category": "diagnostic"}),
    ("sensor", "plan_price", "Plan Price",
     {"unit_of_measurement": "USD", "state_class": "measurement",
      "icon": "mdi:cash", "entity_category": "diagnostic"}),
    ("sensor", "plan_billing_model", "Plan Billing",
     {"icon": "mdi:calendar-refresh", "entity_category": "diagnostic"}),
    ("sensor", "plan_activation_date", "Plan Activated",
     {"device_class": "timestamp", "icon": "mdi:calendar-check",
      "entity_category": "diagnostic"}),
    ("sensor", "plan_renewal_date", "Plan Renews",
     {"device_class": "timestamp", "icon": "mdi:calendar-clock",
      "entity_category": "diagnostic"}),
    ("binary_sensor", "plan_local_events", "Plan: Local Events",
     {"payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:lan", "entity_category": "diagnostic"}),
    ("binary_sensor", "plan_audit_log", "Plan: Audit Log",
     {"payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:clipboard-text", "entity_category": "diagnostic"}),
    ("binary_sensor", "plan_trip_reporting", "Plan: Trip Reporting",
     {"payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:road-variant", "entity_category": "diagnostic"}),
    ("binary_sensor", "plan_dtc_events", "Plan: DTC Events",
     {"payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:engine", "entity_category": "diagnostic"}),
    ("binary_sensor", "plan_speed_limit_api", "Plan: Speed-Limit API",
     {"payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:sign-caution", "entity_category": "diagnostic"}),
    ("binary_sensor", "plan_location_services", "Plan: Location Services",
     {"payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:crosshairs-gps", "entity_category": "diagnostic"}),
    ("binary_sensor", "plan_motion_reporting", "Plan: Motion Reporting",
     {"payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:car-shift-pattern", "entity_category": "diagnostic"}),
    ("sensor", "device_product_name", "Device Product",
     {"icon": "mdi:devices", "entity_category": "diagnostic"}),
    ("sensor", "device_state", "Device State",
     {"icon": "mdi:state-machine", "entity_category": "diagnostic"}),
    ("binary_sensor", "device_hardwired", "Device Hardwired",
     {"payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:car-battery", "entity_category": "diagnostic"}),
    ("sensor", "alert_rules_count", "Alert Rules",
     {"state_class": "measurement", "icon": "mdi:bell-cog",
      "entity_category": "diagnostic"}),
    ("sensor", "alert_rules_enabled_count", "Alert Rules Enabled",
     {"state_class": "measurement", "icon": "mdi:bell-check",
      "entity_category": "diagnostic"}),
    ("sensor", "geofences_count", "Geofences",
     {"state_class": "measurement", "icon": "mdi:map-marker-multiple",
      "entity_category": "diagnostic"}),
    ("sensor", "users_count", "Users on Account",
     {"state_class": "measurement", "icon": "mdi:account-multiple",
      "entity_category": "diagnostic"}),
    ("sensor", "owner_email", "Owner Email",
     {"icon": "mdi:email", "entity_category": "diagnostic"}),
]


# Vehicle-scoped firmware tracker (latest available firmware vs current)
PHASE_B_VEHICLE_FIRMWARE_ENTITIES: list[tuple[str, str, str, dict]] = [
    ("sensor", "latest_firmware_version", "Latest Firmware Available",
     {"icon": "mdi:chip", "entity_category": "diagnostic"}),
    ("binary_sensor", "firmware_update_available", "Firmware Update Available",
     {"device_class": "update", "payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:package-up", "entity_category": "diagnostic"}),
]


# ---------------------- Phase C — StatusPage ----------------------


PHASE_C_SERVICE_ENTITIES: list[tuple[str, str, str, dict]] = [
    ("binary_sensor", "service_degraded", "Service Degraded",
     {"device_class": "problem", "payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:cloud-alert"}),
    ("sensor", "open_incidents_count", "Open Incidents",
     {"state_class": "measurement", "icon": "mdi:alert-circle",
      "entity_category": "diagnostic"}),
    ("sensor", "last_incident_at", "Last Incident",
     {"device_class": "timestamp", "icon": "mdi:history",
      "entity_category": "diagnostic"}),
    ("sensor", "last_incident_name", "Last Incident Name",
     {"icon": "mdi:alert", "entity_category": "diagnostic"}),
    ("binary_sensor", "active_maintenance", "Active Maintenance",
     {"payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:tools", "entity_category": "diagnostic"}),
    ("sensor", "next_maintenance_at", "Next Maintenance",
     {"device_class": "timestamp", "icon": "mdi:calendar-clock",
      "entity_category": "diagnostic"}),
    ("sensor", "next_maintenance_name", "Next Maintenance Name",
     {"icon": "mdi:tools", "entity_category": "diagnostic"}),
    ("sensor", "statuspage_updated_at", "StatusPage Refreshed",
     {"device_class": "timestamp", "icon": "mdi:cloud-sync",
      "entity_category": "diagnostic"}),
]


# ---------------------- Discovery factory ----------------------


def discovery_specs_vehicle(
    v: Vehicle, topic_prefix: str, lwt_topic: str, geofences: list[Geofence],
) -> list[tuple[str, str, dict]]:
    """Per-vehicle entities — Phase-0 telemetry + Phase A last-event sensors
    + Phase B firmware-tracker + one inside_geofence_<slug> binary per
    configured geofence."""
    items: list[tuple[str, str, dict]] = []
    avail = availability_block(lwt_topic)
    dev_uid = f"dronemobile_mqtt_bridge_{v.vehicle_id}"
    device = _vehicle_device_block(v)
    base = f"{topic_prefix}/{v.vehicle_id}"

    # Phase 0 — preserved verbatim from original bridge.
    for component, slug, name, _, extras in ENTITIES:
        uid = f"{dev_uid}_{slug}"
        items.append((
            component, f"{dev_uid}/{slug}",
            build_discovery_payload(
                name=name, unique_id=uid, object_id=uid,
                state_topic=f"{base}/{slug}", device=device, **avail, **extras,
            ),
        ))

    # Phase A — last-event mirrors + counters.
    for component, slug, name, extras in PHASE_A_VEHICLE_ENTITIES + PHASE_B_VEHICLE_FIRMWARE_ENTITIES:
        uid = f"{dev_uid}_{slug}"
        items.append((
            component, f"{dev_uid}/{slug}",
            build_discovery_payload(
                name=name, unique_id=uid, object_id=uid,
                state_topic=f"{base}/{slug}", device=device, **avail, **extras,
            ),
        ))

    # Phase B — one inside_<name> binary per configured geofence.
    for g in geofences:
        slug = f"inside_geofence_{_slugify(g.name)}"
        uid = f"{dev_uid}_{slug}"
        items.append((
            "binary_sensor", f"{dev_uid}/{slug}",
            build_discovery_payload(
                name=f"Inside Geofence: {g.name}", unique_id=uid, object_id=uid,
                state_topic=f"{base}/{slug}", device=device,
                device_class="presence", payload_on="ON", payload_off="OFF",
                icon="mdi:map-marker-radius", **avail,
            ),
        ))

    # device_tracker — places the vehicle on HA's Lovelace map (hand-built
    # since the toolkit doesn't accept source_type; see
    # `_device_tracker_payload`).
    dt_uid = f"{dev_uid}_device_tracker"
    items.append((
        "device_tracker", f"{dev_uid}/device_tracker",
        _device_tracker_payload(
            name="Location", unique_id=dt_uid,
            state_topic=f"{base}/device_tracker/state",
            json_attributes_topic=f"{base}/device_tracker/attrs",
            device=device, avail=avail, icon="mdi:car",
        ),
    ))
    return items


def discovery_specs_account(
    topic_prefix: str, lwt_topic: str,
) -> list[tuple[str, str, dict]]:
    """Account-scoped — fires once per bridge instance regardless of
    vehicle count."""
    items: list[tuple[str, str, dict]] = []
    avail = availability_block(lwt_topic)
    dev_uid = "dronemobile_mqtt_bridge_account"
    device = _account_device_block()
    base = f"{topic_prefix}/account"
    for component, slug, name, extras in PHASE_B_ACCOUNT_ENTITIES:
        uid = f"{dev_uid}_{slug}"
        items.append((
            component, f"{dev_uid}/{slug}",
            build_discovery_payload(
                name=name, unique_id=uid, object_id=uid,
                state_topic=f"{base}/{slug}", device=device, **avail, **extras,
            ),
        ))
    return items


def discovery_specs_service(
    topic_prefix: str, lwt_topic: str,
) -> list[tuple[str, str, dict]]:
    """StatusPage — once per bridge instance."""
    items: list[tuple[str, str, dict]] = []
    avail = availability_block(lwt_topic)
    dev_uid = "dronemobile_mqtt_bridge_service"
    device = _service_device_block()
    base = f"{topic_prefix}/service"
    for component, slug, name, extras in PHASE_C_SERVICE_ENTITIES:
        uid = f"{dev_uid}_{slug}"
        items.append((
            component, f"{dev_uid}/{slug}",
            build_discovery_payload(
                name=name, unique_id=uid, object_id=uid,
                state_topic=f"{base}/{slug}", device=device, **avail, **extras,
            ),
        ))
    return items
