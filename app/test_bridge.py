"""Unit tests for the DroneMobile bridge's parsers, derivations and HA
Discovery payloads — synthetic fixtures only, no network access.

Mirrors govee-mqtt-bridge's `test_discovery_regression.py` layout: stub
out `requests`/`paho` (and point at the in-repo toolkit source) so the
bridge's own modules import cleanly without real credentials or the
vendored toolkit being pip-installed. Run with::

    cd app
    DRONEMOBILE_USERNAME=x DRONEMOBILE_PASSWORD=x \\
        DRONEMOBILE_COGNITO_CLIENT_ID=x MQTT_PASSWORD=x \\
        python -m pytest test_bridge.py -q
"""

from __future__ import annotations

import os
import sys
import types
import unittest

_REQUIRED = {
    "DRONEMOBILE_USERNAME": "x@example.com",
    "DRONEMOBILE_PASSWORD": "test",
    "DRONEMOBILE_COGNITO_CLIENT_ID": "test-client",
    "MQTT_PASSWORD": "test",
}
for k, v in _REQUIRED.items():
    os.environ.setdefault(k, v)


def _stub_module(name: str, **attrs: object) -> None:
    if name in sys.modules:
        return
    mod = types.ModuleType(name)
    for attr_name, attr_val in attrs.items():
        setattr(mod, attr_name, attr_val)
    sys.modules[name] = mod


_stub_module("requests")
sys.modules["requests"].RequestException = Exception  # type: ignore[attr-defined]
sys.modules["requests"].get = lambda *a, **k: None  # type: ignore[attr-defined]
sys.modules["requests"].post = lambda *a, **k: None  # type: ignore[attr-defined]
sys.modules["requests"].request = lambda *a, **k: None  # type: ignore[attr-defined]
_stub_module("paho")
_stub_module("paho.mqtt")
mqtt_stub = types.ModuleType("paho.mqtt.client")


class _FakeCallbackAPIVersion:
    VERSION2 = 2


class _FakeMqttClient:
    def __init__(self, *_, **__):
        pass


mqtt_stub.CallbackAPIVersion = _FakeCallbackAPIVersion
mqtt_stub.Client = _FakeMqttClient
sys.modules["paho.mqtt.client"] = mqtt_stub

if "ha_mqtt_bridge" not in sys.modules:
    here = os.path.dirname(os.path.abspath(__file__))
    toolkit = os.path.normpath(
        os.path.join(here, "..", "..", "..", "_shared", "ha-mqtt-bridge-toolkit")
    )
    sys.path.insert(0, toolkit)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import discovery  # noqa: E402
import main  # noqa: E402
import parsers  # noqa: E402


# -------------------------------------------------------------- parsers / derivations


class TestDerivations(unittest.TestCase):
    def test_get_in_nested(self):
        self.assertEqual(
            parsers._get_in({"a": {"b": {"c": 3}}}, "a.b.c"), 3
        )
        self.assertIsNone(parsers._get_in({"a": {}}, "a.b.c"))
        self.assertIsNone(parsers._get_in({}, "a.b.c"))

    def test_bool_onoff(self):
        self.assertEqual(parsers._bool_onoff(True), "ON")
        self.assertEqual(parsers._bool_onoff(False), "OFF")
        self.assertIsNone(parsers._bool_onoff(None))

    def test_is_moving(self):
        self.assertEqual(
            parsers._is_moving({"last_known_state": {"speed": 12.3}}), "ON"
        )
        self.assertEqual(
            parsers._is_moving({"last_known_state": {"speed": 0.0}}), "OFF"
        )
        self.assertIsNone(parsers._is_moving({"last_known_state": {}}))

    def test_humanize_carrier_brand_override(self):
        self.assertEqual(parsers._humanize_carrier("at&t"), "AT&T")
        self.assertEqual(parsers._humanize_carrier("T-MOBILE"), "T-Mobile")
        self.assertEqual(parsers._humanize_carrier("some other co"), "Some Other Co")
        self.assertIsNone(parsers._humanize_carrier(None))

    def test_compass_direction_octants(self):
        self.assertEqual(
            parsers._compass_direction({"last_known_state": {"gps_degree": 0}}), "N"
        )
        self.assertEqual(
            parsers._compass_direction({"last_known_state": {"gps_degree": 90}}), "E"
        )
        self.assertEqual(
            parsers._compass_direction({"last_known_state": {"gps_degree": 359}}), "N"
        )
        self.assertIsNone(parsers._compass_direction({"last_known_state": {}}))

    def test_signal_quality_buckets(self):
        self.assertEqual(
            parsers._signal_quality({"last_known_state": {"cellular_signal_strength": 28}}),
            "Excellent",
        )
        self.assertEqual(
            parsers._signal_quality({"last_known_state": {"cellular_signal_strength": 5}}),
            "Poor",
        )


class TestGeofence(unittest.TestCase):
    def test_inside_radius(self):
        # Home at (44.9, -93.1); vehicle at the same point -> distance 0,
        # always inside any positive radius.
        payload = {"last_known_state": {"latitude": 44.9, "longitude": -93.1}}
        geofence = {"coordinates": [44.9, -93.1], "radius": 0.5}
        self.assertEqual(parsers.is_inside_geofence(payload, geofence), "ON")

    def test_outside_radius(self):
        payload = {"last_known_state": {"latitude": 45.5, "longitude": -93.1}}
        geofence = {"coordinates": [44.9, -93.1], "radius": 0.5}
        self.assertEqual(parsers.is_inside_geofence(payload, geofence), "OFF")

    def test_missing_position_is_none(self):
        geofence = {"coordinates": [44.9, -93.1], "radius": 0.5}
        self.assertIsNone(parsers.is_inside_geofence({}, geofence))


class TestParseVehicle(unittest.TestCase):
    def test_parse_vehicle_basic(self):
        v = parsers.parse_vehicle({
            "id": 12345,
            "vehicle_name": "M3",
            "vehicle_make": "BMW",
            "vehicle_model": "M3",
            "vehicle_year": 2011,
            "vin": "WBSWD93549PY00000",
        })
        self.assertEqual(v.vehicle_id, "12345")
        self.assertEqual(v.name, "M3")
        self.assertEqual(v.year, 2011)


# -------------------------------------------------------------- device_tracker (main.py)


VEHICLE_PAYLOAD_WITH_POSITION = {
    "last_known_state": {
        "latitude": 44.9,
        "longitude": -93.1,
        "gps_accuracy": 5,
        "speed": 12.3,
        "gps_degree": 90,
    },
    "in_geofence": True,
}


class TestDeviceTrackerAttrs(unittest.TestCase):
    """Regression coverage for `_device_tracker_attrs` /
    `publish_vehicle_position` — new in this pass, replacing the dead
    `publish_counters()` (see CLAUDE.md/audit finding: DroneMobile
    fetched position but never published a device_tracker)."""

    def test_attrs_present_with_position(self):
        attrs = main._device_tracker_attrs(VEHICLE_PAYLOAD_WITH_POSITION)
        self.assertIsNotNone(attrs)
        self.assertEqual(attrs["latitude"], 44.9)
        self.assertEqual(attrs["longitude"], -93.1)
        self.assertEqual(attrs["source_type"], "gps")
        self.assertEqual(attrs["speed"], 12.3)
        self.assertEqual(attrs["course"], 90)

    def test_none_without_position(self):
        self.assertIsNone(main._device_tracker_attrs({"last_known_state": {}}))
        self.assertIsNone(main._device_tracker_attrs({}))

    def test_none_on_malformed_position(self):
        payload = {"last_known_state": {"latitude": "not-a-number", "longitude": -93.1}}
        self.assertIsNone(main._device_tracker_attrs(payload))

    def test_publish_counters_removed(self):
        # The dead Phase-B counter recalculator was deleted in favor of
        # publish_vehicle_position — assert it's actually gone, not just
        # unused, so it can't silently come back via a bad merge.
        self.assertFalse(hasattr(main, "publish_counters"))


# -------------------------------------------------------------- discovery


class TestDiscoverySpecsVehicle(unittest.TestCase):
    def setUp(self):
        self.vehicle = parsers.Vehicle(
            vehicle_id="12345", name="M3", make="BMW", model="M3",
            year=2011, vin="WBSWD93549PY00000",
        )

    def test_device_tracker_entity_present(self):
        items = discovery.discovery_specs_vehicle(
            self.vehicle, "dronemobile", "dronemobile/bridge/online", [],
        )
        dt_entries = [i for i in items if i[0] == "device_tracker"]
        self.assertEqual(len(dt_entries), 1, "expected exactly one device_tracker entity")
        component, unique_id_suffix, payload = dt_entries[0]
        self.assertEqual(component, "device_tracker")
        self.assertTrue(unique_id_suffix.endswith("/device_tracker"))
        self.assertEqual(payload["source_type"], "gps")
        self.assertEqual(payload["payload_home"], "home")
        self.assertEqual(payload["payload_not_home"], "not_home")
        self.assertEqual(
            payload["state_topic"], "dronemobile/12345/device_tracker/state",
        )
        self.assertEqual(
            payload["json_attributes_topic"], "dronemobile/12345/device_tracker/attrs",
        )

    def test_device_tracker_unique_id_stable(self):
        items = discovery.discovery_specs_vehicle(
            self.vehicle, "dronemobile", "dronemobile/bridge/online", [],
        )
        dt_payload = next(p for c, _, p in items if c == "device_tracker")
        self.assertEqual(dt_payload["unique_id"], "dronemobile_mqtt_bridge_12345_device_tracker")


if __name__ == "__main__":
    unittest.main()
