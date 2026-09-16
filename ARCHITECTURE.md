# Architecture

A single Python process authenticates against DroneMobile's Cognito user
pool, discovers every vehicle on the account, and polls DroneMobile's REST
API on three independent cadences, publishing Home Assistant MQTT Discovery
entities plus per-poll state updates and one-shot history events.

```
Cognito (AWS)                    api.dronemobile.com/api/v1/
 InitiateAuth                     vehicle, vehicle/{id}, iot/logs,
 (USER_PASSWORD_AUTH)             alert/event, device, alert/rule,
   │  IdToken, 1h TTL             geofence, user, firmware-update
   ▼                                        │
main.py  ── discover_vehicles() ────────────┤
  │  FAST (60s)   /vehicle/{id} telemetry   │
  │  MED (300s)   iot/logs + alert/event    │  status.dronemobile.com/api/v2/
  │               incremental + StatusPage ─┼─► incidents, maintenances
  │  SLOW (3600s) device/rules/geofences/   │
  │               users/firmware            │
  ▼
ThreadedPublisher (ha_mqtt_bridge) ──publish──►  MQTT broker  ──►  Home Assistant
```

## Layout

| Path | Role |
|---|---|
| `app/main.py` | Env config, Cognito auth (+ re-auth on expiry), REST client, publish helpers, the tiered scheduler, and per-vehicle iteration. |
| `app/parsers.py` | `Vehicle` / `IotLog` / `AlertEvent` / `DeviceInfo` / `AlertRule` / `Geofence` / `AccountUsers` / `Firmware` / `StatusPageSnapshot` dataclasses, their `parse_*` functions, and derivation helpers (compass direction, signal-quality bucket, carrier name, haversine geofence membership). |
| `app/discovery.py` | HA Discovery payload factory. `ENTITIES` (per-vehicle telemetry), `PHASE_A_VEHICLE_ENTITIES` (iot_log/alert last-event mirrors), `PHASE_B_ACCOUNT_ENTITIES` / `PHASE_B_VEHICLE_FIRMWARE_ENTITIES`, `PHASE_C_SERVICE_ENTITIES`, and a per-geofence inside-binary factory. |
| `app/events.py` | `IotLogsStream` / `AlertEventStream` — incremental-poll + startup-backfill pagination over the two paginated endpoints, with a bounded seen-id cache — and `fetch_statuspage()`, a no-auth client for DroneMobile's Atlassian StatusPage. |
| `Dockerfile` | Installs the vendored `ha_mqtt_bridge` and `python_github_error_reporter` packages, then the app's own requirements. Application code is bind-mounted at runtime, not baked into the image. |
| `docker-compose.example.yml` | Builds the image with both packages wired in via Compose `additional_contexts` pointing at `./_shared/...`. |

## Shared code

Two packages are vendored into this repository under `_shared/` at publish
time rather than maintained twice — the same copies used by the author's
other MQTT bridges:

- **`ha_mqtt_bridge`** — the MQTT client (`ThreadedPublisher`: connect, LWT,
  reconnect, publish flavors), HA Discovery payload construction,
  device-block assembly, and epoch→ISO-8601 timestamp formatting.
- **`python_github_error_reporter`** — an opt-in uncaught-exception reporter.
  It installs `sys.excepthook` and `threading.excepthook`; when
  `GITHUB_ERROR_TOKEN` and `GITHUB_REPO` are both set it dispatches a
  `repository_dispatch` event on the first occurrence of a given error type,
  then silently deduplicates repeats for 60 seconds. With neither variable
  set (the default) it does nothing.

## Cadences

| Tier | Interval | What it does |
|---|---|---|
| FAST | 60 s (`FAST_POLL_INTERVAL`) | Per-vehicle `GET /vehicle/{id}` telemetry (~29 fields) and, once geofences are known, an `inside_geofence_<slug>` binary per configured geofence. |
| MED | 300 s (`MED_POLL_INTERVAL`) | Incremental `GET /iot/logs` and `GET /alert/event` (last 7 days, page 1 only — new records since the last poll are republished), a cheap dedicated 24-hour count for each, and a StatusPage refresh. |
| SLOW | 3600 s (`SLOW_POLL_INTERVAL`) | `GET /device` (plan + capabilities), `GET /alert/rule`, `GET /geofence`, `GET /user`, and `GET /firmware-update` per vehicle (compares against the vehicle's current firmware). |

Every tier handler catches its own exceptions so one failing call cannot stall
the others; a `PermissionError` (401) anywhere triggers an immediate
Cognito re-auth via `USER_PASSWORD_AUTH` password grant.

## Startup backfill

On first successful discovery, the bridge walks `iot/logs` and `alert/event`
backward from today by `EVENT_BACKFILL_DAYS` (default 30, capped by the API's
own roughly 365-day retention), publishing each record as a one-shot MQTT
event under `<prefix>/<vehicle_id>/events/{iot_log,alert}` with the record's
original timestamp embedded in the payload — useful for backfilling a
time-series database, not for Home Assistant itself.

## Multi-vehicle

`discover_vehicles()` calls `GET /vehicle` and parses every result; if that
returns empty it falls back to `DRONEMOBILE_VEHICLE_ID` as a hint. Every tier
handler iterates the discovered vehicle list, so an account with more than
one vehicle gets a full entity set — and its own event streams — per vehicle,
while the Account and Service devices publish once regardless of vehicle
count.

## What's deliberately not implemented

- **Mutation endpoints** (`POST /iot/command` for lock/unlock/remote-start,
  geofence create/update, notification-token registration) are not called.
  The official Home Assistant `drone_mobile` integration already exposes
  those controls; this bridge only reads.
- **`GET /vehicle/{id}/obd`** is plan-gated and returns an error on plans that
  don't include it — the bridge does not call it.
- **Refresh-token auth.** The DroneMobile iOS app uses a Cognito
  `GetTokensFromRefreshToken` flow that requires implementing `ConfirmDevice`
  after the first login to obtain a usable device key. The bridge instead
  re-runs the `USER_PASSWORD_AUTH` password grant on every token expiry
  (roughly hourly), which is well under any observed rate limit.
