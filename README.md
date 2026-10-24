# asdb2mqtt
Fetches airplane data from asdb and updates Home Assistant sensor via mqtt
# ADSB2MQTT

Replaces the unreliable FlightRadar24 connection with
[adsb.lol](https://adsb.lol): polls aircraft around your location and
publishes them to MQTT, where Home Assistant picks them up via MQTT
discovery as **`sensor.flightradar24_current_in_area`** — the same entity
your `flightradar24.yaml` template sensor already reads
(`state_attr('sensor.flightradar24_current_in_area', 'flights')`).

Works as a drop-in feed; no HA custom component needed (the supported
`vingerha/ha_adsb_lol` integration is unmaintained, and an MQTT sensor
survives HA version changes).

To reduce load on asdb, configurable on-off hours and switch for refresh. 

## Payload compatibility

Each entry in the `flights` attribute uses the flightradar24 component's
key names where they exist, so the entry/exit/live-refresh branches in
`flightradar24.yaml` keep working:

| key | meaning |
|---|---|
| `flight_number`, `callsign` | callsign, e.g. `FIN1147` |
| `hex_id` | ICAO 24-bit address (stable per-aircraft id) |
| `distance` | km from your point **now** |
| `closest_distance` | sticky minimum km seen so far (like FR24) |
| `on_ground` | `alt_baro == "ground"` |
| `latitude`, `longitude`, `altitude` (ft), `ground_speed` (kt), `track`, `heading`, `vertical_speed`, `squawk`, `emergency` | live motion data |
| `registration`, `aircraft_code`, `aircraft_type`, `category` | airframe info |
| `first_seen`, `last_seen`, `seen_age_s`, `rssi` | feed freshness |

Sensor state = number of aircraft. Aircraft are kept in the list for
`stale_after` seconds (default 90) after their last sighting so short dropouts
don't flap the entry/exit events.

## Install

```bash
cd ADSB2MQTT
pip install -r requirements.txt
cp adsb2mqtt.yaml adsb2mqtt.yaml.bak   # then edit lat/lon/mqtt_host
python3 adsb2mqtt.py --once --dry-run  # sanity check against live API
python3 adsb2mqtt.py                   # start publishing
```

HA needs an MQTT integration connected to the same broker; the sensor is
auto-discovered (check `config/entities` on the MQTT integration if you want
it under a different area/device).

## Interval & polling window

`interval` in `adsb2mqtt.yaml`, or `--interval 20`, or
`ADSB2MQTT_INTERVAL=20` env var (handy in the systemd unit via
`Environment=`). Minimum 1 s.

`active_hours` limits polling to a local-time window, e.g. `"08:00-24:00"`
(also `--active-hours`, `ADSB_ACTIVE_HOURS`, or `null` for 24/7). Wraps past
midnight (`"22:00-06:00"`). Outside the window **zero** adsb.lol requests are
made and the last payload stays in HA (`attributes.polling: idle`,
`attributes.updated_at` shows data age). Window edges are picked up within
~30 s without restarting.

Quota math: 16 h/day at 120 s = 480 polls < 500/day anonymous limit —
10:00–24:00 style schedules work without an API key.

## Update switch

Polling runs **only while the update switch is ON**. Turning it ON fetches
immediately, then continues on the normal `interval`; turning it OFF stops
all adsb.lol requests (the last payload stays in HA,
`attributes.polling: off`). The switch state is retained on MQTT and
restored when the script restarts, so an OFF switch survives a reboot; on a
clean broker the script boots per `start_enabled:` in the YAML
(default on, `--start-disabled` on the CLI).

MQTT discovery creates the switch entity
`switch.flightradar24_current_in_area_updates` ("Updates") on the same
device. Its config is published under the HA discovery prefix
(`discovery_prefix:`, default `homeassistant`, i.e.
`homeassistant/switch/flightradar24_current_in_area_updates/config`) and
also on the legacy bare topic `switch/...` — HA only listens under its
configured prefix, and dedupes by `uniq_id`, so exactly one entity appears
either way. Programmatically:

```bash
mosquitto_pub -h <broker> -t sensor/flightradar24_current_in_area/set_update -m ON
mosquitto_pub -h <broker> -t sensor/flightradar24_current_in_area/set_update -m OFF
```

Accepted payloads: `ON`/`OFF` (also `Update`/`trigger`/`1` as ON,
`Idle`/`0`/`false`/`stop` as OFF). Sending `Update` while the switch is
already ON forces an immediate poll without disturbing the schedule.
Polling happens only when the switch is ON **and** the `active_hours`
window is open (if configured).

Payload attributes `poll_reason` and `polling` (`active` / `idle` / `off`)
tell you why and when the data last moved.

## Enrichment: airline, aircraft model, route

Each flight in `attributes.flights` gets extra human-readable fields:

| field | example | source |
|---|---|---|
| `airline` | `Finnair` | offline table (ICAO code from callsign prefix) |
| `aircraft_model` | `Boeing 737-800` | offline table (ICAO type designator) |
| `origin_iata` / `origin_airport` | `HEL` / `Helsinki Vantaa Airport` | adsbdb.com (background lookup) |
| `destination_iata` / `destination_airport` | `KTT` / `Kittilä Airport` | adsbdb.com |
| `route` | `HEL -> KTT` | derived |
| `flight_number_iata` | `AY451` | adsbdb.com (when known) |

Notes:

- ADS-B itself does not carry route/airline/type names — they are looked
  up. Airline + model are fully offline (`data/airlines.json`,
  `data/types.json`; rebuild with `python3 refresh_databases.py`).
- Routes come from the free community API **adsbdb.com**: one request per
  *new callsign*, at most one per `route_rate` (1.2 s) in a background
  thread, cached 12 h in `data/routes.json`. Scheduled airlines are well
  covered; charters, military and general aviation often are not — those
  flights simply have no route fields.
- Turn route lookups off with `routes: false` / `--no-routes`; airline and
  model names stay on, keeping everything offline.

## Rate limits (important for 10 s polling)

Anonymous adsb.lol access is capped (visible in response headers):
**4 req/s, 60 req/min, 500 req/day**. 10 s polling needs 8640 req/day —
you will hit the daily cap without a key. On 429 the script backs off,
logs it, and keeps re-publishing the last good payload (marked in
`updated_at`), so HA never shows garbage. For continuous 10 s updates get
a free API key (see https://adsb.lol / github.com/absobsd/adsb.lol) and set
`api_key:` in the YAML or `ADSB_LOL_API_KEY`.

If you'd rather stay anonymous, use the polling window above (e.g.
`active_hours: "08:00-24:00"` + `interval: 120` = 480 polls/day, fits the
quota) or `--interval 180` 24/7.

## API note (why not reuse ha_adsb_lol's code)

The old component builds `/v2/lat/{lat}/lon/{lon}/radius/{nm}` which now
returns HTTP 400. The current endpoint (verified live) is:

```
GET /v2/point/{lat}/{lon}/{radius_km}     # radius integer km, 1..250
```

Response `ac[]` items carry `dst` (km from the point) and `dir`, which this
script maps to `distance`/`heading`.

## Run as a service

```bash
sudo cp -r . /opt/ADSB2MQTT
sudo cp adsb2mqtt.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now adsb2mqtt
```

## Files

- `adsb2mqtt.py` — the script (stdlib + requests + paho-mqtt + PyYAML)
- `adsb2mqtt.yaml` — configuration (edit lat/lon/radius/mqtt_host)
- `refresh_databases.py` — rebuild the offline airline/type tables
- `data/` — `airlines.json`, `types.json` (offline lookup tables),
  `routes.json` (adsbdb route cache, created at runtime)
- `adsb2mqtt.service` — systemd unit template
- `test/` — capture files from local end-to-end test + Jinja harness that
  verifies branch 3 of your `flightradar24.yaml` against the real payload
- `adsb_sample.json`, `resp.json` — raw adsb.lol samples used during
  development (safe to delete)
