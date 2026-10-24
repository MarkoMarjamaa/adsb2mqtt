#!/usr/bin/env python3
"""ADSB2MQTT — replace the unreliable FlightRadar24 feed with adsb.lol.

Polls https://api.adsb.lol for aircraft around a point and publishes the
result to Home Assistant over MQTT discovery as an MQTT sensor named
"Flightradar24 current in area" (entity sensor.flightradar24_current_in_area).

The payload mimics the `flights` attribute of the
home-assistant-flightradar24 custom component (entries keyed by
`flight_number`, `hex_id`, `distance`, `closest_distance`, `on_ground`, ...)
so existing trigger-based template sensors keep working.

NOTE on the adsb.lol API: the old ha_adsb_lol component builds
  /v2/lat/{lat}/lon/{lon}/radius/{nm}
which now returns HTTP 400. The current endpoint is
  /v2/point/{lat}/{lon}/{radius_km}
(radius in kilometres, km suffix not accepted). Distances in the response
(`dst`) are km from the searched point.

Quick start:
  python3 adsb2mqtt.py --lat 60.2 --lon 24.9 --radius-km 30 \
      --mqtt-host 192.168.1.10 --interval 10 --active-hours "08:00-24:00"

Everything can also live in adsb2mqtt.yaml (same directory); CLI wins.

Polling window: --active-hours "08:00-24:00" (local time, wraps past
midnight) polls adsb.lol only inside the window, keeping the anonymous
500 req/day quota: 16 h at 120 s = 480 polls. Outside the window nothing
is requested; the last payload stays published (attributes.polling shows
"idle"). Window edges are honoured within ~30 s without restarts.

Update switch: polling runs only while the update switch is ON. The
discovered switch (switch.flightradar24_current_in_area_updates,
"Updates") or any ON/OFF payload on <base_topic>/set_update turns
polling on/off. Turning it ON fetches immediately, then polls on the
normal interval; OFF stops all adsb.lol requests and the last payload
stays published. The state is retained on <base_topic>/set_update/state
and restored after a script restart (start_enabled in the config is the
fallback for a clean broker). Sending "Update" while already ON forces
an immediate poll.

Enrichment: every flight also gets 'airline' (name from the callsign's
ICAO code) and 'aircraft_model' (ICAO type designator -> "Boeing 737-800")
from bundled offline tables, plus — via the free adsbdb.com API, one
background lookup per new callsign, cached — the route: origin_iata,
origin_airport, destination_iata, destination_airport, route. Unknown
flights (charters, GA) simply lack these fields. Disable route lookups
with --no-routes.

adsb.lol anonymous limits (from response headers): 4 req/s, 60/min,
500 req/day. A 10 s interval exceeds the daily quota; the script backs off
automatically on HTTP 429 and keeps the last good payload published. For
continuous 24/7 operation get a free API key (see https://adsb.lol /
github.com/absobsd/adsb.lol) and pass it via --api-key / ADSB_LOL_API_KEY
(sent as Authorization: Bearer).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import queue
import re
import signal
import sys
import threading
import time
from typing import Any

import requests

try:
    import yaml
except ImportError:  # optional — YAML config just becomes unavailable
    yaml = None

LOG = logging.getLogger("adsb2mqtt")

DEFAULTS = {
    # --- adsb.lol ---
    "api_url": "https://api.adsb.lol",
    "lat": None,
    "lon": None,
    "radius_km": 30,
    "api_key": None,            # optional, raises rate limits
    "http_timeout": 15,
    # --- loop ---
    "interval": 10.0,           # seconds between ADSB polls (adjustable)
    "active_hours": None,       # e.g. "08:00-24:00" (local/TZ); None = 24/7.
                                # Outside the window no adsb.lol requests are
                                # made (same as the update switch being OFF).
    "stale_after": 90.0,        # drop a flight from the list after this many
                                # seconds without being seen again
    "start_enabled": True,      # master switch state at boot when the broker
                                # holds no retained switch state. The switch
                                # state is retained + restored across restarts.
    "max_flights": 50,
    # --- enrichment ---
    "routes": True,             # look up flight route (origin/destination
                                # airport names) via the free adsbdb.com API
    "adsbdb_url": "https://api.adsbdb.com/v0",
    "route_cache_file": None,   # default: data/routes.json next to script
    "route_ttl": 12 * 3600,     # reuse a cached route for this long
    "route_fail_ttl": 6 * 3600, # negative-cache failures for this long
    "route_rate": 1.2,          # min seconds between adsbdb requests
    "min_distance_round": 3,    # decimals for distance/closest_distance (km)
    # --- MQTT ---
    "mqtt_host": "localhost",
    "mqtt_port": 1883,
    "mqtt_username": None,
    "mqtt_password": None,
    "mqtt_base_topic": "sensor/flightradar24_current_in_area",
    "discovery_prefix": "homeassistant",  # HA MQTT discovery prefix; switch
                                # config is published to
                                # <prefix>/switch/<uid>_updates/config.
                                # Set '' to skip (legacy bare topics only).
    "mqtt_client_id": "adsb2mqtt",
    "mqtt_keepalive": 60,
    "mqtt_qos": 0,
    "mqtt_retain": True,
    "device_name": "ADSB2MQTT",
    "sensor_name": "Flightradar24 current in area",
    "unique_id": "flightradar24_current_in_area",
}


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------

def load_config(argv: list[str] | None = None) -> dict[str, Any]:
    ap = argparse.ArgumentParser(
        description="Publish aircraft seen by adsb.lol to a Home Assistant "
                    "MQTT sensor compatible with the flightradar24 component "
                    "payload shape.")
    ap.add_argument("-c", "--config",
                    default=os.environ.get("ADSB2MQTT_CONFIG",
                                           os.path.join(script_dir(),
                                                        "adsb2mqtt.yaml")),
                    help="YAML config file (default: adsb2mqtt.yaml next to "
                         "this script, or $ADSB2MQTT_CONFIG)")
    ap.add_argument("--lat", type=float, help="centre latitude (required)")
    ap.add_argument("--lon", type=float, help="centre longitude (required)")
    ap.add_argument("--radius-km", type=float, dest="radius_km")
    ap.add_argument("--api-url", dest="api_url")
    ap.add_argument("--api-key", dest="api_key",
                    help="adsb.lol API key (or $ADSB_LOL_API_KEY)")
    ap.add_argument("--interval", type=float,
                    help="poll interval in seconds (default 10)")
    ap.add_argument("--active-hours", dest="active_hours",
                    help='polling window, e.g. "08:00-24:00" or "06:00-22:30" '
                         '(local time, wraps past midnight, 24:00 allowed). '
                         'Default: poll 24/7')
    ap.add_argument("--stale-after", type=float, dest="stale_after",
                    help="seconds without a sighting before a flight is "
                         "dropped from the list (default 90)")
    ap.add_argument("--max-flights", type=int, dest="max_flights")
    ap.add_argument("--routes", action=argparse.BooleanOptionalAction,
                    default=None, dest="routes",
                    help="look up flight routes (origin/destination airports) "
                         "via adsbdb.com (default on); --no-routes keeps "
                         "enrichment fully offline (airline + aircraft model "
                         "names from bundled tables)")
    ap.add_argument("--mqtt-host", dest="mqtt_host")
    ap.add_argument("--mqtt-port", type=int, dest="mqtt_port")
    ap.add_argument("--mqtt-username", dest="mqtt_username")
    ap.add_argument("--mqtt-password", dest="mqtt_password")
    ap.add_argument("--mqtt-base-topic", dest="mqtt_base_topic",
                    help="base topic; discovery goes to <base>/config, "
                         "payload to <base>/state")
    ap.add_argument("--discovery-prefix", dest="discovery_prefix",
                    help="HA MQTT integration discovery prefix "
                         "(default 'homeassistant'; '' disables prefixed "
                         "switch discovery)")
    ap.add_argument("--mqtt-client-id", dest="mqtt_client_id")
    ap.add_argument("--once", action="store_true",
                    help="fetch once, publish, exit (dry testing)")
    ap.add_argument("--start-disabled", action="store_true",
                    dest="start_disabled",
                    help="boot with the update switch OFF (only when the "
                         "broker holds no retained switch state)")
    ap.add_argument("--dry-run", action="store_true", dest="dry_run",
                    help="fetch and print the payload, do not touch MQTT")
    ap.add_argument("-v", "--verbose", action="store_true")

    args = ap.parse_args(argv)
    cfg = dict(DEFAULTS)

    # 1. YAML file
    if args.config and os.path.exists(args.config) and yaml is not None:
        try:
            with open(args.config) as fh:
                data = yaml.safe_load(fh) or {}
            unknown = set(data) - set(DEFAULTS)
            if unknown:
                LOG.warning("ignoring unknown config keys: %s", sorted(unknown))
            cfg.update({k: v for k, v in data.items() if k in DEFAULTS})
            LOG.info("loaded config from %s", args.config)
        except Exception as exc:
            LOG.error("cannot parse %s: %s", args.config, exc)
            sys.exit(2)

    # 2. environment
    env_map = {
        "ADSB_LAT": ("lat", float), "ADSB_LON": ("lon", float),
        "ADSB_RADIUS_KM": ("radius_km", float),
        "ADSB_LOL_API_KEY": ("api_key", str),
        "MQTT_HOST": ("mqtt_host", str),
        "MQTT_PORT": ("mqtt_port", int),
        "MQTT_USERNAME": ("mqtt_username", str),
        "MQTT_PASSWORD": ("mqtt_password", str),
        "ADSB2MQTT_INTERVAL": ("interval", float),
        "ADSB_ACTIVE_HOURS": ("active_hours", str),
        "ADSB2MQTT_DISCOVERY_PREFIX": ("discovery_prefix", str),
        "ADSB2MQTT_START_ENABLED": ("start_enabled",
                                    lambda v: v.lower() in
                                    ("1", "true", "yes", "on")),
    }
    for env, (key, cast) in env_map.items():
        val = os.environ.get(env)
        if val is not None and val != "":
            cfg[key] = cast(val)

    # 3. CLI
    cfg.update({k: v for k, v in vars(args).items()
                if v is not None and k in DEFAULTS})
    if args.start_disabled:
        cfg["start_enabled"] = False

    if cfg["lat"] is None or cfg["lon"] is None:
        ap.error("--lat and --lon are required (via CLI, YAML, or env)")
    if cfg["interval"] < 1:
        ap.error("--interval must be >= 1 second")
    if cfg["active_hours"] is not None:
        try:
            parse_active_hours(str(cfg["active_hours"]))
        except ValueError as exc:
            ap.error(f"--active-hours: {exc}")
    if not 1 <= cfg["radius_km"] <= 250:
        ap.error("adsb.lol point radius must be between 1 and 250 km")
    cfg["dry_run"] = args.dry_run
    cfg["once"] = args.once
    return cfg


def script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def parse_active_hours(spec: str) -> tuple[int, int]:
    """'08:00-24:00' -> (480, 1440) minutes-of-day; raises ValueError."""
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*", spec)
    if not m:
        raise ValueError(f"expected HH:MM-HH:MM, got {spec!r}")
    h1, m1, h2, m2 = (int(g) for g in m.groups())
    if not (0 <= h1 <= 24 and 0 <= m1 < 60 and 0 <= h2 <= 24 and 0 <= m2 < 60):
        raise ValueError(f"hours 00:00..24:00 only, got {spec!r}")
    return h1 * 60 + m1, h2 * 60 + m2


def in_active_window(spec: str | None, when: time.struct_time) -> bool:
    """True if `when` (local time) falls inside the polling window.

    Window wraps past midnight when start > end (e.g. '22:00-06:00').
    A zero-length window (start == end) is treated as inactive all day;
    use active_hours: null for 24/7.
    """
    if not spec:
        return True
    start, end = parse_active_hours(spec)
    now = when.tm_hour * 60 + when.tm_min
    if start == end:
        return False
    if start < end:
        return start <= now < end
    return now >= start or now < end


# --------------------------------------------------------------------------
# adsb.lol client
# --------------------------------------------------------------------------

class AdsbLol:
    """Thin client for api.adsb.lol v2 point search."""

    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "ADSB2MQTT/1.0 (Home Assistant)"
        if cfg.get("api_key"):
            self.session.headers["Authorization"] = f"Bearer {cfg['api_key']}"
        self.backoff = 0.0  # >0 while rate limited

    @property
    def url(self) -> str:
        # current API form; /v2/lat/../radius/.. (used by the old ha_adsb_lol
        # component) is dead and returns 400
        return (f"{self.cfg['api_url'].rstrip('/')}/v2/point/"
                f"{self.cfg['lat']:.5f}/{self.cfg['lon']:.5f}/"
                f"{int(round(self.cfg['radius_km']))}")

    def fetch(self) -> list[dict[str, Any]] | None:
        """Return list of raw aircraft dicts, or None on failure."""
        if self.backoff > 0:
            LOG.warning("rate-limited, cooling down %.0fs more", self.backoff)
            time.sleep(min(self.backoff, self.cfg["interval"]))
            self.backoff = 0.0
        try:
            resp = self.session.get(self.url,
                                    timeout=self.cfg["http_timeout"])
        except requests.RequestException as exc:
            LOG.error("adsb.lol request failed: %s", exc)
            return None
        if resp.status_code == 429:
            retry = float(resp.headers.get("Retry-After", 60) or 60)
            self.backoff = min(max(retry, self.cfg["interval"]), 600)
            LOG.warning("adsb.lol rate limit (429); backing off %.0fs. "
                        "Consider a free API key for 10s polling.",
                        self.backoff)
            return None
        if resp.status_code in (401, 403):
            LOG.error("adsb.lol auth error %s — check api_key",
                      resp.status_code)
            return None
        if resp.status_code != 200:
            LOG.error("adsb.lol HTTP %s for %s", resp.status_code, self.url)
            return None
        try:
            data = resp.json()
        except ValueError:
            LOG.error("adsb.lol returned non-JSON body (%d bytes)",
                      len(resp.content))
            return None
        return data.get("ac") or []


# --------------------------------------------------------------------------
# flight state: track closest_distance per aircraft, expire stale ones
# --------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = (math.sin(dp / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(a))


class FlightTracker:
    """Keeps a live dict of flights with a sticky closest_distance, like the
    flightradar24 component does for its in_area list."""

    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg
        self._known: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _key(ac: dict[str, Any]) -> str:
        return (ac.get("hex") or "").lower() or ac.get("flight", "").strip()

    def update(self, aircraft: list[dict[str, Any]]) -> list[dict[str, Any]]:
        now = time.monotonic()
        rnd = self.cfg["min_distance_round"]
        lat, lon = self.cfg["lat"], self.cfg["lon"]

        fresh: dict[str, dict[str, Any]] = {}
        for ac in aircraft:
            if "lat" not in ac or "lon" not in ac:
                continue  # no position -> can't compute distance
            key = self._key(ac)
            dist = ac.get("dst")
            if dist is None:
                dist = haversine_km(lat, lon, ac["lat"], ac["lon"])
            prev = self._known.get(key)
            closest = min(dist, prev["closest_distance"]) if prev else dist

            alt = ac.get("alt_baro")
            on_ground = alt == "ground"

            entry = {
                # --- identity (FR24-compatible names where they exist) ---
                "hex_id": ac.get("hex"),
                "flight_number": (ac.get("flight") or "").strip() or None,
                "callsign": (ac.get("flight") or "").strip() or None,
                "registration": ac.get("r"),
                "aircraft_code": ac.get("t"),
                "aircraft_type": ac.get("type"),      # mlat / adsb_icao / ...
                "category": ac.get("category"),
                # --- live position & motion ---
                "latitude": ac["lat"],
                "longitude": ac["lon"],
                "altitude": None if on_ground else alt,  # ft
                "altitude_geom": ac.get("alt_geom"),
                "ground_speed": ac.get("gs"),          # kt
                "track": ac.get("track"),
                "heading": ac.get("mag_heading", ac.get("true_heading")),
                "vertical_speed": (ac.get("baro_rate")
                                   if ac.get("baro_rate") is not None
                                   else ac.get("geom_rate")),
                "squawk": ac.get("squawk"),
                "emergency": ac.get("emergency"),
                "temperature_oat": ac.get("oat"),
                "ias": ac.get("ias"),
                "mach": ac.get("mach"),
                # --- FR24-compatible distance fields (km) ---
                "distance": round(dist, rnd),
                "closest_distance": round(closest, rnd),
                "on_ground": on_ground,   # adsb.lol reports alt_baro="ground"
                # --- extras ---
                "seen_age_s": ac.get("seen"),
                "rssi": ac.get("rssi"),
            }
            entry["first_seen"] = (prev or {}).get("first_seen") or \
                time.strftime("%Y-%m-%dT%H:%M:%S%z")
            entry["_last_seen"] = now
            entry["last_seen"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
            fresh[key] = entry

        # drop stale aircraft (not seen for stale_after seconds)
        alive = {k: v for k, v in self._known.items()
                 if now - v["_last_seen"] < self.cfg["stale_after"]}
        alive.update(fresh)
        self._known = alive

        flights = sorted(self._known.values(),
                         key=lambda f: f["closest_distance"])
        return flights[: self.cfg["max_flights"]]


# --------------------------------------------------------------------------
# enrichment: airline name, aircraft model name, flight route
# --------------------------------------------------------------------------

class Enricher:
    """Adds human-readable details to each flight dict.

    - airline / aircraft_model: offline lookup tables in data/ (built by
      refresh_databases.py); if a table file is missing that part is
      silently skipped.
    - route (origin/destination airport names): adsbdb.com callsign API.
      One worker thread, max one request per route_rate seconds, results
      cached to a JSON file with route_ttl validity — a flight line is
      looked up once, not every poll. Missing data simply means the
      fields are absent for that flight (charters, military, GA...).
    """

    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg
        self.routes_enabled = bool(cfg.get("routes", True))
        self.airlines: dict[str, str] = {}
        self.types: dict[str, str] = {}
        data_dir = os.path.join(script_dir(), "data")
        try:
            with open(os.path.join(data_dir, "airlines.json")) as fh:
                self.airlines = json.load(fh)
        except OSError:
            LOG.warning("data/airlines.json missing; airline names disabled "
                        "(run refresh_databases.py)")
        try:
            with open(os.path.join(data_dir, "types.json")) as fh:
                self.types = json.load(fh)
        except OSError:
            LOG.warning("data/types.json missing; aircraft model names "
                        "disabled (run refresh_databases.py)")

        self.cache_file = (cfg.get("route_cache_file")
                           or os.path.join(data_dir, "routes.json"))
        self.cache: dict[str, dict[str, Any]] = {}
        try:
            with open(self.cache_file) as fh:
                self.cache = json.load(fh)
            LOG.info("loaded %d cached flight routes", len(self.cache))
        except (OSError, ValueError):
            pass

        self._queue: "queue.Queue[str | None]" = queue.Queue()
        self._pending: set[str] = set()
        self._lock = threading.Lock()
        self._dirty = False
        self._last_request = 0.0
        self._thread: threading.Thread | None = None
        if self.routes_enabled:
            self._thread = threading.Thread(target=self._worker,
                                            name="route-lookup", daemon=True)
            self._thread.start()

    # ---- public ----

    def annotate(self, flights: list[dict[str, Any]]) -> None:
        """Enrich flight dicts in place (idempotent, no network here)."""
        for f in flights:
            cs = (f.get("flight_number") or "").strip().upper()
            if cs:
                code = cs[:3]
                if code.isalpha():
                    name = self.airlines.get(code)
                    if name:
                        f["airline"] = name
            model = self.types.get((f.get("aircraft_code") or "").upper())
            if model:
                f["aircraft_model"] = model
            if self.routes_enabled and cs:
                entry = self._fresh_entry(cs)
                if entry is not None and not entry.get("fail"):
                    self._apply_route(f, entry)
                elif entry is None:
                    self._enqueue(cs)

    def drain(self, timeout: float = 10.0) -> None:
        """Wait up to timeout s for queued route lookups (used by --once)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if not self._pending:
                    break
            time.sleep(0.2)

    def stop(self) -> None:
        if self._thread is not None:
            self._queue.put(None)
            self._thread.join(timeout=5)
        self._save_cache()

    # ---- internals ----

    def _fresh_entry(self, cs: str) -> dict[str, Any] | None:
        entry = self.cache.get(cs)
        if not entry:
            return None
        ttl = (self.cfg["route_fail_ttl"] if entry.get("fail")
               else self.cfg["route_ttl"])
        if time.time() - entry.get("ts", 0) > ttl:
            return None
        return entry

    def _enqueue(self, cs: str) -> None:
        with self._lock:
            if cs in self._pending:
                return
            self._pending.add(cs)
        self._queue.put(cs)

    @staticmethod
    def _apply_route(f: dict[str, Any], entry: dict[str, Any]) -> None:
        if entry.get("o"):
            f["origin_iata"] = entry["o"]
        if entry.get("on"):
            f["origin_airport"] = entry["on"]
        if entry.get("d"):
            f["destination_iata"] = entry["d"]
        if entry.get("dn"):
            f["destination_airport"] = entry["dn"]
        if entry.get("o") and entry.get("d"):
            f["route"] = f"{entry['o']} -> {entry['d']}"
        if entry.get("cs_iata"):
            f["flight_number_iata"] = entry["cs_iata"]
        if entry.get("al") and not f.get("airline"):
            f["airline"] = entry["al"]

    def _worker(self) -> None:
        while True:
            cs = self._queue.get()
            if cs is None:
                break
            # rate limit: >= route_rate seconds between adsbdb requests
            wait = (self.cfg["route_rate"]
                    - (time.monotonic() - self._last_request))
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()
            try:
                self._lookup(cs)
            except Exception as exc:   # noqa: BLE001 - never die on one flight
                LOG.debug("route lookup %s failed: %s", cs, exc)
                self._store(cs, {"fail": True, "ts": time.time()})
            with self._lock:
                self._pending.discard(cs)
            if self._dirty and time.time() - getattr(
                    self, "_last_save", 0) > 30:
                self._save_cache()

    def _lookup(self, cs: str) -> None:
        url = f"{self.cfg['adsbdb_url'].rstrip('/')}/callsign/{cs}"
        resp = requests.get(url, timeout=10,
                            headers={"User-Agent": "adsb2mqtt/1.0"})
        route = ((resp.json() or {}).get("response") or {}).get("flightroute")
        if resp.status_code != 200 or not route:
            self._store(cs, {"fail": True, "ts": time.time()})
            return
        origin = route.get("origin") or {}
        dest = route.get("destination") or {}
        airline = route.get("airline") or {}
        entry = {
            "ts": time.time(),
            "o": origin.get("iata_code") or origin.get("icao_code"),
            "on": origin.get("name"),
            "d": dest.get("iata_code") or dest.get("icao_code"),
            "dn": dest.get("name"),
            "al": airline.get("name"),
            "cs_iata": route.get("callsign_iata"),
        }
        LOG.debug("route %s: %s -> %s", cs, entry["o"], entry["d"])
        self._store(cs, entry)

    def _store(self, cs: str, entry: dict[str, Any]) -> None:
        self.cache[cs] = entry
        self._dirty = True

    def _save_cache(self) -> None:
        if not self._dirty:
            return
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            tmp = self.cache_file + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(self.cache, fh)
            os.replace(tmp, self.cache_file)
            self._dirty = False
            self._last_save = time.time()
        except OSError as exc:
            LOG.warning("cannot write route cache %s: %s",
                        self.cache_file, exc)


# --------------------------------------------------------------------------
# payload
# --------------------------------------------------------------------------

def build_payload(flights: list[dict[str, Any]], cfg: dict[str, Any],
                  ok: bool, poll_reason: str = "scheduled",
                  enabled: bool = True) -> str:
    state = str(len(flights)) if ok else "unavailable"
    public = [{k: v for k, v in f.items() if not k.startswith("_")}
              for f in flights]
    if not enabled:
        polling = "off"
    elif in_active_window(cfg.get("active_hours"), time.localtime()):
        polling = "active"
    else:
        polling = "idle"
    body = {
        "state": state,
#        "attributes": {
            "flights": public,
            "count": len(public),
            "unit_of_measurement": "aircraft",
            "icon": "mdi:airplane",
            "lat": cfg["lat"],
            "lon": cfg["lon"],
            "radius_km": cfg["radius_km"],
            "source": "adsb.lol",
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "poll_reason": poll_reason,        # scheduled
            "polling": polling,                # active|idle|off
            "active_hours": cfg.get("active_hours") or "24/7",
            "interval_s": cfg["interval"],
            "friendly_name": cfg["sensor_name"],
#        },
    }
    return json.dumps(body, ensure_ascii=False)


# --------------------------------------------------------------------------
# MQTT
# --------------------------------------------------------------------------

class UpdateSwitch:
    """Master gate for polling, mirrored as an HA switch entity.

    ON  -> first fetch immediately, then the normal interval loop
    OFF -> no adsb.lol requests; the last payload stays in HA

    State is retained on <base>/set_update/state and restored at startup,
    so a script restart keeps the last ON/OFF choice (falls back to
    cfg['start_enabled'] on a clean broker).
    """

    def __init__(self, initial: bool, locked: bool = False):
        self.enabled = initial
        self.locked = locked    # --once: ignore broker restore
        self.restored = locked
        self.restore_event = threading.Event()
        self.wake = threading.Event()   # wakes the main loop on changes
        if locked:
            self.restore_event.set()    # don't stall mqtt_connect on --once

    def set(self, enabled: bool):
        if enabled != self.enabled:
            self.enabled = enabled
            LOG.info("update switch turned %s", "ON" if enabled else "OFF")
        self.wake.set()


def mqtt_connect(cfg: dict[str, Any], switch: UpdateSwitch):
    """Connect, publish discovery + availability, wire the update switch.

    Command topic <base>/set_update accepts ON/OFF (HA switch payloads;
    also Update/trigger/1 treated as ON). Returns (client, state_topic).
    """
    import paho.mqtt.client as mqtt

    base = cfg["mqtt_base_topic"].strip("/")
    discovery_topic = f"{base}/config"
    state_topic = f"{base}/state"
    command_topic = f"{base}/set_update"
    switch_state_topic = f"{base}/set_update/state"

    def _on_message(_client, _userdata, msg):
        payload = (msg.payload or b"").decode("utf-8", "replace").strip()
        if msg.topic == switch_state_topic:
            # retained state delivered right after subscribe: restore once
            if not switch.restored:
                switch.enabled = payload.upper() == "ON"
                switch.restored = True
                switch.restore_event.set()
                LOG.info("update switch state restored from broker: %s",
                         "ON" if switch.enabled else "OFF")
            return
        if msg.topic == command_topic:
            low = payload.lower()
            if low in ("on", "update", "trigger", "1", "true"):
                switch.set(True)
            elif low in ("off", "stop", "0", "false", "idle"):
                switch.set(False)
            else:
                LOG.debug("ignoring switch payload %r on %s",
                          payload, msg.topic)

    discovery = {
        "uniq_id": cfg["unique_id"],
        "name": cfg["sensor_name"],
        "stat_t": state_topic,
        "val_tpl": "{{ value_json.state }}",
        "json_attr_t": state_topic,
        "dev": {
            "ids": cfg["unique_id"],
            "name": cfg["device_name"],
            "manufacturer": "adsb.lol",
            "model": "ADSB2MQTT",
        },
        "avty_t": f"{base}/availability",
        "pl_avail": "online",
        "pl_not_avail": "offline",
    }

    # master switch: polling runs only while it is ON
    switch_discovery = {
        "uniq_id": f"{cfg['unique_id']}_updates",
        "name": "Updates",
        "cmd_t": command_topic,
        "stat_t": switch_state_topic,
        "pl_on": "ON",
        "pl_off": "OFF",
        "icon": "mdi:play-pause",
        "dev": {
            "ids": cfg["unique_id"],
            "name": cfg["device_name"],
            "manufacturer": "adsb.lol",
            "model": "ADSB2MQTT",
        },
        "avty_t": f"{base}/availability",
        "pl_avail": "online",
        "pl_not_avail": "offline",
    }

    try:  # paho >= 2.0
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,
                             client_id=cfg["mqtt_client_id"])
    except (AttributeError, TypeError):  # paho 1.x
        client = mqtt.Client(client_id=cfg["mqtt_client_id"])
    if cfg.get("mqtt_username"):
        client.username_pw_set(cfg["mqtt_username"], cfg.get("mqtt_password"))

    def publish_switch_state():
        client.publish(switch_state_topic,
                       "ON" if switch.enabled else "OFF",
                       qos=1, retain=True)

    client.on_message = _on_message
    client.connect(cfg["mqtt_host"], cfg["mqtt_port"],
                   keepalive=cfg["mqtt_keepalive"])
    # subscribe BEFORE loop_start; also to our own state topic so the
    # retained switch state is delivered immediately after (re)connect
    client.subscribe(command_topic, qos=1)
    client.subscribe(switch_state_topic, qos=1)
    client.loop_start()

    # wait briefly for the retained switch state (falls back to config)
    switch.restore_event.wait(2.0)
    if not switch.restored:
        LOG.info("no retained switch state, starting %s",
                 "ENABLED" if switch.enabled else "DISABLED")
    switch.restored = True   # ignore the echo of our own publish below
    publish_switch_state()

    qos = cfg["mqtt_qos"]
    client.publish(f"{base}/availability", "online", qos=qos, retain=True)
    client.publish(discovery_topic, json.dumps(discovery), qos=1, retain=True)

    # switch discovery: HA only listens under its discovery prefix
    # (default 'homeassistant'). Publish under the prefix AND the legacy
    # bare topic — HA dedupes by uniq_id, so at most one entity appears.
    prefix = str(cfg.get("discovery_prefix") or "").strip("/")
    uid = cfg["unique_id"]
    switch_configs = [f"switch/{uid}_updates/config"]
    if prefix:
        switch_configs.append(f"{prefix}/switch/{uid}_updates/config")
    for topic in switch_configs:
        client.publish(topic, json.dumps(switch_discovery), qos=1, retain=True)
    # remove the old one-shot 'Force update' discovery if it still exists
    force_configs = [f"switch/{uid}_force/config"]
    if prefix:
        force_configs.append(f"{prefix}/switch/{uid}_force/config")
    for topic in force_configs:
        client.publish(topic, "", qos=1, retain=True)
    LOG.info("MQTT connected to %s:%s, discovery at %s, switch config at %s, "
             "switch commands -> %s",
             cfg["mqtt_host"], cfg["mqtt_port"], discovery_topic,
             " + ".join(switch_configs), command_topic)
    return client, state_topic, publish_switch_state


# --------------------------------------------------------------------------
# main loop
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.DEBUG if (argv and "-v" in argv) else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_config(argv)
    if cfg["once"]:
        cfg["active_hours"] = None  # --once always polls regardless of window
    adsb = AdsbLol(cfg)
    tracker = FlightTracker(cfg)
    enricher = Enricher(cfg)

    if cfg.get("active_hours"):
        LOG.info("polling %s every %.0fs between %s (local), stale after %.0fs",
                 adsb.url, cfg["interval"], cfg["active_hours"],
                 cfg["stale_after"])
    else:
        LOG.info("polling %s every %.0fs 24/7 (stale after %.0fs)",
                 adsb.url, cfg["interval"], cfg["stale_after"])

    # master switch: dry-run/--once always poll and ignore the broker
    # (switch restore); otherwise start from start_enabled and restore the
    # retained state from the broker
    once_like = cfg["once"] or cfg["dry_run"]
    switch = UpdateSwitch(initial=True if once_like
                          else bool(cfg.get("start_enabled", True)),
                          locked=once_like)
    client = None
    state_topic = None
    publish_switch_state = lambda: None
    if not cfg["dry_run"]:
        client, state_topic, publish_switch_state = mqtt_connect(cfg, switch)

    stop = {"flag": False}

    def _sig(_s, _f):
        stop["flag"] = True
        switch.wake.set()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    def do_poll(reason: str):
        """One fetch + publish cycle."""
        payload = None
        aircraft = adsb.fetch()
        if aircraft is not None:
            flights = tracker.update(aircraft)
            enricher.annotate(flights)
            if cfg["once"]:
                # give the route worker a moment so --once output is complete
                enricher.drain(15)
                enricher.annotate(flights)
            payload = build_payload(flights, cfg, ok=True, poll_reason=reason,
                                    enabled=switch.enabled)
            LOG.info("%d aircraft in area (%s)", len(flights), reason)
        elif last_ok_payload is not None:
            # feed hiccup: re-publish last good data (updated_at shows when
            # the data actually is from)
            payload = last_ok_payload
            LOG.warning("keeping last good payload (%d aircraft, %s)",
                        json.loads(payload)["attributes"]["count"], reason)
        else:
            payload = build_payload([], cfg, ok=False, poll_reason=reason,
                                    enabled=switch.enabled)
        if client is not None:
            client.publish(state_topic, payload,
                           qos=cfg["mqtt_qos"], retain=cfg["mqtt_retain"])
        return payload

    last_ok_payload = None
    was_active = None
    next_poll_in = 0.0     # seconds until next scheduled poll (0 = now)
    while not stop["flag"]:
        # clear before evaluating: a command arriving later (incl. during a
        # poll) re-sets it, so the wait below returns immediately
        switched = switch.wake.is_set()
        switch.wake.clear()
        enabled = switch.enabled
        if switched:
            publish_switch_state()
        active = enabled and in_active_window(cfg.get("active_hours"),
                                              time.localtime())
        if active and not was_active:
            LOG.info("updates ON -> polling now (window: %s)",
                     cfg.get("active_hours") or "24/7")
            next_poll_in = 0.0
        elif was_active and not enabled:
            LOG.info("updates OFF -> polling stopped "
                     "(last payload stays in HA)")
        was_active = active

        if active and next_poll_in <= 0:
            last_ok_payload = do_poll("scheduled") or last_ok_payload
            next_poll_in = cfg["interval"]
            if cfg["once"]:
                if cfg["dry_run"]:
                    print(json.dumps(json.loads(last_ok_payload),
                                     indent=2)[:4000])
                break

        # Sleep until: scheduled poll due, the update switch changes, or a
        # window edge. Re-checked every <=30 s slice so window edges are
        # honoured with long intervals; when disabled nothing is requested.
        step = min(next_poll_in if active else 30.0, 30.0)
        step = max(step, 0.5)
        if switch.wake.wait(step):
            if stop["flag"]:
                break
            # re-evaluate at loop top: state publish + ON-transition poll;
            # 'ON'/'Update' received while already active forces a poll
            if switch.enabled and active:
                next_poll_in = 0.0
            continue

        if active:
            next_poll_in -= step
        # disabled/idle: loop just re-evaluates every slice

    if client is not None:
        client.publish(f"{cfg['mqtt_base_topic'].strip('/')}/availability",
                       "offline", qos=1, retain=True)
        # clear retained state so HA doesn't show a frozen old list forever
        client.publish(state_topic, "", qos=1, retain=True)
        client.loop_stop()
        client.disconnect()
    enricher.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
