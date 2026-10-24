#!/usr/bin/env python3
"""Refresh the offline lookup tables used for enrichment.

  data/airlines.json  ICAO airline designator -> airline name
                      (source: OpenFlights airlines.dat, Open Database License)
  data/types.json     ICAO type designator -> "Manufacturer Model"
                      (source: ICAO DOC 8643 extract,
                       github.com/ColtJD45/icao-aircraft-designator-list)

Run occasionally (monthly is plenty):  python3 refresh_databases.py
"""

import csv
import io
import json
import os
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")

AIRLINES_URL = ("https://raw.githubusercontent.com/jpatokal/openflights/"
                "master/data/airlines.dat")
TYPES_URL = ("https://raw.githubusercontent.com/ColtJD45/"
             "icao-aircraft-designator-list/main/icao_aircraft_data.csv")


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "adsb2mqtt-db"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", "replace")


def build_airlines() -> dict[str, str]:
    """ICAO (3-letter) -> airline name. Active airlines win conflicts."""
    text = fetch(AIRLINES_URL)
    out: dict[str, str] = {}
    active: dict[str, str] = {}
    # id,name,alias,iata,icao,callsign,country,active
    for row in csv.reader(io.StringIO(text)):
        if len(row) < 8:
            continue
        name, icao, is_active = row[1].strip(), row[4].strip(), row[7].strip()
        if not icao or icao == "\\N" or not name or name == "\\N":
            continue
        icao = icao.upper()
        if is_active.upper() == "Y":
            active.setdefault(icao, name)
        else:
            out.setdefault(icao, name)
    out.update(active)   # active entries override inactive ones
    return out


def build_types() -> dict[str, str]:
    """ICAO type designator -> 'Manufacturer Model' (first row per code)."""
    text = fetch(TYPES_URL)
    out: dict[str, str] = {}
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        code = (row.get("type_designator") or "").strip().upper()
        if not code or code in out:
            continue
        man = (row.get("manufacturer") or "").strip().title()
        model = (row.get("model") or "").strip()
        if model and model.upper().startswith(man.upper()):
            label = model           # 'ATR ATR-72-500' -> 'ATR-72-500'
        else:
            label = f"{man} {model}".strip()
        if label:
            out[code] = label
    return out


def main() -> int:
    os.makedirs(DATA, exist_ok=True)
    for name, builder in (("airlines.json", build_airlines),
                          ("types.json", build_types)):
        table = builder()
        path = os.path.join(DATA, name)
        with open(path, "w") as fh:
            json.dump(table, fh, sort_keys=True)
        print(f"{path}: {len(table)} entries, "
              f"{os.path.getsize(path) // 1024} KiB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
