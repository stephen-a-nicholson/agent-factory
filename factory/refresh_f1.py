"""One-time-ish script: pulls F1 seasons 2020 to the current year from the
Jolpica API (a drop-in replacement for the retired Ergast API) and writes
Parquet snapshots to domains/f1/data/. Run manually and commit the output
so the repo works offline; src/ingest/structured.py reads these files, not
the API. Re-run to pick up a new season or catch up mid-season results.

Usage: uv run python -m factory.refresh_f1
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

API_BASE = "https://api.jolpi.ca/ergast/f1"
FIRST_SEASON = 2020
PAGE_SIZE = 100
REQUEST_DELAY_SECONDS = 0.3
MAX_RETRIES = 5

DATA_DIR = Path(__file__).resolve().parent.parent / "domains" / "f1" / "data"


def _get(path: str, **params: Any) -> dict[str, Any]:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{API_BASE}/{path}.json" + (f"?{query}" if query else "")
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                payload = json.load(resp)
            time.sleep(REQUEST_DELAY_SECONDS)
            return payload["MRData"]
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            time.sleep(2**attempt)
    raise RuntimeError(f"failed to fetch {url} after {MAX_RETRIES} retries") from last_error


def fetch_races(season: int) -> list[dict[str, Any]]:
    data = _get(f"{season}/races", limit=PAGE_SIZE)
    rows = []
    for race in data["RaceTable"]["Races"]:
        round_ = int(race["round"])
        rows.append(
            {
                "race_id": f"{season}_{round_:02d}",
                "season": season,
                "round": round_,
                "race_name": race["raceName"],
                "circuit_id": race["Circuit"]["circuitId"],
                "circuit_name": race["Circuit"]["circuitName"],
                "country": race["Circuit"]["Location"]["country"],
                "locality": race["Circuit"]["Location"]["locality"],
                "date": race["date"],
                "time": race.get("time"),
            }
        )
    return rows


def fetch_results(season: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        data = _get(f"{season}/results", limit=PAGE_SIZE, offset=offset)
        for race in data["RaceTable"]["Races"]:
            race_id = f"{season}_{int(race['round']):02d}"
            for result in race["Results"]:
                rows.append(
                    {
                        "race_id": race_id,
                        "driver_id": result["Driver"]["driverId"],
                        "constructor_id": result["Constructor"]["constructorId"],
                        "number": int(result["number"]),
                        "grid": int(result["grid"]),
                        "position": int(result["position"]),
                        "position_text": result["positionText"],
                        "points": float(result["points"]),
                        "laps": int(result["laps"]),
                        "status": result["status"],
                    }
                )
        offset += PAGE_SIZE
        if offset >= int(data["total"]):
            break
    return rows


def fetch_drivers(season: int) -> list[dict[str, Any]]:
    data = _get(f"{season}/drivers", limit=PAGE_SIZE)
    rows = []
    for driver in data["DriverTable"]["Drivers"]:
        rows.append(
            {
                "driver_id": driver["driverId"],
                "code": driver.get("code"),
                "permanent_number": (
                    int(driver["permanentNumber"]) if "permanentNumber" in driver else None
                ),
                "given_name": driver["givenName"],
                "family_name": driver["familyName"],
                # Reserve/test drivers who've only done practice sessions
                # sometimes have an incomplete bio in the API.
                "date_of_birth": driver.get("dateOfBirth"),
                "nationality": driver.get("nationality"),
            }
        )
    return rows


def fetch_constructors(season: int) -> list[dict[str, Any]]:
    data = _get(f"{season}/constructors", limit=PAGE_SIZE)
    rows = []
    for constructor in data["ConstructorTable"]["Constructors"]:
        rows.append(
            {
                "constructor_id": constructor["constructorId"],
                "name": constructor["name"],
                "nationality": constructor["nationality"],
            }
        )
    return rows


def _parse_duration_seconds(raw: str) -> float | None:
    """Pit stop durations are usually "36.604" but long stops (red flags,
    mechanical issues) come back as "MM:SS.mmm" instead, and are
    occasionally missing altogether."""
    if not raw:
        return None
    if ":" in raw:
        minutes, seconds = raw.split(":")
        return int(minutes) * 60 + float(seconds)
    return float(raw)


def fetch_pit_stops(season: int, round_: int) -> list[dict[str, Any]]:
    race_id = f"{season}_{round_:02d}"
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        data = _get(f"{season}/{round_}/pitstops", limit=PAGE_SIZE, offset=offset)
        races = data["RaceTable"]["Races"]
        if not races:
            return rows
        for pit_stop in races[0]["PitStops"]:
            rows.append(
                {
                    "race_id": race_id,
                    "driver_id": pit_stop["driverId"],
                    "stop": int(pit_stop["stop"]),
                    "lap": int(pit_stop["lap"]),
                    "time": pit_stop["time"],
                    "duration_seconds": _parse_duration_seconds(pit_stop.get("duration", "")),
                }
            )
        offset += PAGE_SIZE
        if offset >= int(data["total"]):
            break
    return rows


def _write_parquet(rows: list[dict[str, Any]], name: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{name}.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    print(f"wrote {len(rows)} rows to {path.relative_to(DATA_DIR.parent.parent.parent)}")


def refresh(seasons: list[int]) -> None:
    all_races: list[dict[str, Any]] = []
    all_results: list[dict[str, Any]] = []
    all_pit_stops: list[dict[str, Any]] = []
    drivers_by_id: dict[str, dict[str, Any]] = {}
    constructors_by_id: dict[str, dict[str, Any]] = {}

    for season in seasons:
        print(f"season {season}: races, results, drivers, constructors")
        races = fetch_races(season)
        all_races.extend(races)
        all_results.extend(fetch_results(season))
        for driver in fetch_drivers(season):
            drivers_by_id[driver["driver_id"]] = driver
        for constructor in fetch_constructors(season):
            constructors_by_id[constructor["constructor_id"]] = constructor

        print(f"season {season}: pit stops for {len(races)} races")
        for race in races:
            all_pit_stops.extend(fetch_pit_stops(season, race["round"]))

    _write_parquet(all_races, "races")
    _write_parquet(all_results, "results")
    _write_parquet(list(drivers_by_id.values()), "drivers")
    _write_parquet(list(constructors_by_id.values()), "constructors")
    _write_parquet(all_pit_stops, "pit_stops")


if __name__ == "__main__":
    current_season = date.today().year
    refresh(list(range(FIRST_SEASON, current_season + 1)))
