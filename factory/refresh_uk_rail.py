"""One-time-ish script: downloads ORR's "Table 1410: Passenger entries,
exits and interchanges by station" CSV (Great Britain, annual, most
recent release) and writes a Parquet snapshot to domains/uk_rail/data/.
Run manually and commit the output so the repo works offline;
src/ingest/structured.py reads this file, not the ORR data portal
directly. Re-run to pick up a new annual release.

Usage: uv run python -m factory.refresh_uk_rail

Source confirmed live and downloadable while building phase 6 (see
docs/PLAN.md notes): the ORR Data Portal's station usage page
(https://dataportal.orr.gov.uk/statistics/usage/estimates-of-station-usage)
links a CSV export of Table 1410 at the URL below. If ORR reorganises
their site and this 404s, the current download link can be found from
that page.
"""

from __future__ import annotations

import urllib.request
from pathlib import Path

import pandas as pd

CSV_URL = (
    "https://dataportal.orr.gov.uk/media/1909/"
    "table-1410-passenger-entries-and-exits-and-interchanges-by-station.csv"
)
DATA_DIR = Path(__file__).resolve().parent.parent / "domains" / "uk_rail" / "data"

# The source CSV has three title/note rows before the header, and a
# handful of trailing blank rows after the data (both confirmed by
# inspecting the real download while building this).
HEADER_ROW = 3

COLUMN_RENAMES = {
    "Station name": "station_name",
    "Entries and exits: \nFull price tickets": "full_price_entries_exits",
    " Entries and exits: \nReduced price tickets": "reduced_price_entries_exits",
    "Entries and exits:\nSeason tickets": "season_ticket_entries_exits",
    "Entries and exits:\nAll tickets": "all_ticket_entries_exits",
    "Entries and exits:\nRank": "usage_rank",
    "Interchanges": "interchanges",
    "Main origin or destination station": "main_destination_station",
    "Number of journeys to or from main origin or destination station": (
        "journeys_to_main_destination"
    ),
    "National Location Code (NLC)": "nlc",
    "Three Letter Code\n(TLC)": "tlc",
    "Region": "region",
    "Station facility owner": "station_facility_owner",
    "Station group": "station_group",
}
NUMERIC_COLUMNS = [
    "full_price_entries_exits",
    "reduced_price_entries_exits",
    "season_ticket_entries_exits",
    "all_ticket_entries_exits",
    "usage_rank",
    "interchanges",
    "journeys_to_main_destination",
]


def _clean_numeric(series: pd.Series) -> pd.Series:
    # Source values are strings like "3,782,112" or "[z]"/"[low]" for
    # not-applicable/suppressed cells; anything that doesn't parse as a
    # plain thousands-separated integer becomes null rather than 0, so it
    # doesn't silently look like a real zero-usage station.
    cleaned = series.astype(str).str.replace(",", "", regex=False)
    return pd.to_numeric(cleaned, errors="coerce").astype("Int64")


def fetch_stations() -> pd.DataFrame:
    with urllib.request.urlopen(CSV_URL) as response:  # noqa: S310
        raw = pd.read_csv(response, skiprows=HEADER_ROW)

    raw = raw.dropna(subset=["Station name"])
    df = raw.rename(columns=COLUMN_RENAMES)[list(COLUMN_RENAMES.values())]
    df["nlc"] = pd.to_numeric(df["nlc"], errors="coerce").astype("Int64")
    for column in NUMERIC_COLUMNS:
        df[column] = _clean_numeric(df[column])
    return df.reset_index(drop=True)


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stations = fetch_stations()
    out_path = DATA_DIR / "stations.parquet"
    stations.to_parquet(out_path, index=False)
    print(f"wrote {len(stations)} stations to {out_path}")


if __name__ == "__main__":
    main()
