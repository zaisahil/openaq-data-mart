from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

SOURCE_CSV = Path("Source_Metrics.csv")
DB_FILE = Path("data_mart.db")
QUALITY_REPORT = Path("quality_report.json")
BAD_DATA_TABLE = "bad_data"

PARAMETER_METADATA = {
    "co": {"name": "Carbon Monoxide", "unit": "ppm", "min": 0.0, "max": 50.0},
    "so2": {"name": "Sulphur Dioxide", "unit": "ppm", "min": 0.0, "max": 10.0},
    "pm25": {"name": "Particulate Matter 2.5", "unit": "µg/m³", "min": 0.0, "max": 500.0},
    "pm10": {"name": "Particulate Matter 10", "unit": "µg/m³", "min": 0.0, "max": 600.0},
    "no2": {"name": "Nitrogen Dioxide", "unit": "ppm", "min": 0.0, "max": 10.0},
    "o3": {"name": "Ozone", "unit": "ppm", "min": 0.0, "max": 5.0},
    "bc": {"name": "Black Carbon", "unit": "µg/m³", "min": 0.0, "max": 200.0},
}

UNIT_ALIASES = {
    "ppm": "ppm",
    "µg/m³": "µg/m³",
    "ug/m3": "µg/m³",
    "ug/m³": "µg/m³",
    "ug/m^3": "µg/m³",
    "µg/m3": "µg/m³",
}

MAX_INTERPOLATION_GAP_HOURS = 3


def normalize_unit(unit: str) -> str:
    """
    Normalize unit string to standard format using aliases.

    Input: raw unit string
    Output: normalized unit string (e.g., 'ug/m3' -> 'µg/m³')
    """
    if not unit:
        return ""
    return UNIT_ALIASES.get(unit.strip().lower(), unit.strip())


@dataclass(frozen=True)
class CleanMeasurement:
    location: str
    city: str
    country: str
    utc: datetime
    local: str
    parameter: str
    value: float
    unit: str
    latitude: float
    longitude: float
    attribution: str
    is_interpolated: bool = False


def parse_utc(utc_value: str) -> Optional[datetime]:
    """
    Parse UTC timestamp string into datetime object.

    Input: ISO timestamp string
    Output: datetime (UTC, timezone removed) or None if invalid
    """
    if not utc_value:
        return None
    try:
        dt = datetime.fromisoformat(utc_value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    """
    Safely convert string to float.

    Input: string value
    Output: float or None if invalid
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_valid_row(row: Dict[str, str]) -> Tuple[bool, Optional[str]]:
    """
    Validate raw CSV row against business rules.

    Input: raw row dict
    Output: (is_valid, reason_if_invalid)
    """
    location = row.get("location", "").strip()
    city = row.get("city", "").strip()
    country = row.get("country", "").strip()
    if not location or not city or not country:
        return False, "missing_dimension"

    utc = parse_utc(row.get("utc", ""))
    if utc is None:
        return False, "invalid_timestamp"

    value = parse_float(row.get("value", ""))
    if value is None:
        return False, "invalid_value"
    if value < 0:
        return False, "negative_value"

    parameter = row.get("parameter", "").strip().lower()
    if parameter == "":
        return False, "missing_parameter"

    latitude = parse_float(row.get("latitude", ""))
    longitude = parse_float(row.get("longitude", ""))
    if latitude is None or longitude is None:
        return False, "missing_coordinates"
    if not (-90.0 <= latitude <= 90.0) or not (-180.0 <= longitude <= 180.0):
        return False, "invalid_coordinates"

    unit = normalize_unit(row.get("unit", ""))
    if parameter in PARAMETER_METADATA:
        expected_unit = PARAMETER_METADATA[parameter]["unit"]
        if unit != expected_unit:
            return False, "invalid_unit"
        max_value = PARAMETER_METADATA[parameter]["max"]
        if value > max_value:
            return False, "out_of_range_value"

    return True, None


def read_source_rows(path: Path) -> Iterator[Dict[str, str]]:
    """
    Read CSV file row-by-row.

    Input: file path
    Output: iterator of row dictionaries
    """
    with path.open("r", encoding="utf-8", errors="replace", newline="") as src:
        reader = csv.DictReader(src)
        for row in reader:
            yield row


def clean_measurement(row: Dict[str, str]) -> Optional[CleanMeasurement]:
    """
    Convert valid row into CleanMeasurement object.

    Input: raw row dict
    Output: CleanMeasurement or None if invalid
    """
    valid, reason = is_valid_row(row)
    if not valid:
        return None
    value = parse_float(row["value"])
    utc = parse_utc(row["utc"])
    return CleanMeasurement(
        location=row["location"].strip(),
        city=row["city"].strip(),
        country=row["country"].strip(),
        utc=utc,
        local=row["local"].strip(),
        parameter=row["parameter"].strip().lower(),
        value=value,
        unit=normalize_unit(row["unit"]),
        latitude=parse_float(row["latitude"]),
        longitude=parse_float(row["longitude"]),
        attribution=row["attribution"].strip(),
    )


def load_cleaned_measurements(path: Path) -> Tuple[List[CleanMeasurement], List[Dict[str, str]]]:
    """
    Load and separate clean vs bad rows.

    Input: CSV file path
    Output: (list of clean measurements, list of bad rows with reason)
    """
    clean: List[CleanMeasurement] = []
    bad_rows: List[Dict[str, str]] = []
    for row in read_source_rows(path):
        valid, reason = is_valid_row(row)
        if not valid:
            row["bad_reason"] = reason
            bad_rows.append(row)
            continue
        measurement = clean_measurement(row)
        if measurement is not None:
            clean.append(measurement)
    return clean, bad_rows


def group_by_location_and_parameter(measurements: List[CleanMeasurement]) -> Dict[Tuple[str, str], List[CleanMeasurement]]:
    """
    Group measurements by (location, parameter).

    Input: list of measurements
    Output: dict with grouped and time-sorted measurements
    """
    groups: Dict[Tuple[str, str], List[CleanMeasurement]] = {}
    for m in measurements:
        key = (m.location, m.parameter)
        groups.setdefault(key, []).append(m)
    for values in groups.values():
        values.sort(key=lambda item: item.utc)
    return groups


def interpolate_group(values: List[CleanMeasurement]) -> List[CleanMeasurement]:
    """
    Fill small time gaps using linear interpolation.

    Input: sorted measurements for one group
    Output: list with interpolated records added
    """
    if len(values) < 2:
        return list(values)
    values = sorted(values, key=lambda item: item.utc)
    result: List[CleanMeasurement] = []
    for previous, next_measurement in zip(values, values[1:]):
        result.append(previous)
        gap_hours = int((next_measurement.utc - previous.utc).total_seconds() / 3600)
        if 1 < gap_hours <= MAX_INTERPOLATION_GAP_HOURS:
            for step in range(1, gap_hours):
                current = previous.utc + timedelta(hours=step)
                ratio = step / gap_hours
                interpolated_value = previous.value + ratio * (next_measurement.value - previous.value)
                result.append(
                    CleanMeasurement(
                        location=previous.location,
                        city=previous.city,
                        country=previous.country,
                        utc=current,
                        local=previous.local,
                        parameter=previous.parameter,
                        value=interpolated_value,
                        unit=previous.unit,
                        latitude=previous.latitude,
                        longitude=previous.longitude,
                        attribution=previous.attribution,
                        is_interpolated=True,
                    )
                )
    result.append(values[-1])
    return result


def interpolate_measurements(measurements: List[CleanMeasurement]) -> List[CleanMeasurement]:
    """
    Apply interpolation across all groups.

    Input: list of clean measurements
    Output: list including interpolated records (sorted)
    """
    groups = group_by_location_and_parameter(measurements)
    interpolated: List[CleanMeasurement] = []
    for values in groups.values():
        interpolated.extend(interpolate_group(values))
    interpolated.sort(key=lambda item: (item.utc, item.location, item.parameter))
    return interpolated


def create_schema(conn: sqlite3.Connection) -> None:
    """
    Create star schema tables in SQLite.

    Input: DB connection
    Output: None (creates tables if not exist)
    """
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS dim_location (
            location_id INTEGER PRIMARY KEY,
            location TEXT NOT NULL,
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            latitude REAL,
            longitude REAL,
            attribution TEXT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_location_key ON dim_location(location, city, country);

        CREATE TABLE IF NOT EXISTS dim_time (
            time_id INTEGER PRIMARY KEY,
            utc TEXT NOT NULL UNIQUE,
            local TEXT,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            hour INTEGER,
            day_of_week INTEGER,
            month_name TEXT
        );

        CREATE TABLE IF NOT EXISTS dim_parameter (
            parameter_id INTEGER PRIMARY KEY,
            parameter TEXT NOT NULL UNIQUE,
            name TEXT,
            unit TEXT
        );

        CREATE TABLE IF NOT EXISTS fact_measurement (
            measurement_id INTEGER PRIMARY KEY,
            location_id INTEGER NOT NULL,
            time_id INTEGER NOT NULL,
            parameter_id INTEGER NOT NULL,
            value REAL NOT NULL,
            is_interpolated INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(location_id) REFERENCES dim_location(location_id),
            FOREIGN KEY(time_id) REFERENCES dim_time(time_id),
            FOREIGN KEY(parameter_id) REFERENCES dim_parameter(parameter_id)
        );

        CREATE INDEX IF NOT EXISTS idx_fact_location ON fact_measurement(location_id);
        CREATE INDEX IF NOT EXISTS idx_fact_time ON fact_measurement(time_id);
        CREATE INDEX IF NOT EXISTS idx_fact_parameter ON fact_measurement(parameter_id);

        CREATE TABLE IF NOT EXISTS bad_data (
            bad_id INTEGER PRIMARY KEY,
            location TEXT,
            city TEXT,
            country TEXT,
            utc TEXT,
            local TEXT,
            parameter TEXT,
            value TEXT,
            unit TEXT,
            latitude TEXT,
            longitude TEXT,
            attribution TEXT,
            bad_reason TEXT
        );
        """
    )


def ensure_parameter(conn: sqlite3.Connection, parameter: str, unit: str) -> int:
    """
    Ensure parameter exists in dimension table.

    Input: DB connection, parameter name, unit
    Output: parameter_id
    """
    cursor = conn.execute(
        "SELECT parameter_id FROM dim_parameter WHERE parameter = ?",
        (parameter,),
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor = conn.execute(
        "INSERT INTO dim_parameter(parameter, name, unit) VALUES (?, ?, ?)",
        (parameter, PARAMETER_METADATA.get(parameter, {}).get("name", parameter), unit),
    )
    return cursor.lastrowid


def ensure_location(conn: sqlite3.Connection, measurement: CleanMeasurement) -> int:
    """
    Ensure location exists in dimension table.

    Input: DB connection, measurement
    Output: location_id
    """
    cursor = conn.execute(
        "SELECT location_id FROM dim_location WHERE location = ? AND city = ? AND country = ?",
        (measurement.location, measurement.city, measurement.country),
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor = conn.execute(
        "INSERT INTO dim_location(location, city, country, latitude, longitude, attribution) VALUES (?, ?, ?, ?, ?, ?)",
        (
            measurement.location,
            measurement.city,
            measurement.country,
            measurement.latitude,
            measurement.longitude,
            measurement.attribution,
        ),
    )
    return cursor.lastrowid


def ensure_time(conn: sqlite3.Connection, measurement: CleanMeasurement) -> int:
    """
    Ensure time dimension record exists.

    Input: DB connection, measurement
    Output: time_id
    """
    utc_text = measurement.utc.isoformat()
    cursor = conn.execute(
        "SELECT time_id FROM dim_time WHERE utc = ?",
        (utc_text,),
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    dt = measurement.utc
    cursor = conn.execute(
        "INSERT INTO dim_time(utc, local, year, month, day, hour, day_of_week, month_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            utc_text,
            measurement.local,
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.isoweekday(),
            dt.strftime("%B"),
        ),
    )
    return cursor.lastrowid


def insert_bad_data(conn: sqlite3.Connection, bad_rows: List[Dict[str, str]]) -> None:
    """
    Store invalid rows into bad_data table.

    Input: DB connection, list of bad rows
    Output: None
    """
    if not bad_rows:
        return
    conn.executemany(
        "INSERT INTO bad_data(location, city, country, utc, local, parameter, value, unit, latitude, longitude, attribution, bad_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                row.get("location"),
                row.get("city"),
                row.get("country"),
                row.get("utc"),
                row.get("local"),
                row.get("parameter"),
                row.get("value"),
                row.get("unit"),
                row.get("latitude"),
                row.get("longitude"),
                row.get("attribution"),
                row.get("bad_reason"),
            )
            for row in bad_rows
        ],
    )


def load_measurements(conn: sqlite3.Connection, measurements: List[CleanMeasurement]) -> None:
    """
    Load fact table with measurements.

    Input: DB connection, list of measurements
    Output: None
    """
    parameter_ids: Dict[str, int] = {}
    for m in measurements:
        if m.parameter not in parameter_ids:
            parameter_ids[m.parameter] = ensure_parameter(conn, m.parameter, m.unit)
    for measurement in measurements:
        location_id = ensure_location(conn, measurement)
        time_id = ensure_time(conn, measurement)
        parameter_id = parameter_ids[measurement.parameter]
        conn.execute(
            "INSERT INTO fact_measurement(location_id, time_id, parameter_id, value, is_interpolated) VALUES (?, ?, ?, ?, ?)",
            (
                location_id,
                time_id,
                parameter_id,
                measurement.value,
                1 if measurement.is_interpolated else 0,
            ),
        )


def save_quality_report(bad_rows: List[Dict[str, str]], report_path: Path) -> None:
    """
    Generate JSON report of bad data.

    Input: bad rows list, output file path
    Output: None (writes JSON file)
    """
    counts: Dict[str, int] = {}
    for row in bad_rows:
        counts[row.get("bad_reason", "unknown")] = counts.get(row.get("bad_reason", "unknown"), 0) + 1
    report = {
        "source_file": str(SOURCE_CSV),
        "total_bad_rows": len(bad_rows),
        "reasons": counts,
    }
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


def run_pipeline() -> None:
    """
    Main pipeline execution.

    Steps:
    1. Load & validate data
    2. Interpolate missing values
    3. Create DB schema
    4. Load data into warehouse
    5. Save quality report

    Output: SQLite DB + JSON report
    """
    print("[*] Loading source data from", SOURCE_CSV)
    clean_rows, bad_rows = load_cleaned_measurements(SOURCE_CSV)
    print(f"[*] Clean rows: {len(clean_rows)}, bad rows: {len(bad_rows)}")

    interpolated_rows = interpolate_measurements(clean_rows)
    print(f"[*] Records after interpolation: {len(interpolated_rows)}")

    if DB_FILE.exists():
        DB_FILE.unlink()
    with sqlite3.connect(DB_FILE) as conn:
        create_schema(conn)
        insert_bad_data(conn, bad_rows)
        load_measurements(conn, interpolated_rows)
        conn.commit()

    save_quality_report(bad_rows, QUALITY_REPORT)
    print("[*] Data mart created at", DB_FILE)
    print("[*] Quality report saved at", QUALITY_REPORT)


if __name__ == "__main__":
    run_pipeline()
