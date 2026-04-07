from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import List, Optional

DB_FILE = Path("data_mart.db")
QUERIES_FILE = Path("queries.sql")


class PercentileCont:
    def __init__(self):
        self.percentile: Optional[float] = None
        self.values: List[float] = []

    def step(self, percentile: float, value: Optional[float]) -> None:
        if value is None:
            return
        if self.percentile is None:
            self.percentile = float(percentile)
        self.values.append(float(value))

    def finalize(self) -> Optional[float]:
        if self.percentile is None or not self.values:
            return None
        self.values.sort()
        p = max(0.0, min(1.0, self.percentile))
        n = len(self.values)
        if p == 0.0:
            return self.values[0]
        if p == 1.0:
            return self.values[-1]
        rank = p * (n - 1)
        lower = int(rank)
        upper = min(lower + 1, n - 1)
        if lower == upper:
            return self.values[lower]
        fraction = rank - lower
        return self.values[lower] + fraction * (self.values[upper] - self.values[lower])


def date_trunc(unit: str, utc_value: Optional[str]) -> Optional[str]:
    if utc_value is None:
        return None
    normalized = str(utc_value).strip()
    if unit == "month":
        return normalized[:7] + "-01"
    if unit == "day":
        return normalized[:10]
    if unit == "hour":
        return normalized[:13] + ":00:00"
    return normalized


def load_statements(path: Path) -> List[str]:
    text = path.read_text(encoding="utf-8")
    statements: List[str] = []
    current: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current))
            current = []
    if current:
        statements.append("\n".join(current))
    return statements


def export_to_csv(cursor: sqlite3.Cursor, output_path: Path) -> None:
    columns = [description[0] for description in cursor.description] if cursor.description else []
    rows = cursor.fetchall()
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)


def main(output_dir: Path) -> int:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return 1
    if not QUERIES_FILE.exists():
        print(f"Queries file not found: {QUERIES_FILE}")
        return 1

    output_dir.mkdir(exist_ok=True)

    connection = sqlite3.connect(str(DB_FILE))
    connection.create_aggregate("percentile_cont", 2, PercentileCont)
    connection.create_function("date_trunc", 2, date_trunc)

    statements = load_statements(QUERIES_FILE)

    for idx, statement in enumerate(statements, start=1):
        output_path = output_dir / f"query_{idx}.csv"
        print(f"Exporting query {idx} to {output_path}")
        try:
            cursor = connection.execute(statement)
            export_to_csv(cursor, output_path)
        except sqlite3.Error as exc:
            print(f"Failed to execute query {idx}: {exc}")
            print(statement)

    connection.close()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export queries.sql results to CSV files")
    parser.add_argument("--output", "-o", type=Path, default=Path("."), help="Output directory for CSV files")
    args = parser.parse_args()
    raise SystemExit(main(args.output))
