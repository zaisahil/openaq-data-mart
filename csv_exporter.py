from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import List, Optional

DB_FILE = Path("data_mart.db")
QUERIES_FILE = Path("queries.sql")


class PercentileCont:
    """SQLite aggregate that computes a continuous percentile."""

    def __init__(self):
        # This stores the percentile requested by the SQL query.
        self.target_percentile: Optional[float] = None
        # This stores all numeric values that are passed in.
        self.values: List[float] = []

    def step(self, percentile: float, value: Optional[float]) -> None:
        # Called once for each row in the query.
        if value is None:
            return
        if self.target_percentile is None:
            self.target_percentile = float(percentile)
        self.values.append(float(value))

    def finalize(self) -> Optional[float]:
        # Called when all rows are processed.
        if self.target_percentile is None or not self.values:
            return None

        # Sort the collected values from smallest to largest.
        self.values.sort()

        # Clamp the percentile value to the valid range [0.0, 1.0].
        p = self.target_percentile
        if p < 0.0:
            p = 0.0
        elif p > 1.0:
            p = 1.0

        # If the percentile is 0, return the smallest value.
        if p == 0.0:
            return self.values[0]

        # If the percentile is 1, return the largest value.
        if p == 1.0:
            return self.values[-1]

        # Find the exact position in the sorted list.
        n = len(self.values)
        position = p * (n - 1)
        lower_index = int(position)
        upper_index = lower_index + 1

        # If we landed on the last item, just return it.
        if upper_index >= n:
            return self.values[lower_index]

        # Interpolate between the lower and upper value.
        fraction = position - lower_index
        low_value = self.values[lower_index]
        high_value = self.values[upper_index]
        return low_value + (high_value - low_value) * fraction


def date_trunc(unit: str, utc_value: Optional[str]) -> Optional[str]:
    """Truncate an ISO timestamp string to the desired time bucket."""
    if utc_value is None:
        return None
    value = utc_value.strip()
    if unit == "month":
        return value[:7] + "-01"
    if unit == "day":
        return value[:10]
    if unit == "hour":
        return value[:13] + ":00:00"
    return value


def load_statements(path: Path) -> List[str]:
    """Read SQL statements from a file, ignoring comments and blank lines."""
    raw = path.read_text(encoding="utf-8")
    statements: List[str] = []
    current: List[str] = []

    for line in raw.splitlines():
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
    """Save query results from a cursor into a CSV file."""
    columns = [description[0] for description in cursor.description] if cursor.description else []
    rows = cursor.fetchall()

    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)


def run_export(output_dir: Path) -> int:
    """Execute all SQL statements and export each result set to a separate CSV."""
    if not DB_FILE.exists():
        print(f"[!] Database not found: {DB_FILE}")
        return 1
    if not QUERIES_FILE.exists():
        print(f"[!] Queries file not found: {QUERIES_FILE}")
        return 1

    output_dir.mkdir(exist_ok=True)

    with sqlite3.connect(str(DB_FILE)) as connection:
        connection.create_aggregate("percentile_cont", 2, PercentileCont)
        connection.create_function("date_trunc", 2, date_trunc)

        statements = load_statements(QUERIES_FILE)
        for idx, statement in enumerate(statements, start=1):
            output_path = output_dir / f"query_{idx}.csv"
            print(f"[*] Exporting query {idx} to {output_path}")
            try:
                cursor = connection.execute(statement)
                export_to_csv(cursor, output_path)
            except sqlite3.Error as exc:
                print(f"[!] Failed to execute query {idx}: {exc}")
                print(statement)

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export queries.sql results to CSV files")
    parser.add_argument("--output", "-o", type=Path, default=Path("."), help="Output directory for CSV files")
    args = parser.parse_args()
    raise SystemExit(run_export(args.output))
