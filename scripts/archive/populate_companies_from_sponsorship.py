#!/usr/bin/env python3
"""Populate data/companies.csv from Biotech_Companies_Sponsorship.csv."""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


def clean_name(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.strip())
    return cleaned


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract company names from sponsorship CSV.")
    parser.add_argument(
        "--input",
        default="data/Biotech_Companies_Sponsorship.csv",
        help="Path to sponsorship CSV.",
    )
    parser.add_argument(
        "--output",
        default="data/companies_all.csv",
        help="Path to output companies CSV.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    with input_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)

        headers = None
        for row in reader:
            if row and row[0] == "Sl No":
                headers = row
                break

        if not headers:
            raise SystemExit("Header row not found.")

        try:
            name_idx = headers.index("Employer (Petitioner) Name")
        except ValueError as exc:
            raise SystemExit("Employer (Petitioner) Name column not found.") from exc

        seen = set()
        names: list[str] = []

        for row in reader:
            if not row or len(row) <= name_idx:
                continue
            name = clean_name(row[name_idx])
            if not name or name in seen:
                continue
            seen.add(name)
            names.append(name)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["company_name"])
        for name in names:
            writer.writerow([name])

    print(f"Wrote {len(names)} companies to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
