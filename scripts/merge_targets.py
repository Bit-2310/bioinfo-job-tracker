#!/usr/bin/env python3
"""Merge multiple target list JSON files into a single de-duplicated file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_json(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    return []


def merge_targets(paths: list[Path]) -> list[dict]:
    seen = set()
    merged: list[dict] = []
    for path in paths:
        for row in load_json(path):
            company = (row.get("company_name") or "").strip()
            api_url = (row.get("api_url") or "").strip()
            key = (company, api_url)
            if not company or not api_url:
                continue
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)
    return merged


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge target lists into one JSON.")
    parser.add_argument(
        "--inputs",
        nargs="+",
        default=[
            "data/targeted_list.json",
            "data/targeted_list_validation.json",
            "data/target_sponsor.json",
            "data/targeted_list_biotech_reference_verified.json",
        ],
    )
    parser.add_argument("--output", default="data/targeted_list_combined.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    inputs = [Path(p) for p in args.inputs]
    merged = merge_targets(inputs)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(merged, indent=2), encoding="utf-8")
    print(f"Merged {len(merged)} entries into {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
