#!/usr/bin/env python3
"""Detect common ATS/job board providers for a list of companies."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import requests
from tqdm import tqdm

API_CODES = {
    "none": 0,
    "greenhouse": 1,
    "lever": 2,
    "ashby": 3,
    "icims": 4,
    "workday": 5,
}

USER_AGENT = "bioinfo-job-tracker/0.1"


@dataclass
class CompanyRow:
    company_name: str


@dataclass
class DetectionResult:
    company_name: str
    api_code: int
    api_name: str
    api_url: str


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "", text.lower())
    return slug


def load_companies(path: Path) -> list[CompanyRow]:
    companies: list[CompanyRow] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not row.get("company_name"):
                continue
            companies.append(
                CompanyRow(
                    company_name=row.get("company_name", "").strip(),
                )
            )
    return companies


PROVIDER_MARKERS = {
    "icims": ["icims"],
    "workday": ["myworkdayjobs"],
}

NOT_FOUND_MARKERS = [
    "not found",
    "page not found",
    "we can't find",
    "does not exist",
    "oops",
]


def request_ok(url: str, session: requests.Session, timeout: int, api_name: str) -> bool:
    try:
        response = session.get(url, timeout=timeout, allow_redirects=True)
    except Exception:
        return False

    if response.status_code >= 400:
        return False

    if api_name == "greenhouse":
        try:
            payload = response.json()
        except ValueError:
            return False
        return isinstance(payload, dict) and "jobs" in payload

    if api_name == "lever":
        try:
            payload = response.json()
        except ValueError:
            return False
        return isinstance(payload, list)

    if api_name == "ashby":
        try:
            payload = response.json()
        except ValueError:
            return False
        return isinstance(payload, dict) and "jobs" in payload

    body = response.text.lower()
    if any(marker in body for marker in NOT_FOUND_MARKERS):
        return False

    markers = PROVIDER_MARKERS.get(api_name, [])
    if markers and not any(marker in body for marker in markers):
        return False

    return True


def candidate_urls(company: CompanyRow) -> list[tuple[str, str]]:
    slug = slugify(company.company_name)

    urls = [
        ("greenhouse", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"),
        ("lever", f"https://api.lever.co/v0/postings/{slug}"),
        ("ashby", f"https://api.ashbyhq.com/posting-api/job-board/{slug}"),
    ]

    if len(slug) <= 60:
        icims_domain = f"{slug}.icims.com"
        urls.append(("icims", f"https://{icims_domain}/jobs/search?ss=1"))

    return urls


def detect_company(company: CompanyRow, session: requests.Session, timeout: int) -> Optional[DetectionResult]:
    for api_name, url in candidate_urls(company):
        if request_ok(url, session, timeout, api_name):
            return DetectionResult(
                company_name=company.company_name,
                api_code=API_CODES[api_name],
                api_name=api_name,
                api_url=url,
            )
    return None


def write_json(path: Path, data: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(list(data), handle, indent=2)


def write_no_api_csv(path: Path, companies: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["company_name"])
        for name in companies:
            writer.writerow([name])


def write_no_api_json(path: Path, companies: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(list(companies), handle, indent=2)


def load_progress(progress_path: Path) -> list[dict]:
    if not progress_path.exists():
        return []
    entries = []
    with progress_path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            entries.append(json.loads(line))
    return entries


def append_progress(progress_path: Path, payload: dict) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    with progress_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect ATS providers for company list.")
    parser.add_argument(
        "--input",
        default="data/companies.csv",
        help="Path to companies CSV.",
    )
    parser.add_argument(
        "--output-targeted",
        default="data/targeted_list.json",
        help="Path to JSON output for detected APIs.",
    )
    parser.add_argument(
        "--output-no-api",
        default="data/no_api_companies.csv",
        help="Path to CSV output for companies with no detected API.",
    )
    parser.add_argument(
        "--progress-dir",
        default="logs/company_api_scan",
        help="Directory to store progress JSONL files.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from existing progress files if present.",
    )
    parser.add_argument("--timeout", type=int, default=8, help="HTTP timeout in seconds.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    input_path = Path(args.input)
    companies = load_companies(input_path)
    if not companies:
        logging.error("No companies found in %s", input_path)
        return 1

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    progress_dir = Path(args.progress_dir)
    detected_progress = progress_dir / "detected.jsonl"
    no_api_progress = progress_dir / "no_api.jsonl"

    detected_entries = load_progress(detected_progress) if args.resume else []
    no_api_entries = load_progress(no_api_progress) if args.resume else []

    seen = {entry["company_name"] for entry in detected_entries if "company_name" in entry}
    seen.update(
        entry["company_name"] for entry in no_api_entries if "company_name" in entry
    )

    for company in tqdm(companies, desc="Checking companies"):
        if company.company_name in seen:
            continue
        result = detect_company(company, session, timeout=args.timeout)
        if result:
            payload = {
                "company_name": result.company_name,
                "api_code": result.api_code,
                "api_name": result.api_name,
                "api_url": result.api_url,
            }
            append_progress(detected_progress, payload)
        else:
            append_progress(no_api_progress, {"company_name": company.company_name})

    detected_entries = load_progress(detected_progress)
    no_api_entries = load_progress(no_api_progress)

    write_json(Path(args.output_targeted), detected_entries)
    no_api_path = Path(args.output_no_api)
    no_api_names = [entry["company_name"] for entry in no_api_entries]
    if no_api_path.suffix.lower() == ".json":
        write_no_api_json(no_api_path, no_api_names)
    else:
        write_no_api_csv(no_api_path, no_api_names)

    logging.info("Detected %s companies with ATS.", len(detected_entries))
    logging.info("No API detected for %s companies.", len(no_api_entries))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
