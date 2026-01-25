#!/usr/bin/env python3
"""Validate ATS detection by sampling companies and checking job endpoints."""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests

USER_AGENT = "bioinfo-job-tracker/0.3"

API_CODES = {
    "careers_url": 0,
    "none": 0,
    "greenhouse": 1,
    "lever": 2,
    "ashby": 3,
    "icims": 4,
    "workday": 5,
    "smartrecruiters": 6,
}

NOT_FOUND_MARKERS = [
    "not found",
    "page not found",
    "we can't find",
    "does not exist",
    "oops",
]


@dataclass
class ValidationResult:
    company_name: str
    original_api_name: str
    original_api_url: str
    original_success: bool
    original_jobs_count: int | None
    retried: bool
    retry_api_name: str | None
    retry_api_url: str | None
    retry_success: bool | None
    retry_jobs_count: int | None
    notes: str | None


@dataclass
class CompanyRow:
    company_name: str


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "", text.lower())
    return slug


def candidate_urls(company_name: str) -> list[tuple[str, str]]:
    slug = slugify(company_name)
    urls = [
        ("greenhouse", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"),
        ("lever", f"https://api.lever.co/v0/postings/{slug}"),
        ("ashby", f"https://api.ashbyhq.com/posting-api/job-board/{slug}"),
        ("smartrecruiters", f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/"),
    ]

    if len(slug) <= 60:
        urls.append(("icims", f"https://{slug}.icims.com/jobs/search?ss=1"))

    return urls


def request_ok(url: str, session: requests.Session, timeout: int, api_name: str) -> tuple[bool, int | None]:
    try:
        response = session.get(url, timeout=timeout, allow_redirects=True)
    except Exception:
        return False, None

    if response.status_code >= 400:
        return False, None
    if api_name == "careers_url":
        return True, None

    if api_name == "greenhouse":
        try:
            payload = response.json()
        except ValueError:
            return False, None
        if isinstance(payload, dict) and "jobs" in payload and isinstance(payload["jobs"], list):
            return True, len(payload["jobs"])
        return False, None

    if api_name == "lever":
        try:
            payload = response.json()
        except ValueError:
            return False, None
        if isinstance(payload, list):
            return True, len(payload)
        return False, None

    if api_name == "ashby":
        try:
            payload = response.json()
        except ValueError:
            return False, None
        if isinstance(payload, dict) and "jobs" in payload and isinstance(payload["jobs"], list):
            return True, len(payload["jobs"])
        return False, None
    if api_name == "workday":
        try:
            payload = response.json()
        except ValueError:
            return False, None
        if isinstance(payload, dict):
            for key in ("total", "jobPostings", "items"):
                if key in payload:
                    count = payload.get("total")
                    if isinstance(count, int):
                        return True, count
                    postings = payload.get("jobPostings") or payload.get("items")
                    if isinstance(postings, list):
                        return True, len(postings)
                    return True, None
        return False, None
    if api_name == "smartrecruiters":
        try:
            payload = response.json()
        except ValueError:
            return False, None
        if isinstance(payload, dict) and isinstance(payload.get("content"), list):
            return True, len(payload["content"])
        return False, None

    body = response.text.lower()
    if any(marker in body for marker in NOT_FOUND_MARKERS):
        return False, None

    return True, None


def load_targeted(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def sample_companies(rows: list[dict], sample_size: int, seed: int) -> list[dict]:
    if sample_size <= 0 or sample_size >= len(rows):
        return rows
    rng = random.Random(seed)
    return rng.sample(rows, sample_size)


def write_results(path: Path, rows: Iterable[ValidationResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [row.__dict__ for row in rows]
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate ATS detection with random sampling.")
    parser.add_argument(
        "--input",
        default="data/targeted_list.json",
        help="Path to targeted_list.json.",
    )
    parser.add_argument(
        "--output",
        default="data/targeted_list_validation.json",
        help="Path to validation output JSON.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=250,
        help="Number of companies to sample.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout in seconds.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    targeted = load_targeted(Path(args.input))
    if not targeted:
        print("No entries found in targeted_list.json")
        return 1

    sample = sample_companies(targeted, args.sample_size, args.seed)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    results: list[ValidationResult] = []

    for row in sample:
        company_name = row.get("company_name", "").strip()
        api_name = row.get("api_name", "")
        api_url = row.get("api_url", "")

        if not company_name or not api_name or not api_url:
            results.append(
                ValidationResult(
                    company_name=company_name or "",
                    original_api_name=api_name or "",
                    original_api_url=api_url or "",
                    original_success=False,
                    original_jobs_count=None,
                    retried=False,
                    retry_api_name=None,
                    retry_api_url=None,
                    retry_success=None,
                    retry_jobs_count=None,
                    notes="missing_fields",
                )
            )
            continue

        success, count = request_ok(api_url, session, args.timeout, api_name)

        retry_used = False
        retry_success = None
        retry_count = None
        retry_api_name = None
        retry_api_url = None
        notes = None

        if not success:
            for candidate_api, candidate_url in candidate_urls(company_name):
                if candidate_api == api_name:
                    continue
                retry_success, retry_count = request_ok(
                    candidate_url, session, args.timeout, candidate_api
                )
                retry_used = True
                retry_api_name = candidate_api
                retry_api_url = candidate_url
                if retry_success:
                    break

            if retry_used and not retry_success:
                notes = "retry_failed"

        results.append(
            ValidationResult(
                company_name=company_name,
                original_api_name=api_name,
                original_api_url=api_url,
                original_success=success,
                original_jobs_count=count,
                retried=retry_used,
                retry_api_name=retry_api_name,
                retry_api_url=retry_api_url,
                retry_success=retry_success,
                retry_jobs_count=retry_count,
                notes=notes,
            )
        )

    write_results(Path(args.output), results)
    print(f"Validated {len(results)} companies. Output: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
