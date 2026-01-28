#!/usr/bin/env python3
"""Ping ATS endpoints for a target list and report availability."""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

USER_AGENT = "bioinfo-job-tracker/0.9"

API_NAMES = {
    "greenhouse",
    "lever",
    "ashby",
    "icims",
    "workday",
    "smartrecruiters",
    "careers_url",
    "rippling",
}

NOT_FOUND_MARKERS = [
    "not found",
    "page not found",
    "we can't find",
    "does not exist",
    "oops",
]


@dataclass
class Target:
    company_name: str
    api_name: str
    api_url: str
    list_source: str


def load_targets(path: Path) -> list[Target]:
    data = json.loads(path.read_text(encoding="utf-8"))
    targets: list[Target] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        api_name = row.get("api_name") or row.get("original_api_name") or ""
        api_url = row.get("api_url") or row.get("original_api_url") or ""
        company = row.get("company_name") or ""
        if not company or not api_name or not api_url:
            continue
        targets.append(
            Target(
                company_name=company,
                api_name=str(api_name).lower(),
                api_url=str(api_url),
                list_source=row.get("list_source") or row.get("source") or "",
            )
        )
    return targets


def request_ok(url: str, api_name: str, session: requests.Session, timeout: int) -> tuple[bool, str]:
    last_exc = None
    resp = None
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=True)
            break
        except Exception as exc:
            last_exc = exc
            time.sleep(0.8 * (attempt + 1))
    if resp is None:
        return False, f"request_error:{last_exc.__class__.__name__ if last_exc else 'Unknown'}"

    if resp.status_code >= 400:
        return False, f"http_{resp.status_code}"

    if api_name == "greenhouse":
        try:
            payload = resp.json()
        except ValueError:
            return False, "invalid_json"
        if isinstance(payload, dict) and "jobs" in payload:
            return True, "ok"
        return False, "missing_jobs"

    if api_name == "lever":
        try:
            payload = resp.json()
        except ValueError:
            return False, "invalid_json"
        if isinstance(payload, list):
            return True, "ok"
        return False, "not_list"

    if api_name == "ashby":
        try:
            payload = resp.json()
        except ValueError:
            return False, "invalid_json"
        if isinstance(payload, dict) and "jobs" in payload:
            return True, "ok"
        return False, "missing_jobs"

    body = resp.text.lower()
    if any(marker in body for marker in NOT_FOUND_MARKERS):
        return False, "not_found_marker"

    if api_name == "careers_url":
        path = urlparse(url).path.lower()
        if any(token in path for token in ("/careers", "/jobs", "/openings", "/positions")):
            return True, "ok"
        if any(token in body for token in ("careers", "jobs", "openings", "positions")):
            return True, "ok"
        if len(body) > 5000:
            return True, "ok_content_length"
        return False, "missing_careers_marker"

    if api_name == "rippling":
        if "ats.rippling.com" in body or "/jobs/" in body:
            return True, "ok"
        return False, "missing_rippling_marker"

    if api_name in {"workday", "icims", "smartrecruiters"}:
        return True, "ok"

    return True, "ok"


def summarize(results: Iterable[dict]) -> dict:
    summary = {
        "total": 0,
        "ok": 0,
        "failed": 0,
        "by_api": {},
        "by_status": {},
    }
    for row in results:
        summary["total"] += 1
        api = row.get("api_name", "unknown")
        summary["by_api"].setdefault(api, {"ok": 0, "failed": 0})
        status = row.get("status", "unknown")
        summary["by_status"][status] = summary["by_status"].get(status, 0) + 1
        if row.get("ok"):
            summary["ok"] += 1
            summary["by_api"][api]["ok"] += 1
        else:
            summary["failed"] += 1
            summary["by_api"][api]["failed"] += 1
    return summary


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate ATS endpoints for a target list.")
    parser.add_argument("--input", default="data/targeted_list_combined.json")
    parser.add_argument("--output", default="data/ats_validation_report.json")
    parser.add_argument("--output-targeted", default="data/targeted_list_validated.json")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--limit", type=int, default=0, help="Optional cap on number of targets.")
    parser.add_argument("--workers", type=int, default=10)
    return parser.parse_args(argv)


def _check_target(target: Target, timeout: int) -> dict:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    if target.api_name not in API_NAMES:
        return {
            "company_name": target.company_name,
            "api_name": target.api_name,
            "api_url": target.api_url,
            "list_source": target.list_source,
            "ok": False,
            "status": "unknown_api",
        }
    ok, status = request_ok(target.api_url, target.api_name, session, timeout)
    return {
        "company_name": target.company_name,
        "api_name": target.api_name,
        "api_url": target.api_url,
        "list_source": target.list_source,
        "ok": ok,
        "status": status,
    }


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    path = Path(args.input)
    if not path.exists():
        print(f"Input not found: {path}")
        return 1

    targets = load_targets(path)
    if args.limit > 0:
        targets = targets[: args.limit]

    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(_check_target, t, args.timeout): t for t in targets}
        for idx, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            if idx % 25 == 0:
                print(f"Checked {idx}/{len(targets)}")

    report = {
        "input": str(path),
        "total_targets": len(targets),
        "results": results,
        "summary": summarize(results),
    }
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    validated = [
        {
            "company_name": row["company_name"],
            "api_name": row["api_name"],
            "api_url": row["api_url"],
        }
        for row in results
        if row.get("ok")
    ]
    Path(args.output_targeted).write_text(json.dumps(validated, indent=2), encoding="utf-8")
    print(json.dumps(report["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
