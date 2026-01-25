#!/usr/bin/env python3
"""Repair failed entries in targeted_list_validation.json and update targeted_list.json."""

from __future__ import annotations

import argparse
import json
import re
import multiprocessing as mp
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode, urlparse
import xml.etree.ElementTree as ET

import requests

USER_AGENT = "bioinfo-job-tracker/0.8"
BING_RSS = "https://www.bing.com/search?"

API_CODES = {
    "careers_url": 0,
    "greenhouse": 1,
    "lever": 2,
    "ashby": 3,
    "icims": 4,
    "workday": 5,
    "smartrecruiters": 6,
}

ATS_DOMAINS = [
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.lever.co",
    "jobs.ashbyhq.com",
    "careers.smartrecruiters.com",
    "smartrecruiters.com",
    "myworkdayjobs.com",
    "icims.com",
]

NOT_FOUND_MARKERS = [
    "not found",
    "page not found",
    "we can't find",
    "does not exist",
    "oops",
]


@dataclass
class RepairResult:
    company_name: str
    old_api_name: str
    old_api_url: str
    new_api_name: str | None
    new_api_url: str | None
    verified: bool
    reason: str


def normalize_name(value: str) -> str:
    value = value.lower().strip().replace("&", "and")
    value = re.sub(
        r"\b(the|inc|incorporated|corp|corporation|co|company|llc|ltd|plc|gmbh|ag|sa|sarl|bv|kg|lp|llp)\b",
        "",
        value,
    )
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value


def slug_from_path(url: str) -> str | None:
    path = urlparse(url).path.strip("/")
    if not path:
        return None
    return path.split("/")[0]


def greenhouse_api_from_url(url: str) -> str | None:
    slug = slug_from_path(url)
    if not slug:
        return None
    return f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"


def lever_api_from_url(url: str) -> str | None:
    slug = slug_from_path(url)
    if not slug:
        return None
    return f"https://api.lever.co/v0/postings/{slug}"


def ashby_api_from_url(url: str) -> str | None:
    slug = slug_from_path(url)
    if not slug:
        return None
    return f"https://api.ashbyhq.com/posting-api/job-board/{slug}"


def icims_api_from_url(url: str) -> str | None:
    host = urlparse(url).hostname
    if not host or "icims.com" not in host:
        return None
    return f"https://{host}/jobs/search?ss=1"


def workday_api_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    if not parsed.hostname or "myworkdayjobs.com" not in parsed.hostname:
        return None
    host = parsed.hostname
    tenant = host.split(".")[0]
    path = parsed.path.strip("/")
    if not path:
        return None
    parts = [p for p in path.split("/") if p]
    if not parts:
        return None
    if parts[0].lower() == "en-us" and len(parts) > 1:
        site = parts[1]
    else:
        site = parts[0]
    return f"https://{tenant}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"


def smartrecruiters_api_from_url(url: str) -> str | None:
    path = urlparse(url).path.strip("/")
    if not path:
        return None
    slug = path.split("/")[0]
    return f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/"


def detect_from_url(url: str) -> tuple[str | None, str | None]:
    url_lower = url.lower()
    if "greenhouse.io" in url_lower:
        return "greenhouse", greenhouse_api_from_url(url)
    if "lever.co" in url_lower:
        return "lever", lever_api_from_url(url)
    if "ashbyhq.com" in url_lower:
        return "ashby", ashby_api_from_url(url)
    if "icims.com" in url_lower:
        return "icims", icims_api_from_url(url)
    if "myworkdayjobs.com" in url_lower:
        return "workday", workday_api_from_url(url)
    if "smartrecruiters.com" in url_lower:
        return "smartrecruiters", smartrecruiters_api_from_url(url)
    return None, None


def detect_from_html(html: str) -> tuple[str | None, str | None]:
    patterns = [
        ("greenhouse", r"https?://(?:boards|job-boards)\.greenhouse\.io/([A-Za-z0-9_-]+)"),
        ("lever", r"https?://jobs\.lever\.co/([A-Za-z0-9_-]+)"),
        ("ashby", r"https?://jobs\.ashbyhq\.com/([A-Za-z0-9_-]+)"),
        ("icims", r"https?://([A-Za-z0-9.-]+\.icims\.com)"),
        ("workday", r"https?://([A-Za-z0-9.-]+\.myworkdayjobs\.com/[^\"'\s]+)"),
        ("smartrecruiters", r"https?://(?:www\.)?smartrecruiters\.com/([A-Za-z0-9_-]+)"),
        ("smartrecruiters", r"https?://careers\.smartrecruiters\.com/([A-Za-z0-9_-]+)"),
    ]
    for api_name, pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if not match:
            continue
        if api_name == "greenhouse":
            return api_name, f"https://boards-api.greenhouse.io/v1/boards/{match.group(1)}/jobs"
        if api_name == "lever":
            return api_name, f"https://api.lever.co/v0/postings/{match.group(1)}"
        if api_name == "ashby":
            return api_name, f"https://api.ashbyhq.com/posting-api/job-board/{match.group(1)}"
        if api_name == "icims":
            return api_name, f"https://{match.group(1)}/jobs/search?ss=1"
        if api_name == "workday":
            return api_name, workday_api_from_url(f"https://{match.group(1)}")
        if api_name == "smartrecruiters":
            return api_name, f"https://api.smartrecruiters.com/v1/companies/{match.group(1)}/postings/"
    return None, None


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
        return isinstance(payload, dict) and isinstance(payload.get("jobs"), list)

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
        return isinstance(payload, dict) and isinstance(payload.get("jobs"), list)

    if api_name == "workday":
        try:
            payload = response.json()
        except ValueError:
            return False
        if isinstance(payload, dict):
            return any(key in payload for key in ("total", "jobPostings", "items"))
        return False

    if api_name == "smartrecruiters":
        try:
            payload = response.json()
        except ValueError:
            return False
        return isinstance(payload, dict) and isinstance(payload.get("content"), list)

    body = response.text.lower()
    if any(marker in body for marker in NOT_FOUND_MARKERS):
        return False

    return True


def bing_rss_search(query: str, session: requests.Session, timeout: int) -> list[str]:
    url = BING_RSS + urlencode({"q": query, "format": "rss"})
    try:
        resp = session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    except Exception:
        return []
    if resp.status_code >= 400:
        return []
    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError:
        return []

    links = []
    for item in root.findall(".//item"):
        link = item.findtext("link")
        if link:
            links.append(link.strip())
    return links


def search_ats_links(company: str, session: requests.Session, timeout: int) -> list[str]:
    site_query = " OR ".join(f"site:{d}" for d in ATS_DOMAINS)
    query = f"{company} careers ({site_query})"
    links = bing_rss_search(query, session, timeout)
    return [link for link in links if any(d in link for d in ATS_DOMAINS)]


def process_failed(args: tuple[str, str, str, int]) -> RepairResult:
    company, api_name, api_url, timeout = args
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # For careers_url, try to fetch and parse HTML for ATS links
    if api_name == "careers_url" and api_url:
        try:
            resp = session.get(api_url, timeout=timeout, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
            if resp.url:
                det_name, det_url = detect_from_url(resp.url)
                if det_name and det_url and request_ok(det_url, session, timeout, det_name):
                    return RepairResult(company, api_name, api_url, det_name, det_url, True, None)
            det_name, det_url = detect_from_html(resp.text)
            if det_name and det_url and request_ok(det_url, session, timeout, det_name):
                return RepairResult(company, api_name, api_url, det_name, det_url, True, None)
        except Exception:
            pass

    # Search ATS links via Bing
    links = search_ats_links(company, session, timeout)
    for link in links:
        det_name, det_url = detect_from_url(link)
        if det_name and det_url and request_ok(det_url, session, timeout, det_name):
            return RepairResult(company, api_name, api_url, det_name, det_url, True, None)

    return RepairResult(company, api_name, api_url, None, None, False, "no_fix")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair failed validation entries.")
    parser.add_argument("--targeted", default="data/targeted_list.json")
    parser.add_argument("--validation", default="data/targeted_list_validation.json")
    parser.add_argument("--report", default="data/validation_repair_report.json")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--timeout", type=int, default=15)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    targeted_path = Path(args.targeted)
    with targeted_path.open(encoding="utf-8") as handle:
        targeted = json.load(handle)

    validation = json.load(open(args.validation, encoding="utf-8"))
    failed = [
        (row.get("company_name", ""), row.get("original_api_name", ""), row.get("original_api_url", ""), args.timeout)
        for row in validation
        if row.get("original_success") is False
    ]

    if not failed:
        print("No failed entries to repair.")
        return 0

    with mp.Pool(processes=args.workers) as pool:
        results = pool.map(process_failed, failed)

    report_rows = []
    fixed = 0
    for result in results:
        report_rows.append(result.__dict__)
        if result.verified and result.new_api_name and result.new_api_url:
            for row in targeted:
                if row.get("company_name") == result.company_name:
                    row["api_name"] = result.new_api_name
                    row["api_url"] = result.new_api_url
                    row["api_code"] = API_CODES.get(result.new_api_name, row.get("api_code", 0))
                    fixed += 1
                    break

    with targeted_path.open("w", encoding="utf-8") as handle:
        json.dump(targeted, handle, indent=2)

    with Path(args.report).open("w", encoding="utf-8") as handle:
        json.dump(report_rows, handle, indent=2)

    print(f"Processed {len(failed)} failed entries, fixed {fixed}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
