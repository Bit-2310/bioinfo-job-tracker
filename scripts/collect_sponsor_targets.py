#!/usr/bin/env python3
"""Collect ATS/careers URLs for sponsorship candidates using Bing RSS search."""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode, urlparse
import xml.etree.ElementTree as ET

import requests

USER_AGENT = "bioinfo-job-tracker/0.6"
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
class EnrichResult:
    company_name: str
    api_name: str | None
    api_url: str | None
    verified: bool
    reason: str | None


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


def search_careers_link(company: str, session: requests.Session, timeout: int) -> str | None:
    query = f"{company} careers"
    links = bing_rss_search(query, session, timeout)
    for link in links:
        if "careers" in link or "jobs" in link:
            return link
    return links[0] if links else None


def process_company(args: tuple[str, int]) -> EnrichResult:
    company, timeout = args
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    ats_links = search_ats_links(company, session, timeout)
    for link in ats_links:
        api_name, api_url = detect_from_url(link)
        if not api_name or not api_url:
            continue
        verified = request_ok(api_url, session, timeout, api_name)
        return EnrichResult(company, api_name, api_url, verified, None if verified else "api_unverified")

    careers_link = search_careers_link(company, session, timeout)
    if careers_link:
        return EnrichResult(company, "careers_url", careers_link, False, "careers_url_only")

    return EnrichResult(company, None, None, False, "no_results")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect ATS targets for sponsorship candidates via web search.")
    parser.add_argument("--candidates", default="data/target_sponsor_candidates.json")
    parser.add_argument("--targeted", default="data/targeted_list.json")
    parser.add_argument("--output", default="data/target_sponsor.json")
    parser.add_argument("--report", default="data/target_sponsor_report.json")
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=1000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    candidates_path = Path(args.candidates)
    targeted_path = Path(args.targeted)
    output_path = Path(args.output)

    with targeted_path.open(encoding="utf-8") as handle:
        targeted = json.load(handle)

    existing = {normalize_name(row.get("company_name", "")) for row in targeted}

    candidates = json.load(candidates_path.open(encoding="utf-8"))
    companies = []
    for row in candidates:
        name = (row or {}).get("company_name", "").strip()
        if not name:
            continue
        if normalize_name(name) in existing:
            continue
        companies.append(name)

    if not companies:
        print("No sponsor candidates to enrich.")
        return 0

    results = []
    batch_size = max(1, args.batch_size)
    for start in range(0, len(companies), batch_size):
        batch = companies[start:start + batch_size]
        try:
            with mp.Pool(processes=args.workers) as pool:
                results.extend(pool.map(process_company, [(c, args.timeout) for c in batch]))
        except PermissionError:
            for company in batch:
                results.append(process_company((company, args.timeout)))

    report_rows = []
    added = 0
    output_entries = []
    for result in results:
        report_rows.append(result.__dict__)
        if result.api_name and result.api_url:
            output_entries.append(
                {
                    "company_name": result.company_name,
                    "api_code": API_CODES.get(result.api_name, 0),
                    "api_name": result.api_name,
                    "api_url": result.api_url,
                }
            )
            added += 1

    output_path.write_text(json.dumps(output_entries, indent=2), encoding="utf-8")
    Path(args.report).write_text(json.dumps(report_rows, indent=2), encoding="utf-8")

    print(f"Processed {len(companies)} companies, added {added} to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
