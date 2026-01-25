#!/usr/bin/env python3
"""Enrich targeted_list.json using data/newlist.csv careers URLs and ATS hints."""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.parse import urlencode

import requests

USER_AGENT = "bioinfo-job-tracker/0.5"

API_CODES = {
    "careers_url": 0,
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
ATS_SEARCH_DOMAINS = [
    "myworkdayjobs.com",
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.lever.co",
    "jobs.ashbyhq.com",
    "smartrecruiters.com",
    "careers.smartrecruiters.com",
    "icims.com",
]


@dataclass
class NewListRow:
    company: str
    careers_url: str
    ats: str


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


def smartrecruiters_api_from_url(url: str) -> str | None:
    path = urlparse(url).path.strip("/")
    if not path:
        return None
    slug = path.split("/")[0]
    return f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/"


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


def request_ok(url: str, session: requests.Session, timeout: int, api_name: str) -> tuple[bool, int | None]:
    try:
        response = session.get(url, timeout=timeout, allow_redirects=True)
    except Exception:
        return False, None

    if response.status_code >= 400:
        return False, None

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


def search_ats_via_ddg(company: str, session: requests.Session, timeout: int) -> list[str]:
    query = f"{company} careers {' OR '.join(ATS_SEARCH_DOMAINS)}"
    url = "https://duckduckgo.com/lite/?" + urlencode({"q": query})
    try:
        resp = session.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0"},
        )
    except Exception:
        return []
    if resp.status_code >= 400:
        return []
    html = resp.text
    links = re.findall(r'href=\"(https?://[^\"]+)\"', html)
    results = []
    for link in links:
        if any(domain in link for domain in ATS_SEARCH_DOMAINS):
            results.append(link)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich targeted_list.json from newlist.csv")
    parser.add_argument("--newlist", default="data/newlist.csv", help="Path to newlist.csv")
    parser.add_argument(
        "--targeted",
        default="data/targeted_list.json",
        help="Path to targeted_list.json",
    )
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout")
    parser.add_argument(
        "--report",
        default="data/newlist_unverified.json",
        help="Path to write failures report.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    newlist_path = Path(args.newlist)
    targeted_path = Path(args.targeted)

    with targeted_path.open(encoding="utf-8") as handle:
        targeted = json.load(handle)

    existing = {normalize_name(row.get("company_name", "")) for row in targeted}

    rows: list[NewListRow] = []
    with newlist_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                NewListRow(
                    company=row.get("COMPANY", "").strip(),
                    careers_url=row.get("CAREERS_URL", "").strip(),
                    ats=row.get("ATS", "").strip(),
                )
            )

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    added = 0
    failures = []

    for row in rows:
        if not row.company:
            continue
        if normalize_name(row.company) in existing:
            continue

        api_name = None
        api_url = None

        ats = row.ats.lower()
        if ats in API_CODES:
            api_name = ats
            if api_name == "greenhouse":
                api_url = greenhouse_api_from_url(row.careers_url)
            elif api_name == "lever":
                api_url = lever_api_from_url(row.careers_url)
            elif api_name == "ashby":
                api_url = ashby_api_from_url(row.careers_url)
            elif api_name == "icims":
                api_url = icims_api_from_url(row.careers_url)
            elif api_name == "workday":
                api_url = workday_api_from_url(row.careers_url)
            elif api_name == "smartrecruiters":
                api_url = smartrecruiters_api_from_url(row.careers_url)
            if api_url is None:
                api_name = None

        if not api_name and row.careers_url:
            api_name, api_url = detect_from_url(row.careers_url)

        if not api_name and row.careers_url:
            try:
                resp = session.get(row.careers_url, timeout=args.timeout, allow_redirects=True)
                if resp.url and not api_name:
                    api_name, api_url = detect_from_url(resp.url)
                if not api_name:
                    html = resp.text
                    api_name, api_url = detect_from_html(html)
            except Exception:
                api_name = None
                api_url = None

        if not api_name:
            candidates = search_ats_via_ddg(row.company, session, args.timeout)
            for link in candidates:
                api_name, api_url = detect_from_url(link)
                if api_name and api_url:
                    break

        if not api_name or not api_url:
            status = None
            if row.careers_url:
                try:
                    resp = session.get(
                        row.careers_url,
                        timeout=args.timeout,
                        allow_redirects=True,
                        headers={"User-Agent": "Mozilla/5.0"},
                    )
                    status = resp.status_code
                except Exception:
                    status = None

                targeted.append(
                    {
                        "company_name": row.company,
                        "api_code": API_CODES["careers_url"],
                        "api_name": "careers_url",
                        "api_url": row.careers_url,
                    }
                )
                existing.add(normalize_name(row.company))
                added += 1

                failures.append(
                    {
                        "company": row.company,
                        "careers_url": row.careers_url,
                        "reason": "careers_url_unverified" if status is None or status >= 400 else "careers_url_only",
                        "status": status,
                    }
                )
                continue

            failures.append(
                {
                    "company": row.company,
                    "careers_url": row.careers_url,
                    "reason": "no_ats_detected",
                }
            )
            continue

        ok, _ = request_ok(api_url, session, args.timeout, api_name)
        if not ok:
            targeted.append(
                {
                    "company_name": row.company,
                    "api_code": API_CODES[api_name],
                    "api_name": api_name,
                    "api_url": api_url,
                }
            )
            existing.add(normalize_name(row.company))
            added += 1
            failures.append(
                {
                    "company": row.company,
                    "careers_url": row.careers_url,
                    "reason": "api_unverified",
                    "api_name": api_name,
                    "api_url": api_url,
                }
            )
            continue

        targeted.append(
            {
                "company_name": row.company,
                "api_code": API_CODES[api_name],
                "api_name": api_name,
                "api_url": api_url,
            }
        )
        existing.add(normalize_name(row.company))
        added += 1

    with targeted_path.open("w", encoding="utf-8") as handle:
        json.dump(targeted, handle, indent=2)

    if failures:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(failures, handle, indent=2)

    print(f"Added {added} companies to {targeted_path}")
    if failures:
        print(f"Unverified: {len(failures)} -> {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
