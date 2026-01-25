#!/usr/bin/env python3
"""One-off parallel job pull test for successful validations."""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning
import warnings

USER_AGENT = "Mozilla/5.0"


@dataclass
class JobPullResult:
    company_name: str
    api_name: str
    api_url: str
    status: str
    http_status: int | None
    jobs_count: int | None
    jobs_count_method: str | None
    error: str | None


def count_jobs_from_html(html: str, base_url: str) -> tuple[int | None, str | None]:
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    soup = BeautifulSoup(html, "html.parser")

    # Heuristic 1: links with job-like paths on same domain
    base_host = urlparse(base_url).hostname or ""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue
        if href.startswith("/"):
            href = f"{urlparse(base_url).scheme}://{base_host}{href}"
        host = urlparse(href).hostname or ""
        if host and base_host and host != base_host:
            continue
        if re.search(r"\bjob(s)?\b|careers|positions|openings", href, re.IGNORECASE):
            links.add(href)

    if links:
        return len(links), "links"

    # Heuristic 2: data-job-id like attributes
    job_nodes = soup.find_all(attrs={
        "data-job-id": True
    })
    if job_nodes:
        return len(job_nodes), "data-job-id"

    # Heuristic 3: embedded JSON with jobId
    job_ids = re.findall(r"\"jobId\"\s*:\s*\"?([A-Za-z0-9_-]+)\"?", html)
    if job_ids:
        return len(set(job_ids)), "jobId_json"

    # Heuristic 4: requisitionId / job_id / postingId / jobTitle
    requisition_ids = re.findall(r"\"requisitionId\"\s*:\s*\"?([A-Za-z0-9_-]+)\"?", html, re.IGNORECASE)
    if requisition_ids:
        return len(set(requisition_ids)), "requisitionId_json"

    job_ids_alt = re.findall(r"\"job_id\"\s*:\s*\"?([A-Za-z0-9_-]+)\"?", html, re.IGNORECASE)
    if job_ids_alt:
        return len(set(job_ids_alt)), "job_id_json"

    posting_ids = re.findall(r"\"postingId\"\s*:\s*\"?([A-Za-z0-9_-]+)\"?", html, re.IGNORECASE)
    if posting_ids:
        return len(set(posting_ids)), "postingId_json"

    titles = re.findall(r"\"jobTitle\"\s*:\s*\"([^\"]+)\"", html, re.IGNORECASE)
    if titles:
        return len(set(titles)), "jobTitle_json"

    return None, None


def detect_ats_from_html(html: str) -> list[tuple[str, str]]:
    patterns = [
        ("greenhouse", r"https?://(?:boards|job-boards)\\.greenhouse\\.io/([A-Za-z0-9_-]+)"),
        ("lever", r"https?://jobs\\.lever\\.co/([A-Za-z0-9_-]+)"),
        ("ashby", r"https?://jobs\\.ashbyhq\\.com/([A-Za-z0-9_-]+)"),
        ("icims", r"https?://([A-Za-z0-9.-]+\\.icims\\.com)"),
        ("workday", r"https?://([A-Za-z0-9.-]+\\.myworkdayjobs\\.com/[^\\\"'\\s]+)"),
        ("smartrecruiters", r"https?://(?:www\\.)?smartrecruiters\\.com/([A-Za-z0-9_-]+)"),
        ("smartrecruiters", r"https?://careers\\.smartrecruiters\\.com/([A-Za-z0-9_-]+)"),
    ]
    results = []
    for api_name, pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if not match:
            continue
        if api_name == "greenhouse":
            results.append((api_name, f"https://boards-api.greenhouse.io/v1/boards/{match.group(1)}/jobs"))
        elif api_name == "lever":
            results.append((api_name, f"https://api.lever.co/v0/postings/{match.group(1)}"))
        elif api_name == "ashby":
            results.append((api_name, f"https://api.ashbyhq.com/posting-api/job-board/{match.group(1)}"))
        elif api_name == "icims":
            results.append((api_name, f"https://{match.group(1)}/jobs/search?ss=1"))
        elif api_name == "workday":
            results.append((api_name, f"https://{match.group(1)}"))
        elif api_name == "smartrecruiters":
            results.append((api_name, f"https://api.smartrecruiters.com/v1/companies/{match.group(1)}/postings/"))
    return results


def workday_post_attempt(url: str, timeout: int) -> int | None:
    payload = {
        "appliedFacets": {},
        "limit": 20,
        "offset": 0,
        "searchText": "",
    }
    headers = {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
    except Exception:
        return None
    if resp.status_code >= 400:
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    if isinstance(data, dict):
        if isinstance(data.get("jobPostings"), list):
            return len(data["jobPostings"])
        if isinstance(data.get("items"), list):
            return len(data["items"])
        if isinstance(data.get("total"), int):
            return data["total"]
    return None


def fetch_company(args: tuple[dict, dict]) -> JobPullResult:
    row, lookup = args
    name = row.get("company_name")
    info = lookup.get(name, {})
    api_name = info.get("api_name") or row.get("original_api_name")
    api_url = info.get("api_url") or row.get("original_api_url")

    if not api_url:
        return JobPullResult(name, api_name or "", "", "no_url", None, None, None, "missing_url")

    try:
        resp = requests.get(api_url, timeout=20, allow_redirects=True, headers={"User-Agent": USER_AGENT})
        http_status = resp.status_code
        if http_status >= 400:
            return JobPullResult(name, api_name or "", api_url, "http_error", http_status, None, None, None)

        jobs_count = None
        jobs_method = None

        if api_name == "greenhouse":
            payload = resp.json()
            jobs = payload.get("jobs", [])
            jobs_count = len(jobs) if isinstance(jobs, list) else None
            jobs_method = "greenhouse_json"
        elif api_name == "lever":
            payload = resp.json()
            jobs_count = len(payload) if isinstance(payload, list) else None
            jobs_method = "lever_json"
        elif api_name == "ashby":
            payload = resp.json()
            jobs = payload.get("jobs", [])
            jobs_count = len(jobs) if isinstance(jobs, list) else None
            jobs_method = "ashby_json"
        elif api_name == "workday":
            payload = resp.json()
            if isinstance(payload, dict):
                if isinstance(payload.get("jobPostings"), list):
                    jobs_count = len(payload["jobPostings"])
                    jobs_method = "workday_jobPostings"
                elif isinstance(payload.get("items"), list):
                    jobs_count = len(payload["items"])
                    jobs_method = "workday_items"
                else:
                    jobs_count = payload.get("total")
                    jobs_method = "workday_total"
            if jobs_count is None:
                alt = workday_post_attempt(api_url, 20)
                if alt is not None:
                    jobs_count = alt
                    jobs_method = "workday_post"
        elif api_name == "smartrecruiters":
            payload = resp.json()
            if isinstance(payload, dict) and isinstance(payload.get("content"), list):
                jobs_count = len(payload["content"])
                jobs_method = "smartrecruiters_json"
        elif api_name == "careers_url":
            jobs_count, jobs_method = count_jobs_from_html(resp.text, resp.url)
            if jobs_count is None:
                ats_candidates = detect_ats_from_html(resp.text)
                for ats_name, ats_url in ats_candidates:
                    try:
                        ats_resp = requests.get(ats_url, timeout=20, allow_redirects=True, headers={"User-Agent": USER_AGENT})
                    except Exception:
                        continue
                    if ats_resp.status_code >= 400:
                        continue
                    try:
                        if ats_name == "greenhouse":
                            payload = ats_resp.json()
                            jobs = payload.get("jobs", [])
                            if isinstance(jobs, list):
                                jobs_count = len(jobs)
                                jobs_method = "greenhouse_html_link"
                                break
                        if ats_name == "lever":
                            payload = ats_resp.json()
                            if isinstance(payload, list):
                                jobs_count = len(payload)
                                jobs_method = "lever_html_link"
                                break
                        if ats_name == "ashby":
                            payload = ats_resp.json()
                            jobs = payload.get("jobs", [])
                            if isinstance(jobs, list):
                                jobs_count = len(jobs)
                                jobs_method = "ashby_html_link"
                                break
                        if ats_name == "smartrecruiters":
                            payload = ats_resp.json()
                            if isinstance(payload, dict) and isinstance(payload.get("content"), list):
                                jobs_count = len(payload["content"])
                                jobs_method = "smartrecruiters_html_link"
                                break
                    except Exception:
                        continue

        return JobPullResult(name, api_name or "", api_url, "ok", http_status, jobs_count, jobs_method, None)
    except Exception as exc:
        return JobPullResult(name, api_name or "", api_url, "error", None, None, None, str(exc))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-off job pull test.")
    parser.add_argument("--validation", default="data/targeted_list_validation.json")
    parser.add_argument("--targeted", default="data/targeted_list.json")
    parser.add_argument("--output", default="data/job_pull_test.json")
    parser.add_argument("--workers", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    validation = json.load(open(args.validation, encoding="utf-8"))
    targeted = json.load(open(args.targeted, encoding="utf-8"))

    lookup = {row["company_name"]: row for row in targeted if row.get("company_name")}
    success = [row for row in validation if row.get("original_success")]

    with mp.Pool(processes=args.workers) as pool:
        results = pool.map(fetch_company, [(row, lookup) for row in success])

    payload = [r.__dict__ for r in results]
    Path(args.output).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"saved {args.output} count {len(payload)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
