#!/usr/bin/env python3
"""Pull jobs from ATS sources, filter them, and emit CSVs for the UI."""

from __future__ import annotations

import argparse
import csv
import json
import re
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning
import warnings

USER_AGENT = "bioinfo-job-tracker/1.0"


@dataclass
class JobRecord:
    company: str
    job_title: str
    location: str
    remote_or_hybrid: str
    posting_date: str
    source: str
    job_url: str
    job_id: str
    description: str
    list_source: str


def normalize_text(text: str) -> str:
    return (text or "").strip()


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def parse_date(value: str) -> str:
    if not value:
        return ""
    value = value.strip()
    # ISO 8601
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        pass
    # Common formats
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.date().isoformat()
        except Exception:
            continue
    return ""


def age_days(posting_date: str) -> int | None:
    if not posting_date:
        return None
    try:
        dt = datetime.fromisoformat(posting_date)
        now = datetime.now(timezone.utc).date()
        return (now - dt.date()).days
    except Exception:
        return None


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def load_targets(paths: list[Path]) -> list[tuple[dict, str]]:
    rows: list[tuple[dict, str]] = []
    for path in paths:
        data = load_json(path)
        for row in data:
            if not isinstance(row, dict):
                continue
            if row.get("api_name") and row.get("api_url"):
                rows.append((row, path.name))
                continue
            if row.get("original_api_name") and row.get("original_api_url"):
                mapped = dict(row)
                mapped["api_name"] = row.get("original_api_name")
                mapped["api_url"] = row.get("original_api_url")
                rows.append((mapped, path.name))
                continue
    # de-dupe by company+api_url
    seen = set()
    unique: list[tuple[dict, str]] = []
    for row, source in rows:
        key = (row.get("company_name"), row.get("api_url"))
        if key in seen:
            continue
        seen.add(key)
        unique.append((row, source))
    return unique


def request_json(url: str, session: requests.Session, retries: int = 2) -> dict | list | None:
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, timeout=20, allow_redirects=True)
            if resp.status_code >= 400:
                return None
            return resp.json()
        except Exception:
            if attempt >= retries:
                return None
    return None


def detect_remote(text: str) -> str:
    text = (text or "").lower()
    if "remote" in text:
        return "remote"
    if "hybrid" in text:
        return "hybrid"
    return "onsite"


def pull_greenhouse(company: str, url: str, session: requests.Session, list_source: str) -> list[JobRecord]:
    if "content=true" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}content=true"
    payload = request_json(url, session)
    if not isinstance(payload, dict):
        return []
    jobs = payload.get("jobs", [])
    if not isinstance(jobs, list):
        return []
    results = []
    for job in jobs:
        title = normalize_text(job.get("title"))
        loc = normalize_text(job.get("location", {}).get("name", ""))
        job_url = normalize_text(job.get("absolute_url"))
        posted = parse_date(job.get("updated_at") or job.get("created_at") or "")
        remote = detect_remote(title + " " + loc)
        results.append(
            JobRecord(
                company=company,
                job_title=title,
                location=loc,
                remote_or_hybrid=remote,
                posting_date=posted,
                source="greenhouse",
                job_url=job_url,
                job_id=str(job.get("id", "")),
                description=normalize_text(job.get("content", "")),
                list_source=list_source,
            )
        )
    return results


def pull_lever(company: str, url: str, session: requests.Session, list_source: str) -> list[JobRecord]:
    payload = request_json(url, session)
    if not isinstance(payload, list):
        return []
    results = []
    for job in payload:
        title = normalize_text(job.get("text"))
        loc = normalize_text(job.get("categories", {}).get("location", ""))
        job_url = normalize_text(job.get("hostedUrl"))
        posted = parse_date(job.get("createdAt") and datetime.utcfromtimestamp(job.get("createdAt") / 1000).date().isoformat())
        remote = detect_remote(title + " " + loc)
        description = normalize_text(job.get("description", ""))
        results.append(
            JobRecord(
                company=company,
                job_title=title,
                location=loc,
                remote_or_hybrid=remote,
                posting_date=posted,
                source="lever",
                job_url=job_url,
                job_id=str(job.get("id", "")),
                description=description,
                list_source=list_source,
            )
        )
    return results


def pull_ashby(company: str, url: str, session: requests.Session, list_source: str) -> list[JobRecord]:
    payload = request_json(url, session)
    if not isinstance(payload, dict):
        return []
    jobs = payload.get("jobs", [])
    if not isinstance(jobs, list):
        return []
    results = []
    for job in jobs:
        title = normalize_text(job.get("title"))
        loc = normalize_text(job.get("location", ""))
        job_url = normalize_text(job.get("jobUrl"))
        posted = parse_date(job.get("updatedAt") or job.get("createdAt") or "")
        remote = detect_remote(title + " " + loc)
        results.append(
            JobRecord(
                company=company,
                job_title=title,
                location=loc,
                remote_or_hybrid=remote,
                posting_date=posted,
                source="ashby",
                job_url=job_url,
                job_id=str(job.get("id", "")),
                description=normalize_text(job.get("description", "")),
                list_source=list_source,
            )
        )
    return results


def pull_smartrecruiters(company: str, url: str, session: requests.Session, list_source: str) -> list[JobRecord]:
    results = []
    offset = 0
    limit = 100
    while True:
        separator = "&" if "?" in url else "?"
        page_url = f"{url}{separator}offset={offset}&limit={limit}&country=us"
        payload = request_json(page_url, session)
        if not isinstance(payload, dict):
            break
        jobs = payload.get("content", [])
        if not isinstance(jobs, list) or not jobs:
            break
        for job in jobs:
            title = normalize_text(job.get("name"))
            loc = normalize_text(job.get("location", {}).get("city", ""))
            job_url = normalize_text(job.get("ref", ""))
            posted = parse_date(job.get("releasedDate") or "")
            remote = detect_remote(title + " " + loc)
            results.append(
                JobRecord(
                    company=company,
                    job_title=title,
                    location=loc,
                    remote_or_hybrid=remote,
                    posting_date=posted,
                    source="smartrecruiters",
                    job_url=job_url,
                    job_id=str(job.get("id", "")),
                    description=normalize_text(job.get("jobAd", {}).get("sections", {}).get("jobDescription", "")),
                    list_source=list_source,
                )
            )
        if len(jobs) < limit:
            break
        offset += limit
    return results


def pull_workday(company: str, url: str, session: requests.Session, list_source: str) -> list[JobRecord]:
    payload = request_json(url, session)
    if not isinstance(payload, dict):
        return []
    jobs = payload.get("jobPostings") or payload.get("items") or []
    if not isinstance(jobs, list):
        return []
    results = []
    for job in jobs:
        title = normalize_text(job.get("title") or job.get("jobTitle"))
        loc = normalize_text(job.get("locationsText") or job.get("location") or "")
        job_url = normalize_text(job.get("externalPath") or "")
        posted = parse_date(job.get("postedOn") or "")
        remote = detect_remote(title + " " + loc)
        results.append(
            JobRecord(
                company=company,
                job_title=title,
                location=loc,
                remote_or_hybrid=remote,
                posting_date=posted,
                source="workday",
                job_url=job_url,
                job_id=str(job.get("id", "")),
                description=normalize_text(job.get("jobDescription", "")),
                list_source=list_source,
            )
        )
    return results


def pull_icims(company: str, url: str, session: requests.Session, list_source: str) -> list[JobRecord]:
    try:
        resp = session.get(url, timeout=20, allow_redirects=True)
        if resp.status_code >= 400:
            return []
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return []

    results = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/jobs/" not in href:
            continue
        title = normalize_text(a.get_text(" "))
        if not title:
            continue
        if href.startswith("/"):
            href = f"{urlparse(url).scheme}://{urlparse(url).hostname}{href}"
        results.append(
            JobRecord(
                company=company,
                job_title=title,
                location="",
                remote_or_hybrid=detect_remote(title),
                posting_date="",
                source="icims",
                job_url=href,
                job_id="",
                description="",
                list_source=list_source,
            )
        )
    return results


def pull_careers_url(company: str, url: str, session: requests.Session, list_source: str) -> list[JobRecord]:
    try:
        resp = session.get(url, timeout=20, allow_redirects=True, headers={"User-Agent": USER_AGENT})
        if resp.status_code >= 400:
            return []
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return []

    base_host = urlparse(resp.url).hostname or ""
    results = []
    seen = set()
    # Basic role title guard to avoid nav links like "students and graduates"
    role_keywords = [
        "SCIENTIST",
        "ENGINEER",
        "ANALYST",
        "BIOINFORMATICS",
        "COMPUTATIONAL",
        "DATA",
        "RESEARCH",
        "DEVELOPER",
        "PROGRAMMER",
        "BIOLOGIST",
        "GENOMICS",
        "INFORMATICS",
        "SOFTWARE",
        "ML",
        "AI",
        "MACHINE LEARNING",
        "STATISTICIAN",
        "BIOSTATISTICIAN",
        "QA",
        "QC",
        "INTERN",
        "ASSOCIATE",
        "FELLOW",
        "MANAGER",
        "DIRECTOR",
        "LEAD",
        "SENIOR",
        "PRINCIPAL",
    ]
    nav_phrases = [
        "STUDENTS AND GRADUATES",
        "WHY",
        "YOUR CAREER",
        "OUR DEPARTMENTS",
        "MEET OUR COLLEAGUES",
        "LATEST UPDATES",
        "INFORMATION CENTRE",
        "EXPLORE ALL CAREERS",
        "GLOBAL",
        "CAREERS",
        "JOBS",
        "OPENINGS",
        "POSITIONS",
        "COUNTRY",
        "REGION",
    ]
    country_terms = [
        "AUSTRIA",
        "BELGIUM",
        "BRAZIL",
        "CHILE",
        "COLOMBIA",
        "COSTA RICA",
        "DENMARK",
        "FINLAND",
        "FRANCE",
        "GERMANY",
        "INDIA",
        "IRELAND",
        "ITALY",
        "JAPAN",
        "KOREA",
        "MEXICO",
        "NETHERLANDS",
        "NORWAY",
        "POLAND",
        "PORTUGAL",
        "SINGAPORE",
        "SPAIN",
        "SWEDEN",
        "SWITZERLAND",
        "UNITED KINGDOM",
        "UK",
        "CANADA",
    ]
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue
        if href.startswith("/"):
            href = f"{urlparse(resp.url).scheme}://{base_host}{href}"
        host = urlparse(href).hostname or ""
        if host and base_host and host != base_host:
            continue
        if not re.search(r"\bjob(s)?\b|careers|positions|openings", href, re.IGNORECASE):
            continue
        title_raw = a.get_text(" ")
        title = normalize_text(title_raw)
        if not title:
            continue
        title_norm = normalize(title)
        if not match_any(role_keywords, title_norm):
            continue
        if match_any(nav_phrases, title_norm):
            continue
        if match_any(country_terms, title_norm):
            continue
        key = (title, href)
        if key in seen:
            continue
        seen.add(key)
        results.append(
            JobRecord(
                company=company,
                job_title=title,
                location="",
                remote_or_hybrid=detect_remote(title),
                posting_date="",
                source="careers_url",
                job_url=href,
                job_id="",
                description="",
                list_source=list_source,
            )
        )
    return results


def pull_jobs_for_target(row: dict, session: requests.Session, list_source: str) -> list[JobRecord]:
    company = row.get("company_name", "")
    api_name = row.get("api_name", "")
    api_url = row.get("api_url", "")

    if not company or not api_name or not api_url:
        return []

    if api_name == "greenhouse":
        return pull_greenhouse(company, api_url, session, list_source)
    if api_name == "lever":
        return pull_lever(company, api_url, session, list_source)
    if api_name == "ashby":
        return pull_ashby(company, api_url, session, list_source)
    if api_name == "smartrecruiters":
        return pull_smartrecruiters(company, api_url, session, list_source)
    if api_name == "workday":
        return pull_workday(company, api_url, session, list_source)
    if api_name == "icims":
        return pull_icims(company, api_url, session, list_source)
    if api_name == "careers_url":
        return pull_careers_url(company, api_url, session, list_source)

    return []


def normalize(text: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", " ", text or "")
    value = re.sub(r"\s+", " ", value).strip()
    return value.upper()


def is_phrase(token: str) -> bool:
    return " " in normalize(token)


def match_token(token: str, text_norm: str) -> bool:
    token_norm = normalize(token)
    if not token_norm:
        return False
    if " " in token_norm:
        return token_norm in text_norm
    if len(token_norm) <= 3:
        return re.search(rf"\b{re.escape(token_norm)}\b", text_norm) is not None
    return token_norm in text_norm


def match_any(tokens: list[str], text_norm: str) -> bool:
    return any(match_token(token, text_norm) for token in tokens or [])


def match_all(tokens: list[str], text_norm: str) -> bool:
    return all(match_token(token, text_norm) for token in tokens or [])


def count_matches(tokens: list[str], text_norm: str) -> int:
    return sum(1 for token in tokens or [] if match_token(token, text_norm))


def build_filter_text(job: JobRecord) -> str:
    return normalize(
        " ".join(
            [
                job.job_title,
                job.location,
                job.description,
            ]
        )
    )


def keyword_hits(text: str, keywords: list[str]) -> int:
    return sum(1 for kw in keywords if kw in text)


def upper_list(values: Iterable[str]) -> list[str]:
    return [str(v).upper() for v in values or []]


US_STATE_ABBREVIATIONS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
    "VA", "WA", "WV", "WI", "WY", "DC",
}

NON_US_LOCATION_TOKENS = [
    "CANADA",
    "UNITED KINGDOM",
    "UK",
    "ENGLAND",
    "LONDON",
    "EUROPE",
    "EMEA",
    "APAC",
    "LATAM",
    "INDIA",
    "CHINA",
    "SINGAPORE",
    "GERMANY",
    "FRANCE",
    "SPAIN",
    "ITALY",
    "JAPAN",
    "KOREA",
    "AUSTRALIA",
    "IRELAND",
    "NETHERLANDS",
    "SWITZERLAND",
]

def is_us_location(text: str) -> bool:
    value = normalize(text or "")
    if not value:
        return False
    if match_any(["UNITED STATES", "USA", "US"], value):
        return True
    if match_any(["REMOTE", "HYBRID"], value) and not match_any(NON_US_LOCATION_TOKENS, value):
        return True
    if "WASHINGTON DC" in value:
        return True
    # City, ST pattern
    parts = [p.strip() for p in value.split(",")]
    if len(parts) >= 2:
        state = parts[-1].replace(".", "").strip()
        if state in US_STATE_ABBREVIATIONS:
            return True
    if " DC" in value:
        return True
    return False


def filter_jobs(jobs: list[JobRecord], filter_cfg: dict) -> tuple[list[dict], list[dict], dict]:
    results: list[dict] = []
    dropped: list[dict] = []
    drop_stats: dict[str, int] = {}
    location_include = upper_list(filter_cfg.get("location_filter", {}).get("include_any", []))
    location_exclude = upper_list(filter_cfg.get("location_filter", {}).get("exclude_any", []))
    title_filter = filter_cfg.get("title_filter", {})
    title_include = upper_list(title_filter.get("include_any", []))
    title_strict = upper_list(title_filter.get("strict_include_any", []))
    title_soft = upper_list(title_filter.get("soft_include_any", []))
    title_soft.extend(
        [
            "BIOINFORMATICS",
            "BIOINFORMATICS SCIENTIST",
            "BIOINFORMATICS ANALYST",
            "BIOINFORMATICS ENGINEER",
            "COMPUTATIONAL BIOLOGY",
            "COMPUTATIONAL BIOLOGIST",
            "COMPUTATIONAL SCIENTIST",
            "COMPUTATIONAL GENOMICS",
            "GENOMICS",
            "GENOMICS SCIENTIST",
            "GENOMIC",
            "GENOMIC DATA SCIENTIST",
            "BIOMEDICAL DATA SCIENTIST",
            "TRANSCRIPTOMICS",
            "SINGLE CELL",
            "SINGLE-CELL",
            "OMICS",
            "MULTI-OMICS",
            "NGS",
            "SEQUENCING",
            "PIPELINE",
            "WORKFLOW",
            "ALGORITHMS SCIENTIST (GENOMICS)",
        ]
    )
    title_exclude = upper_list(title_filter.get("exclude_any", []))
    seniority_exclude = upper_list(filter_cfg.get("seniority_filter", {}).get("exclude_any", []))
    experience_exclude = upper_list(filter_cfg.get("experience_traps", {}).get("exclude_if_contains_any", []))
    experience_filter = filter_cfg.get("experience_filter", {})
    experience_block = upper_list(experience_filter.get("exclude_if_contains_any", []))
    global_exclude = upper_list(filter_cfg.get("global_exclusions", {}).get("exclude_if_contains_any", []))
    employment_exclude = upper_list(filter_cfg.get("global_exclusions", {}).get("employment_type_excludes_any", []))
    hard_must_have = upper_list(filter_cfg.get("hard_gates", {}).get("must_have_any", []))
    domain_gates = filter_cfg.get("hard_gates", {}).get("domain_gates_any_of", [])
    scoring = filter_cfg.get("keyword_scoring", {})
    scoring_strong = upper_list(scoring.get("strong", []))
    scoring_medium = upper_list(scoring.get("medium", []))
    scoring_nice = upper_list(scoring.get("nice_to_have", []))
    scoring_neg_high = upper_list(scoring.get("negative_keywords", {}).get("high_penalty", []))
    scoring_neg_medium = upper_list(scoring.get("negative_keywords", {}).get("medium_penalty", []))
    weak_domain_signals = [
        "GENOM",
        "RNA",
        "OMICS",
        "SEQUENC",
        "TRANSCRIPT",
        "VARIANT",
        "SINGLE CELL",
    ]
    bio_context_terms = [
        "GENE",
        "GENOM",
        "GENOME",
        "DNA",
        "RNA",
        "PROTEIN",
        "CELL",
        "SEQUENC",
        "OMICS",
        "BIOINFORMATICS",
        "TRANSCRIPTOMICS",
        "NGS",
        "ASSAY",
        "EXPRESSION",
        "VARIANT",
        "PATHWAY",
        "CLINICAL",
        "MICROBIOME",
    ]
    for job in jobs:
        text = build_filter_text(job)
        title = normalize(job.job_title or "")
        location = normalize(job.location or "")
        location_basis = normalize(job.location or "")
        description = normalize(job.description or "")
        has_description = len((job.description or "").strip()) >= 40
        stage1_pass_reasons = []

        # Location include/exclude (soft include, hard exclude)
        location_match = False
        if location_basis:
            location_match = is_us_location(location_basis)
        if not location_match and location_include and location_basis:
            location_match = match_any(location_include, location_basis)
        non_us_tokens = NON_US_LOCATION_TOKENS
        if match_any(location_exclude, text):
            dropped.append(
                {
                    "company": job.company,
                    "job_title": job.job_title,
                    "location": job.location,
                    "remote_or_hybrid": job.remote_or_hybrid,
                    "posting_date": job.posting_date,
                    "source": job.source,
                    "job_url": job.job_url,
                    "score": 0,
                    "list_source": job.list_source,
                    "stage1_drop_reason": "location_exclude",
                }
            )
            drop_stats["location_exclude"] = drop_stats.get("location_exclude", 0) + 1
            continue
        if location_basis and match_any(non_us_tokens, location_basis) and not is_us_location(location_basis):
            dropped.append(
                {
                    "company": job.company,
                    "job_title": job.job_title,
                    "location": job.location,
                    "remote_or_hybrid": job.remote_or_hybrid,
                    "posting_date": job.posting_date,
                    "source": job.source,
                    "job_url": job.job_url,
                    "score": 0,
                    "list_source": job.list_source,
                    "stage1_drop_reason": "location_exclude_non_us",
                }
            )
            drop_stats["location_exclude_non_us"] = drop_stats.get("location_exclude_non_us", 0) + 1
            continue

        # Title include/exclude
        title_match = False
        title_strict_match = False
        if title_include:
            title_match = match_any(title_include, title)
        if title_strict:
            title_strict_match = match_any(title_strict, title)
            if title_strict_match:
                title_match = True
        if title_soft and not title_match:
            title_match = match_any(title_soft, title)
        if match_any(title_exclude, title):
            dropped.append(
                {
                    "company": job.company,
                    "job_title": job.job_title,
                    "location": job.location,
                    "remote_or_hybrid": job.remote_or_hybrid,
                    "posting_date": job.posting_date,
                    "source": job.source,
                    "job_url": job.job_url,
                    "score": 0,
                    "list_source": job.list_source,
                    "stage1_drop_reason": "title_exclude",
                }
            )
            drop_stats["title_exclude"] = drop_stats.get("title_exclude", 0) + 1
            continue
        if match_any(["PIPELINE"], title) and match_any(
            ["COMMERCIAL", "MARKET ACCESS", "STRATEGY", "OPERATIONS", "SALES", "MARKETING"],
            title,
        ):
            dropped.append(
                {
                    "company": job.company,
                    "job_title": job.job_title,
                    "location": job.location,
                    "remote_or_hybrid": job.remote_or_hybrid,
                    "posting_date": job.posting_date,
                    "source": job.source,
                    "job_url": job.job_url,
                    "score": 0,
                    "list_source": job.list_source,
                    "stage1_drop_reason": "title_exclude_pipeline_business",
                }
            )
            drop_stats["title_exclude_pipeline_business"] = drop_stats.get("title_exclude_pipeline_business", 0) + 1
            continue

        # Seniority include/exclude
        if match_any(seniority_exclude, title):
            dropped.append(
                {
                    "company": job.company,
                    "job_title": job.job_title,
                    "location": job.location,
                    "remote_or_hybrid": job.remote_or_hybrid,
                    "posting_date": job.posting_date,
                    "source": job.source,
                    "job_url": job.job_url,
                    "score": 0,
                    "list_source": job.list_source,
                    "stage1_drop_reason": "seniority_exclude",
                }
            )
            drop_stats["seniority_exclude"] = drop_stats.get("seniority_exclude", 0) + 1
            continue
        # If include_any provided, do not require match; just use to boost

        # Experience traps / filter
        if match_any(experience_exclude, text):
            dropped.append(
                {
                    "company": job.company,
                    "job_title": job.job_title,
                    "location": job.location,
                    "remote_or_hybrid": job.remote_or_hybrid,
                    "posting_date": job.posting_date,
                    "source": job.source,
                    "job_url": job.job_url,
                    "score": 0,
                    "list_source": job.list_source,
                    "stage1_drop_reason": "experience_exclude",
                }
            )
            drop_stats["experience_exclude"] = drop_stats.get("experience_exclude", 0) + 1
            continue
        if experience_block and match_any(experience_block, text):
            dropped.append(
                {
                    "company": job.company,
                    "job_title": job.job_title,
                    "location": job.location,
                    "remote_or_hybrid": job.remote_or_hybrid,
                    "posting_date": job.posting_date,
                    "source": job.source,
                    "job_url": job.job_url,
                    "score": 0,
                    "list_source": job.list_source,
                    "stage1_drop_reason": "experience_exclude",
                }
            )
            drop_stats["experience_exclude"] = drop_stats.get("experience_exclude", 0) + 1
            continue

        # Global exclusions
        if match_any(global_exclude, text):
            dropped.append(
                {
                    "company": job.company,
                    "job_title": job.job_title,
                    "location": job.location,
                    "remote_or_hybrid": job.remote_or_hybrid,
                    "posting_date": job.posting_date,
                    "source": job.source,
                    "job_url": job.job_url,
                    "score": 0,
                    "list_source": job.list_source,
                    "stage1_drop_reason": "global_exclude",
                }
            )
            drop_stats["global_exclude"] = drop_stats.get("global_exclude", 0) + 1
            continue
        if match_any(employment_exclude, text):
            dropped.append(
                {
                    "company": job.company,
                    "job_title": job.job_title,
                    "location": job.location,
                    "remote_or_hybrid": job.remote_or_hybrid,
                    "posting_date": job.posting_date,
                    "source": job.source,
                    "job_url": job.job_url,
                    "score": 0,
                    "list_source": job.list_source,
                    "stage1_drop_reason": "employment_exclude",
                }
            )
            drop_stats["employment_exclude"] = drop_stats.get("employment_exclude", 0) + 1
            continue

        # Stage 1 bioinfo-likely pass condition
        if title_match:
            stage1_pass_reasons.append("title_match")
        weak_in_title = match_any(weak_domain_signals, title)
        weak_in_desc = match_any(weak_domain_signals, description) and match_any(bio_context_terms, description)
        if weak_in_title or weak_in_desc:
            stage1_pass_reasons.append("weak_domain_signal")
        if not stage1_pass_reasons:
            dropped.append(
                {
                    "company": job.company,
                    "job_title": job.job_title,
                    "location": job.location,
                    "remote_or_hybrid": job.remote_or_hybrid,
                    "posting_date": job.posting_date,
                    "source": job.source,
                    "job_url": job.job_url,
                    "score": 0,
                    "list_source": job.list_source,
                    "stage1_drop_reason": "no_bioinfo_signal",
                }
            )
            drop_stats["no_bioinfo_signal"] = drop_stats.get("no_bioinfo_signal", 0) + 1
            continue

        # Temporal filter (only if posting date is present)
        posting_age = age_days(job.posting_date)
        temporal = filter_cfg.get("temporal_filter", {})
        hard_exclude_days = temporal.get("hard_exclude_older_than_days")
        max_days = temporal.get("max_posting_age_days")
        if posting_age is not None:
            if hard_exclude_days is not None and posting_age > hard_exclude_days:
                dropped.append(
                    {
                        "company": job.company,
                        "job_title": job.job_title,
                        "location": job.location,
                        "remote_or_hybrid": job.remote_or_hybrid,
                        "posting_date": job.posting_date,
                        "source": job.source,
                        "job_url": job.job_url,
                        "score": 0,
                        "list_source": job.list_source,
                        "stage1_drop_reason": "too_old_hard",
                    }
                )
                drop_stats["too_old_hard"] = drop_stats.get("too_old_hard", 0) + 1
                continue
            if max_days is not None and posting_age > max_days:
                dropped.append(
                    {
                        "company": job.company,
                        "job_title": job.job_title,
                        "location": job.location,
                        "remote_or_hybrid": job.remote_or_hybrid,
                        "posting_date": job.posting_date,
                        "source": job.source,
                        "job_url": job.job_url,
                        "score": 0,
                        "list_source": job.list_source,
                        "stage1_drop_reason": "too_old",
                    }
                )
                drop_stats["too_old"] = drop_stats.get("too_old", 0) + 1
                continue

        # Scoring
        strong_hits = count_matches(scoring_strong, text)
        medium_hits = count_matches(scoring_medium, text)
        nice_hits = count_matches(scoring_nice, text)
        weights = scoring.get("weights", {})
        score = (
            strong_hits * weights.get("strong", 0)
            + medium_hits * weights.get("medium", 0)
            + nice_hits * weights.get("nice_to_have", 0)
        )
        if title_match:
            score += weights.get("strong", 0)
        if location_match:
            score += 1

        # Negative penalties
        neg = scoring.get("negative_keywords", {})
        if match_any(scoring_neg_high, text):
            score += neg.get("penalty_weights", {}).get("high_penalty", 0)
        if match_any(scoring_neg_medium, text):
            score += neg.get("penalty_weights", {}).get("medium_penalty", 0)

        # Freshness bonus
        bonus = 0
        if posting_age is not None:
            fresh = filter_cfg.get("priority_logic", {}).get("fresh_posting_bonus", {})
            if posting_age <= 3:
                bonus = fresh.get("days_0_to_3", 0)
            elif posting_age <= 7:
                bonus = fresh.get("days_4_to_7", 0)
            else:
                bonus = fresh.get("older_than_7", 0)
        if title_strict_match:
            bonus += filter_cfg.get("priority_logic", {}).get("title_strict_match_bonus", 0)
        score += bonus

        results.append(
            {
                "company": job.company,
                "job_title": job.job_title,
                "location": job.location,
                "remote_or_hybrid": job.remote_or_hybrid,
                "posting_date": job.posting_date,
                "source": job.source,
                "job_url": job.job_url,
                "score": score,
                "list_source": job.list_source,
                "stage1_pass_reasons": stage1_pass_reasons,
                "score_breakdown": {
                    "title_bonus": weights.get("strong", 0) if title_match else 0,
                    "freshness_bonus": bonus,
                    "strong_hits": strong_hits,
                    "medium_hits": medium_hits,
                    "nice_hits": nice_hits,
                    "penalties": (
                        (neg.get("penalty_weights", {}).get("high_penalty", 0) if match_any(scoring_neg_high, text) else 0)
                        + (neg.get("penalty_weights", {}).get("medium_penalty", 0) if match_any(scoring_neg_medium, text) else 0)
                    ),
                },
            }
        )
    return results, dropped, drop_stats


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "company",
        "job_title",
        "location",
        "remote_or_hybrid",
        "posting_date",
        "source",
        "job_url",
        "score",
        "list_source",
    ]
    optional_headers = [
        "stage1_pass_reasons",
        "stage1_drop_reason",
        "score_breakdown",
    ]
    if rows:
        present = {key for row in rows for key in row.keys()}
        for key in optional_headers:
            if key in present:
                headers.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            if not row.get("location"):
                row["location"] = "NA"
            if not row.get("posting_date"):
                row["posting_date"] = "NA"
            for key in optional_headers:
                if key in row and isinstance(row[key], (list, dict)):
                    row[key] = json.dumps(row[key], ensure_ascii=True)
            writer.writerow(row)


def merge_history(history_path: Path, latest_rows: list[dict]) -> list[dict]:
    if not history_path.exists():
        return latest_rows

    with history_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        existing = list(reader)

    seen = {(row.get("job_url") or "", row.get("company") or "") for row in existing}
    combined = existing[:]
    for row in latest_rows:
        key = (row.get("job_url") or "", row.get("company") or "")
        if key in seen:
            continue
        seen.add(key)
        combined.append(row)
    return combined


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull and filter jobs from ATS sources.")
    parser.add_argument("--targeted", nargs="+", default=["data/targeted_list.json"])
    parser.add_argument("--filter", default="data/jobs_filter.json")
    parser.add_argument("--unfiltered-output", default="data/jobs_unfiltered.jsonl")
    parser.add_argument("--filtered-output", default="data/jobs_filtered.jsonl")
    parser.add_argument("--latest-csv", default="data/jobs_latest.csv")
    parser.add_argument("--history-csv", default="data/jobs_history.csv")
    parser.add_argument("--batch-interval-seconds", type=int, default=120)
    parser.add_argument("--skip-network-check", action="store_true")
    return parser.parse_args()


def network_preflight() -> bool:
    try:
        socket.getaddrinfo("boards-api.greenhouse.io", 443)
        return True
    except OSError:
        return False


def _run_sanity_checks() -> None:
    assert match_token("US", normalize("BUSINESS")) is False
    assert match_token("US", normalize("REMOTE US")) is True
    assert match_token("GENOM", normalize("GENOMICS")) is True
    assert match_token("COMPUTATIONAL BIOLOGY", normalize("Scientist Computational Biology")) is True


def main() -> int:
    _run_sanity_checks()
    args = parse_args()
    if not args.skip_network_check and not network_preflight():
        print("Network/DNS unavailable: cannot resolve boards-api.greenhouse.io")
        return 2

    target_paths = [Path(p) for p in args.targeted]
    targets = load_targets(target_paths)

    filter_cfg = load_json(Path(args.filter))

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    all_jobs: list[JobRecord] = []
    unfiltered_path = Path(args.unfiltered_output)
    filtered_path = Path(args.filtered_output)
    latest_csv_path = Path(args.latest_csv)
    history_csv_path = Path(args.history_csv)
    unfiltered_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_path.parent.mkdir(parents=True, exist_ok=True)
    latest_csv_path.parent.mkdir(parents=True, exist_ok=True)
    history_csv_path.parent.mkdir(parents=True, exist_ok=True)

    last_batch = time.monotonic()
    batch_interval = max(0, args.batch_interval_seconds)
    for row, list_source in targets:
        if row.get("company_name"):
            all_jobs.extend(pull_jobs_for_target(row, session, list_source))
        if batch_interval and (time.monotonic() - last_batch) >= batch_interval:
            with unfiltered_path.open("w", encoding="utf-8") as handle:
                for job in all_jobs:
                    handle.write(json.dumps(job.__dict__, ensure_ascii=True) + "\n")
            filtered_rows, dropped_rows, drop_stats = filter_jobs(all_jobs, filter_cfg)
            with filtered_path.open("w", encoding="utf-8") as handle:
                for row in filtered_rows:
                    handle.write(json.dumps(row, ensure_ascii=True) + "\n")
            write_csv(latest_csv_path, filtered_rows)
            history_rows = merge_history(history_csv_path, filtered_rows)
            write_csv(history_csv_path, history_rows)
            print(f"Batch write: {len(all_jobs)} jobs total; {len(filtered_rows)} filtered")
            if drop_stats:
                top_reasons = sorted(drop_stats.items(), key=lambda item: item[1], reverse=True)[:5]
                print("Top drop reasons:", ", ".join(f"{reason}={count}" for reason, count in top_reasons))
            last_batch = time.monotonic()

    # Write unfiltered JSONL
    with unfiltered_path.open("w", encoding="utf-8") as handle:
        for job in all_jobs:
            handle.write(json.dumps(job.__dict__, ensure_ascii=True) + "\n")

    filtered_rows, dropped_rows, drop_stats = filter_jobs(all_jobs, filter_cfg)

    # Write filtered JSONL
    with filtered_path.open("w", encoding="utf-8") as handle:
        for row in filtered_rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")

    write_csv(latest_csv_path, filtered_rows)

    history_rows = merge_history(history_csv_path, filtered_rows)
    write_csv(history_csv_path, history_rows)

    print(f"Pulled {len(all_jobs)} jobs; filtered to {len(filtered_rows)}")
    if drop_stats:
        top_reasons = sorted(drop_stats.items(), key=lambda item: item[1], reverse=True)[:5]
        print("Top drop reasons:", ", ".join(f"{reason}={count}" for reason, count in top_reasons))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
