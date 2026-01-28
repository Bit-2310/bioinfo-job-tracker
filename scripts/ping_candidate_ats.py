#!/usr/bin/env python3
"""Build a candidate pool from archive datasets and ping ATS endpoints."""

from __future__ import annotations

import argparse
import csv
import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests

USER_AGENT = "bioinfo-job-tracker/1.1"

API_CODES = {
    "careers_url": 0,
    "greenhouse": 1,
    "lever": 2,
    "ashby": 3,
    "icims": 4,
    "workday": 5,
    "smartrecruiters": 6,
    "rippling": 7,
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
    "ats.rippling.com",
]

NOT_FOUND_MARKERS = [
    "not found",
    "page not found",
    "we can't find",
    "does not exist",
    "oops",
]


@dataclass
class Candidate:
    company_name: str
    careers_url: str | None = None


def normalize_name(value: str) -> str:
    value = value.lower().strip().replace("&", "and")
    value = re.sub(
        r"\b(the|inc|incorporated|corp|corporation|co|company|llc|ltd|plc|gmbh|ag|sa|sarl|bv|kg|lp|llp)\b",
        "",
        value,
    )
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value


def load_allowlist(path: Path) -> set[str]:
    if not path.exists():
        return set()
    values = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        values.append(line)
    return {normalize_name(v) for v in values}


def load_denylist(archive_zip: Path) -> set[str]:
    try:
        with zipfile.ZipFile(archive_zip, "r") as z:
            with z.open("data/archive/bioinformatics_denylist.txt") as f:
                lines = f.read().decode("utf-8", errors="replace").splitlines()
        return {normalize_name(line) for line in lines if line.strip() and not line.startswith("#")}
    except KeyError:
        return set()


def is_bioinfo_company(name: str, allow_norm: set[str], deny_norm: set[str]) -> bool:
    if not name or len(name) > 120:
        return False
    norm = normalize_name(name)
    if norm in deny_norm:
        return False
    if norm in allow_norm:
        return True
    non_bio_terms = [
        "hospital",
        "medical center",
        "health system",
        "clinic",
        "dental",
        "rehabilitation",
        "nursing",
        "homecare",
        "imaging",
        "pharmacy",
        "ambulatory",
        "surgery",
        "urgent care",
        "behavioral health",
        "regional health",
        "healthcare",
    ]
    lower = name.lower()
    if any(term in lower for term in non_bio_terms):
        return False
    keywords = [
        "bioinformatics",
        "bioinformatic",
        "computational",
        "genomics",
        "genomic",
        "genome",
        "sequencing",
        "sequence",
        "omics",
        "transcript",
        "proteomics",
        "metagenomics",
        "single cell",
        "ngs",
    ]
    pattern = re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE)
    return bool(pattern.search(name))


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def slugify_company(name: str) -> list[str]:
    raw = slugify(name)
    normalized = normalize_name(name)
    slugs = [raw, normalized]
    # remove common suffixes for slug attempts
    cleaned = re.sub(r"(inc|corp|corporation|holdings|group|class|plc|llc|ltd|sa)$", "", normalized)
    if cleaned:
        slugs.append(cleaned)
    # de-dupe while preserving order
    seen = set()
    uniq = []
    for s in slugs:
        if not s or s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    return uniq


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


def rippling_api_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    if not parsed.hostname or "ats.rippling.com" not in parsed.hostname:
        return None
    path = parsed.path.strip("/")
    if not path:
        return None
    slug = path.split("/")[0]
    return f"https://ats.rippling.com/{slug}/jobs"


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
    if "ats.rippling.com" in url_lower:
        return "rippling", rippling_api_from_url(url)
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
        if not isinstance(payload, dict):
            return False
        if "errors" in payload or "message" in payload:
            return False
        if not isinstance(payload.get("content"), list):
            return False
        total = payload.get("totalFound")
        if isinstance(total, int):
            return total > 0
        return len(payload.get("content", [])) > 0

    if api_name == "rippling":
        return True

    body = response.text.lower()
    if any(marker in body for marker in NOT_FOUND_MARKERS):
        return False
    return True


def load_archive_csvs(archive_zip: Path, filenames: Iterable[str]) -> list[dict]:
    rows: list[dict] = []
    with zipfile.ZipFile(archive_zip, "r") as z:
        for name in filenames:
            try:
                with z.open(f"data/archive/{name}") as f:
                    content = f.read().decode("utf-8", errors="replace").splitlines()
            except KeyError:
                continue
            reader = csv.DictReader(content)
            for row in reader:
                rows.append(row)
    return rows


def build_candidates(archive_zip: Path) -> list[Candidate]:
    candidates: dict[str, Candidate] = {}
    csv_sets = [
        "biotech_reference_companies.csv",
        "Biotech_Companies_Sponsorship.csv",
        "companies.csv",
        "companies_all.csv",
        "companies_top100.csv",
        "companies_top300.csv",
        "newlist.csv",
        "newlist_companies.csv",
    ]
    rows = load_archive_csvs(archive_zip, csv_sets)
    for row in rows:
        name = (row.get("company_name") or row.get("company") or "").strip()
        if not name:
            continue
        key = normalize_name(name)
        if not key:
            continue
        careers_url = (row.get("careers_url") or row.get("careers") or row.get("url") or "").strip()
        if key not in candidates:
            candidates[key] = Candidate(company_name=name, careers_url=careers_url or None)
        elif careers_url and not candidates[key].careers_url:
            candidates[key].careers_url = careers_url

    # also include sponsor candidates if present
    sponsor_candidates = Path("data/target_sponsor_candidates.json")
    if sponsor_candidates.exists():
        data = json.loads(sponsor_candidates.read_text(encoding="utf-8"))
        for row in data:
            name = (row or {}).get("company_name", "").strip()
            if not name:
                continue
            key = normalize_name(name)
            if key not in candidates:
                candidates[key] = Candidate(company_name=name)
    return list(candidates.values())


def load_existing_targets(path: Path) -> set[str]:
    if not path.exists():
        return set()
    data = json.loads(path.read_text(encoding="utf-8"))
    return {normalize_name(row.get("company_name", "")) for row in data if row.get("company_name")}


def candidate_urls(company: str, careers_url: str | None) -> list[tuple[str, str]]:
    urls: list[tuple[str, str]] = []
    if careers_url and any(domain in careers_url for domain in ATS_DOMAINS):
        api_name, api_url = detect_from_url(careers_url)
        if api_name and api_url:
            urls.append((api_name, api_url))

    for slug in slugify_company(company):
        urls.extend(
            [
                ("greenhouse", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"),
                ("lever", f"https://api.lever.co/v0/postings/{slug}"),
                ("ashby", f"https://api.ashbyhq.com/posting-api/job-board/{slug}"),
                ("rippling", f"https://ats.rippling.com/{slug}/jobs"),
            ]
        )
        if len(slug) <= 60:
            urls.append(("icims", f"https://{slug}.icims.com/jobs/search?ss=1"))
        urls.append(("smartrecruiters", f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/"))
    return urls


def check_candidate(candidate: Candidate, timeout: int) -> dict:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    urls = candidate_urls(candidate.company_name, candidate.careers_url)
    for api_name, api_url in urls:
        if request_ok(api_url, session, timeout, api_name):
            return {
                "company_name": candidate.company_name,
                "api_name": api_name,
                "api_url": api_url,
                "verified": True,
            }
    return {
        "company_name": candidate.company_name,
        "api_name": None,
        "api_url": None,
        "verified": False,
        "reason": "no_verified_ats",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ping ATS endpoints for archive-based candidates.")
    parser.add_argument("--archive-zip", default="data/archive.zip")
    parser.add_argument("--existing-targets", default="data/targeted_list_combined.json")
    parser.add_argument("--output", default="data/targeted_list_from_archives.json")
    parser.add_argument("--report", default="data/targeted_list_from_archives_report.json")
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--workers", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    archive_zip = Path(args.archive_zip)
    candidates = build_candidates(archive_zip)
    existing = load_existing_targets(Path(args.existing_targets))

    allow_norm = load_allowlist(Path("data/bioinformatics_allowlist.txt"))
    deny_norm = load_denylist(archive_zip)
    filtered = [
        c
        for c in candidates
        if normalize_name(c.company_name) not in existing
        and is_bioinfo_company(c.company_name, allow_norm, deny_norm)
    ]
    if not filtered:
        print("No new candidates after filtering existing targets.")
        return 0

    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(check_candidate, c, args.timeout): c for c in filtered}
        for idx, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            if idx % 50 == 0:
                print(f"Checked {idx}/{len(filtered)}")

    verified = [
        {
            "company_name": row["company_name"],
            "api_name": row["api_name"],
            "api_url": row["api_url"],
            "api_code": API_CODES.get(row["api_name"], 0),
        }
        for row in results
        if row.get("verified")
    ]
    Path(args.output).write_text(json.dumps(verified, indent=2), encoding="utf-8")
    Path(args.report).write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"Processed {len(filtered)} candidates, verified {len(verified)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
