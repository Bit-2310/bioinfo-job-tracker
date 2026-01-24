from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

import requests


@dataclass(frozen=True)
class WorkdaySite:
    api_base: str  # e.g. https://illumina.wd1.myworkdayjobs.com
    tenant: str    # e.g. illumina
    site: str      # e.g. IlluminaCareers


def parse_workday_site(careers_url: str) -> WorkdaySite:
    """Parse a Workday careers URL and return tenant/site info.

    Expected patterns:
      https://<tenant>.<cluster>.myworkdayjobs.com/en-US/<site>

    We intentionally do NOT try to guess tenant/site from company name.
    """
    p = urlparse(careers_url)
    host = p.netloc
    if "myworkdayjobs.com" not in host:
        raise ValueError("Not a Workday (myworkdayjobs.com) URL")

    tenant = host.split(".")[0]
    parts = [x for x in p.path.split("/") if x]

    # Common: /en-US/<site>
    site = parts[-1] if parts else ""
    if not site or site.lower() in {"en-us", "en", "us"}:
        raise ValueError("Cannot extract Workday site from URL path")

    api_base = f"https://{host}"
    return WorkdaySite(api_base=api_base, tenant=tenant, site=site)


def fetch_workday(careers_url: str, *, timeout_s: int = 30, limit: int = 50) -> list[dict]:
    """Fetch jobs from Workday CXS endpoint.

    Returns a list of normalized job dicts.
    """
    site = parse_workday_site(careers_url)

    jobs: list[dict] = []
    offset = 0

    while True:
        api_url = (
            f"{site.api_base}/wday/cxs/{site.tenant}/{site.site}/jobs"
            f"?offset={offset}&limit={limit}"
        )
        r = requests.get(api_url, timeout=timeout_s)
        if r.status_code != 200:
            return []

        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        postings = data.get("jobPostings") or data.get("jobPostings", [])
        if not postings:
            break

        for j in postings:
            title = j.get("title", "") or ""
            job_url = j.get("externalPath") or j.get("externalUrl") or ""
            if job_url and job_url.startswith("/"):
                job_url = f"{site.api_base}{job_url}"

            # Location varies; try a few common fields
            loc = (
                (j.get("locationsText") or "")
                or (j.get("primaryLocation") or "")
                or (j.get("location") or "")
            )

            jobs.append(
                {
                    "company": "",  # set by caller
                    "job_title": title,
                    "location": loc,
                    "remote_or_hybrid": "",
                    "posting_date": j.get("postedOn") or j.get("postedDate") or "",
                    "job_url": job_url,
                    "source": "workday",
                    "source_id": j.get("bulletFields", {}).get("jobReqId")
                    if isinstance(j.get("bulletFields"), dict)
                    else "",
                    "_workday_api": api_url,
                }
            )

        total = data.get("total")
        offset += limit
        if total is not None and offset >= int(total):
            break

        # Safety: if Workday doesn't return total, stop when fewer than limit
        if len(postings) < limit:
            break

    return jobs
