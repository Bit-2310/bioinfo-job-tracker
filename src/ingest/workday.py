"""Workday ingestion (myworkdayjobs.com).

Uses the public Workday "CXS" JSON endpoints.

We query:
  https://{host}/wday/cxs/{tenant}/{site}/jobs?offset=0&limit=50

Notes:
- Many portals hide/omit posted date. Treat first_seen as ground truth.
- Best-effort; some tenants block CXS requests.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests


@dataclass(frozen=True)
class WorkdaySite:
    host: str
    tenant: str
    site: str
    locale: str = "en-US"


_HOST_RE = re.compile(r"^(?P<tenant>[a-z0-9-]+)(?:\.wd\d+)?\.myworkdayjobs\.com$", re.I)


def parse_workday_careers_url(careers_url: str) -> WorkdaySite:
    """Parse a Workday careers URL into host/tenant/site/locale.

    Expected URL forms:
      - https://TENANT.wdX.myworkdayjobs.com/en-US/SITE
      - https://TENANT.myworkdayjobs.com/SITE
      - https://TENANT.wdX.myworkdayjobs.com/SITE

    We treat the first non-locale path component as `site`.
    """
    u = urlparse(careers_url)
    host = (u.netloc or "").strip()
    if not host:
        raise ValueError(f"Invalid Workday URL (no host): {careers_url}")

    m = _HOST_RE.match(host)
    if not m:
        raise ValueError(f"Not a myworkdayjobs.com host: {host}")

    tenant = m.group("tenant")

    # path like /en-US/SITE or /SITE
    parts = [p for p in (u.path or "").split("/") if p]
    locale = "en-US"
    site = ""
    if parts:
        if re.fullmatch(r"[a-z]{2}-[A-Z]{2}", parts[0]):
            locale = parts[0]
            site = parts[1] if len(parts) > 1 else ""
        else:
            site = parts[0]

    if not site:
        raise ValueError(f"Workday URL missing site segment: {careers_url}")

    return WorkdaySite(host=host, tenant=tenant, site=site, locale=locale)


def _cxs_search_url(site: WorkdaySite) -> str:
    return f"https://{site.host}/wday/cxs/{site.tenant}/{site.site}/jobs"


def _get_json(url: str, params: Dict[str, Any], timeout_s: int = 25) -> Dict[str, Any]:
    """GET JSON with basic retries/backoff."""
    headers = {
        "User-Agent": "bioinfo-job-tracker/1.0 (+github-actions)",
        "Accept": "application/json, text/plain, */*",
    }

    last_err: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout_s)
            if resp.status_code in (429, 502, 503, 504):
                time.sleep(1.5 * attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(1.0 * attempt)

    raise RuntimeError(f"Workday request failed after retries: {url} params={params} err={repr(last_err)}")


def _normalize_posting(
    company: str,
    site: WorkdaySite,
    posting: Dict[str, Any],
) -> Dict[str, Any]:
    """Map a Workday posting to our internal row schema."""
    title = posting.get("title") or posting.get("postingTitle") or ""
    location = posting.get("locationsText") or posting.get("location") or ""

    # Workday often provides an externalPath like: /job/City-State-Country/.../JR-12345
    external_path = posting.get("externalPath") or ""
    job_url = ""
    if external_path:
        job_url = f"https://{site.host}/{site.locale}/{site.site}{external_path}"
    else:
        # fallback: sometimes there's a url field
        job_url = posting.get("url") or posting.get("jobUrl") or ""

    posted = (
        posting.get("postedOn")
        or posting.get("postedOnDate")
        or posting.get("startDate")
        or ""
    )

    # Keep raw fields if present
    req_id = posting.get("jobRequisitionId") or posting.get("requisitionId") or ""

    return {
        "company": company,
        "target_role": "",
        "job_title": str(title).strip(),
        "location": str(location).strip(),
        "remote_or_hybrid": "",
        "posting_date": str(posted).strip(),
        "job_url": str(job_url).strip(),
        "source": "workday",
        "source_id": str(req_id).strip(),
    }


def fetch_workday(careers_url: str, company: str, limit_per_page: int = 50, max_pages: int = 40) -> List[Dict[str, Any]]:
    """Fetch jobs for a Workday-hosted career site.

    Returns a list of normalized rows.
    """
    site = parse_workday_careers_url(careers_url)
    base = _cxs_search_url(site)

    rows: List[Dict[str, Any]] = []
    offset = 0

    for _ in range(max_pages):
        data = _get_json(base, params={"offset": offset, "limit": limit_per_page})

        postings = data.get("jobPostings") or data.get("items") or []
        if not isinstance(postings, list):
            break

        if not postings:
            break

        for p in postings:
            if isinstance(p, dict):
                rows.append(_normalize_posting(company=company, site=site, posting=p))

        # Pagination: some responses include total, but offset+count works fine.
        offset += len(postings)
        if len(postings) < limit_per_page:
            break

    return rows
