from __future__ import annotations

import re
from urllib.parse import urlparse


def detect_ats(careers_url: str) -> str:
    """Best-effort ATS detection from a careers URL."""
    host = urlparse(str(careers_url)).netloc.lower()
    path = urlparse(str(careers_url)).path.lower()

    if "myworkdayjobs.com" in host:
        return "workday"
    if "greenhouse.io" in host:
        return "greenhouse"
    if "lever.co" in host:
        return "lever"
    if "ashbyhq.com" in host:
        return "ashby"
    if "icims.com" in host or host.endswith(".icims.com"):
        return "icims"

    # Some companies link to Greenhouse job boards via a wrapper path.
    if "greenhouse" in host or re.search(r"greenhouse", path):
        return "greenhouse"

    return "unknown"


def extract_org_slug(ats: str, careers_url: str) -> str | None:
    """Extract org slug for ATS job board URLs where applicable."""
    u = urlparse(str(careers_url))
    parts = [p for p in u.path.split("/") if p]

    if ats == "greenhouse":
        # boards.greenhouse.io/<org> or job-boards.greenhouse.io/<org>
        return parts[0] if parts else None

    if ats == "lever":
        # jobs.lever.co/<org>
        return parts[0] if parts else None

    if ats == "ashby":
        # jobs.ashbyhq.com/<org>
        return parts[0] if parts else None

    return None
