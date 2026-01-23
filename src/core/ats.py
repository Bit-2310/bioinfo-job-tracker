from __future__ import annotations

import re
from urllib.parse import urlparse


def _token_after(path: str, marker: str) -> str | None:
    if marker not in path:
        return None
    tail = path.split(marker, 1)[1].lstrip("/")
    token = tail.split("/", 1)[0].strip()
    return token or None


def detect_ats(careers_url: str) -> tuple[str, str] | None:
    """Infer ATS type and board token/host from a careers URL.

    Returns:
      ("greenhouse", board_token)
      ("lever", board_token)
      ("ashby", board_token)
      ("icims", host)
    """

    u = (careers_url or "").strip()
    if not u:
        return None

    parsed = urlparse(u)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    # Greenhouse
    if "greenhouse.io" in host or "greenhouse" in host:
        # examples:
        # - https://boards.greenhouse.io/<token>
        # - https://boards.greenhouse.io/<token>/jobs/...
        m = re.match(r"^/([^/]+)", path)
        if host.startswith("boards.greenhouse.io") and m:
            return ("greenhouse", m.group(1))

        token = _token_after(path, "/boards/")
        if token:
            return ("greenhouse", token)

    # Lever
    if "lever.co" in host:
        # https://jobs.lever.co/<token>
        m = re.match(r"^/([^/]+)", path)
        if host.startswith("jobs.lever.co") and m:
            return ("lever", m.group(1))

    # Ashby
    if "ashbyhq.com" in host:
        # https://jobs.ashbyhq.com/<token>
        m = re.match(r"^/([^/]+)", path)
        if host.startswith("jobs.ashbyhq.com") and m:
            return ("ashby", m.group(1))

    # iCIMS
    if "icims.com" in host:
        # Many iCIMS boards live at https://<subdomain>.icims.com/...
        if host.endswith(".icims.com"):
            return ("icims", host)

    return None
