"""Validate job sources for priority companies.

This script is intentionally *trackability-aware*.

Instead of only checking that the careers page returns HTTP 200, we validate
that the configured source can actually be fetched by our trackers.

Validated sources stay active (`is_active=1`). Sources that fail validation are
deactivated (`is_active=0`) so the tracker doesn't waste time.
"""

from __future__ import annotations

import re

import yaml

from src.utils.db import connect, ensure_tables
from src.utils.http import get


SETTINGS = yaml.safe_load(open("src/config/settings.yml"))
DB_PATH = SETTINGS["db_path"]
TIMEOUT = SETTINGS["http_timeout_sec"]
RETRIES = SETTINGS["http_retries"]

GREENHOUSE_BOARD_RE = re.compile(r"boards\.greenhouse\.io/([^/?#]+)", re.I)
LEVER_COMPANY_RE = re.compile(r"jobs\.lever\.co/([^/?#]+)", re.I)


def _ok(resp) -> bool:
    return bool(resp) and getattr(resp, "status_code", 0) < 400


def validate_greenhouse(careers_url: str) -> bool:
    m = GREENHOUSE_BOARD_RE.search(careers_url or "")
    if not m:
        return False
    board = m.group(1)
    api = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs"
    r = get(api, timeout=TIMEOUT, retries=RETRIES, pause=0.2)
    if not _ok(r):
        return False
    try:
        j = r.json()
    except Exception:
        return False
    return isinstance(j, dict) and "jobs" in j


def validate_lever(careers_url: str) -> bool:
    m = LEVER_COMPANY_RE.search(careers_url or "")
    if not m:
        return False
    company = m.group(1)
    api = f"https://api.lever.co/v0/postings/{company}?mode=json"
    r = get(api, timeout=TIMEOUT, retries=RETRIES, pause=0.2)
    if not _ok(r):
        return False
    try:
        j = r.json()
    except Exception:
        return False
    return isinstance(j, list)


def validate_custom(careers_url: str) -> bool:
    # Custom sources are not trackable yet (no parser).
    # We keep the validation strict so the tracker doesn't waste cycles.
    return False


def main(limit: int = 2000) -> None:
    with connect(DB_PATH) as con:
        ensure_tables(con)
        cur = con.cursor()

        # Only validate sources for curated priority companies (groups 1 and 2)
        cur.execute(
            """
            SELECT s.source_id, s.source_type, s.careers_url
            FROM company_job_sources s
            JOIN company_classification cc ON cc.company_id = s.company_id
            WHERE s.is_active=1
              AND cc.`group` IN (1, 2)
            ORDER BY s.source_id
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()

        ok = 0
        bad = 0

        for source_id, stype, url in rows:
            stype = (stype or "").lower().strip()
            if stype == "greenhouse":
                valid = validate_greenhouse(url)
            elif stype == "lever":
                valid = validate_lever(url)
            else:
                valid = validate_custom(url)

            if not valid:
                cur.execute(
                    "UPDATE company_job_sources SET is_active=0, last_checked_at=datetime('now') WHERE source_id=?",
                    (source_id,),
                )
                bad += 1
            else:
                cur.execute(
                    "UPDATE company_job_sources SET last_checked_at=datetime('now') WHERE source_id=?",
                    (source_id,),
                )
                ok += 1

        print(f"validate: checked={len(rows)} ok={ok} deactivated={bad}", flush=True)


if __name__ == "__main__":
    main()
