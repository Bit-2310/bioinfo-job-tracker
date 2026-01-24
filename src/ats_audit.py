"""One-time ATS/API audit.

Goal
  Scan ALL companies from Bioinformatics_Job_Target_List.xlsx once.
  For each company, detect ATS from Careers Page URL and hit the best
  public API endpoint we can access.

Output
  Writes: data/ats_audit_baseline.json

This file is meant to be a baseline snapshot you can use to:
  - see what works vs what fails (by company)
  - cluster failure reasons
  - build/improve a persistent ATS registry later

Notes
  - We do NOT try to scrape HTML pages here.
  - We keep requests light: a single API call per company (plus small pagination
    checks where needed).
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests

from ingest.workday import parse_workday_careers_url


INPUT_XLSX = Path("Bioinformatics_Job_Target_List.xlsx")
OUT_JSON = Path("data/ats_audit_baseline.json")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _token_after(path: str, marker: str) -> Optional[str]:
    if marker not in path:
        return None
    tail = path.split(marker, 1)[1].lstrip("/")
    token = tail.split("/", 1)[0].strip()
    return token or None


def detect_ats_from_url(careers_url: str) -> Optional[Tuple[str, Dict[str, str]]]:
    """Detect ATS type + extracted identifiers from a careers URL.

    Returns:
      ("workday", {"host":..., "tenant":..., "site":..., "locale":...})
      ("greenhouse", {"token":...})
      ("lever", {"token":...})
      ("ashby", {"token":...})
      ("icims", {"host":...})
      None
    """
    u = (careers_url or "").strip()
    if not u:
        return None

    parsed = urlparse(u)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    # Workday
    if "myworkdayjobs.com" in host:
        try:
            site = parse_workday_careers_url(u)
            return (
                "workday",
                {
                    "host": site.host,
                    "tenant": site.tenant,
                    "site": site.site,
                    "locale": site.locale,
                },
            )
        except Exception:
            # If it's Workday but parsing failed, still classify it.
            return ("workday", {"host": host})

    # Greenhouse
    if "greenhouse.io" in host or "greenhouse" in host:
        # examples:
        # - https://boards.greenhouse.io/<token>
        # - https://job-boards.greenhouse.io/<token>
        # - https://<anything>/boards/<token>
        m = re.match(r"^/([^/]+)", path)
        if (host.startswith("boards.greenhouse.io") or host.startswith("job-boards.greenhouse.io")) and m:
            return ("greenhouse", {"token": m.group(1)})
        token = _token_after(path, "/boards/")
        if token:
            return ("greenhouse", {"token": token})

    # Lever
    if "lever.co" in host:
        # https://jobs.lever.co/<token>
        m = re.match(r"^/([^/]+)", path)
        if host.startswith("jobs.lever.co") and m:
            return ("lever", {"token": m.group(1)})

    # Ashby
    if "ashbyhq.com" in host:
        # https://jobs.ashbyhq.com/<token>
        m = re.match(r"^/([^/]+)", path)
        if host.startswith("jobs.ashbyhq.com") and m:
            return ("ashby", {"token": m.group(1)})

    # iCIMS
    if "icims.com" in host and host.endswith(".icims.com"):
        return ("icims", {"host": host})

    return None


def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout_s: int = 25) -> Tuple[int, Any]:
    headers = {
        "User-Agent": "bioinfo-job-tracker/1.0 (+ats-audit)",
        "Accept": "application/json, text/plain, */*",
    }
    r = requests.get(url, params=params or {}, headers=headers, timeout=timeout_s)
    status = int(r.status_code)
    try:
        return status, r.json()
    except Exception:
        return status, None


def audit_one(company: str, careers_url: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "careers_url": careers_url,
        "detected_ats": "unknown",
        "attempts": [],
        "final_status": "unknown",
    }

    det = detect_ats_from_url(careers_url)
    if not det:
        result["final_status"] = "no_careers_url_or_unknown"
        return result

    ats_type, meta = det
    result["detected_ats"] = ats_type

    try:
        if ats_type == "greenhouse":
            token = meta.get("token", "")
            api_url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
            status, data = _http_get_json(api_url)
            jobs = (data or {}).get("jobs", []) if isinstance(data, dict) else []
            ok = status == 200 and isinstance(jobs, list)
            result["attempts"].append(
                {
                    "ats": "greenhouse",
                    "meta": meta,
                    "api_url": api_url,
                    "http_status": status,
                    "ok": ok,
                    "jobs_count": len(jobs) if isinstance(jobs, list) else None,
                    "error": None,
                }
            )
            result["final_status"] = "ok" if ok else "error"
            return result

        if ats_type == "lever":
            token = meta.get("token", "")
            api_url = f"https://api.lever.co/v0/postings/{token}"
            status, data = _http_get_json(api_url, params={"mode": "json"})
            ok = status == 200 and isinstance(data, list)
            result["attempts"].append(
                {
                    "ats": "lever",
                    "meta": meta,
                    "api_url": api_url,
                    "http_status": status,
                    "ok": ok,
                    "jobs_count": len(data) if isinstance(data, list) else None,
                    "error": None,
                }
            )
            result["final_status"] = "ok" if ok else "error"
            return result

        if ats_type == "ashby":
            token = meta.get("token", "")
            api_url = f"https://api.ashbyhq.com/posting-api/job-board/{token}"
            status, data = _http_get_json(api_url)
            jobs = (data or {}).get("jobs", []) if isinstance(data, dict) else []
            ok = status == 200 and isinstance(jobs, list)
            result["attempts"].append(
                {
                    "ats": "ashby",
                    "meta": meta,
                    "api_url": api_url,
                    "http_status": status,
                    "ok": ok,
                    "jobs_count": len(jobs) if isinstance(jobs, list) else None,
                    "error": None,
                }
            )
            result["final_status"] = "ok" if ok else "error"
            return result

        if ats_type == "workday":
            # Keep it light: request only first page
            host = meta.get("host", "")
            tenant = meta.get("tenant", "")
            site = meta.get("site", "")

            if not (host and tenant and site):
                # parsing failed
                result["attempts"].append(
                    {
                        "ats": "workday",
                        "meta": meta,
                        "api_url": None,
                        "http_status": None,
                        "ok": False,
                        "jobs_count": None,
                        "error": "workday_parse_failed",
                    }
                )
                result["final_status"] = "parse_failed"
                return result

            api_url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
            status, data = _http_get_json(api_url, params={"offset": 0, "limit": 1})
            postings = (data or {}).get("jobPostings", []) if isinstance(data, dict) else []
            ok = status == 200 and isinstance(postings, list)
            result["attempts"].append(
                {
                    "ats": "workday",
                    "meta": meta,
                    "api_url": api_url,
                    "http_status": status,
                    "ok": ok,
                    "jobs_count": len(postings) if isinstance(postings, list) else None,
                    "error": None,
                }
            )
            result["final_status"] = "ok" if ok else "error"
            return result

        if ats_type == "icims":
            host = meta.get("host", "")
            api_url = f"https://{host}/jobs/search"
            # iCIMS is inconsistent; treat 200 as "reachable" and try JSON if possible.
            status, data = _http_get_json(api_url)
            ok = status == 200
            jobs_count = None
            if isinstance(data, dict) and isinstance(data.get("jobs"), list):
                jobs_count = len(data.get("jobs"))
            result["attempts"].append(
                {
                    "ats": "icims",
                    "meta": meta,
                    "api_url": api_url,
                    "http_status": status,
                    "ok": ok,
                    "jobs_count": jobs_count,
                    "error": None if ok else "icims_unreachable",
                }
            )
            result["final_status"] = "ok" if ok else "error"
            return result

        result["final_status"] = "unknown"
        return result

    except Exception as e:
        result["attempts"].append(
            {
                "ats": ats_type,
                "meta": meta,
                "api_url": None,
                "http_status": None,
                "ok": False,
                "jobs_count": None,
                "error": repr(e),
            }
        )
        result["final_status"] = "exception"
        return result


def main() -> None:
    if not INPUT_XLSX.exists():
        raise FileNotFoundError(
            f"Missing {INPUT_XLSX}. Place the Excel file in the repo root."
        )

    df = pd.read_excel(INPUT_XLSX)
    required = {"Company Name", "Careers Page URL"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"{INPUT_XLSX} must contain columns: {sorted(required)}")

    tmp = df[["Company Name", "Careers Page URL"]].dropna()
    company_to_url: Dict[str, str] = {}
    for company, url in tmp.itertuples(index=False):
        c = str(company).strip()
        u = str(url).strip()
        if c and u and c not in company_to_url:
            company_to_url[c] = u

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    out: Dict[str, Any] = {
        "metadata": {
            "run_id": _utc_now_iso(),
            "companies_scanned": len(company_to_url),
            "input_file": str(INPUT_XLSX),
            "notes": "Baseline ATS/API audit (one-time run)",
        },
        "results": {},
    }

    for company, url in sorted(company_to_url.items()):
        out["results"][company] = audit_one(company=company, careers_url=url)

    OUT_JSON.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote {OUT_JSON} (companies={len(company_to_url)})")


if __name__ == "__main__":
    main()
