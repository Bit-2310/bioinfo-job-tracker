from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests

from ingest.greenhouse import fetch_greenhouse
from ingest.lever import fetch_lever
from ingest.ashby import fetch_ashby
from ingest.workday import fetch_workday
from ingest.icims import fetch_icims


INPUT_XLSX = Path("Bioinformatics_Job_Target_List.xlsx")
OUTPUT_JSON = Path("data/ats_audit_baseline.json")

# Hard runtime cap for GitHub Actions (also enforced via workflow timeout)
MAX_RUNTIME_SECONDS = 600  # 10 minutes

# Network safety
HTTP_TIMEOUT = 10
UA = "Mozilla/5.0 (compatible; bioinfo-job-tracker/1.0; +https://github.com/Bit-2310/bioinfo-job-tracker)"


@dataclass
class Attempt:
    ats: str
    api_url: str
    http_status: int | None
    ok: bool
    jobs_count: int | None = None
    error: str | None = None


def _safe_get(url: str) -> requests.Response:
    return requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": UA}, allow_redirects=True)


def _extract_token_from_html(html: str) -> tuple[str, str] | None:
    """Try to discover ATS token from an arbitrary careers page HTML."""
    # Greenhouse patterns
    m = re.search(r"https?://boards\.greenhouse\.io/([A-Za-z0-9_\-]+)/?", html)
    if m:
        return ("greenhouse", m.group(1))
    m = re.search(r"https?://job-boards\.greenhouse\.io/([A-Za-z0-9_\-]+)/?", html)
    if m:
        return ("greenhouse", m.group(1))
    m = re.search(r"https?://boards-api\.greenhouse\.io/v1/boards/([A-Za-z0-9_\-]+)/jobs", html)
    if m:
        return ("greenhouse", m.group(1))

    # Lever
    m = re.search(r"https?://jobs\.lever\.co/([A-Za-z0-9_\-]+)/?", html)
    if m:
        return ("lever", m.group(1))

    # Ashby
    m = re.search(r"https?://jobs\.ashbyhq\.com/([A-Za-z0-9_\-]+)/?", html)
    if m:
        return ("ashby", m.group(1))

    # Workday (host is enough; tenant/site need URL)
    m = re.search(r"https?://([A-Za-z0-9\-\.]*myworkdayjobs\.com)/", html)
    if m:
        # We can't reliably infer tenant/site from HTML alone; caller will use careers_url.
        return ("workday", m.group(1))

    # iCIMS host
    m = re.search(r"https?://([A-Za-z0-9\-\.]+\.icims\.com)/", html)
    if m:
        return ("icims", m.group(1).lower())

    return None


def _detect_from_url(careers_url: str) -> tuple[str, str] | None:
    u = (careers_url or "").strip()
    if not u:
        return None
    p = urlparse(u)
    host = (p.netloc or "").lower()
    path = p.path or ""

    if "myworkdayjobs.com" in host:
        return ("workday", u)

    if host.startswith("boards.greenhouse.io") or host.startswith("job-boards.greenhouse.io"):
        token = path.strip("/").split("/", 1)[0]
        if token:
            return ("greenhouse", token)

    if host.startswith("jobs.lever.co"):
        token = path.strip("/").split("/", 1)[0]
        if token:
            return ("lever", token)

    if host.startswith("jobs.ashbyhq.com"):
        token = path.strip("/").split("/", 1)[0]
        if token:
            return ("ashby", token)

    if host.endswith(".icims.com"):
        return ("icims", host)

    return None


def _attempt_api(company: str, careers_url: str, detected: tuple[str, str] | None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "careers_url": careers_url,
        "detected_ats": detected[0] if detected else "unknown",
        "attempts": [],
        "final_status": "unknown",
    }

    def add_attempt(attempt: Attempt):
        out["attempts"].append(
            {
                "ats": attempt.ats,
                "api_url": attempt.api_url,
                "http_status": attempt.http_status,
                "ok": attempt.ok,
                "jobs_count": attempt.jobs_count,
                "error": attempt.error,
            }
        )

    if not detected:
        out["final_status"] = "unknown_ats"
        return out

    ats, token = detected

    try:
        if ats == "workday":
            rows = fetch_workday(str(token), company=company)
            add_attempt(Attempt("workday", str(token), 200 if rows is not None else None, True, jobs_count=len(rows)))
            out["final_status"] = "ok" if len(rows) > 0 else "ok_zero_jobs"
            return out

        if ats == "greenhouse":
            api_url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
            rows = fetch_greenhouse(str(token))
            add_attempt(Attempt("greenhouse", api_url, 200, True, jobs_count=len(rows)))
            out["final_status"] = "ok" if len(rows) > 0 else "ok_zero_jobs"
            return out

        if ats == "lever":
            api_url = f"https://api.lever.co/v0/postings/{token}"
            rows = fetch_lever(str(token))
            add_attempt(Attempt("lever", api_url, 200, True, jobs_count=len(rows)))
            out["final_status"] = "ok" if len(rows) > 0 else "ok_zero_jobs"
            return out

        if ats == "ashby":
            api_url = f"https://jobs.ashbyhq.com/api/non-auth/jobs?organizationSlug={token}"
            rows = fetch_ashby(str(token))
            add_attempt(Attempt("ashby", api_url, 200, True, jobs_count=len(rows)))
            out["final_status"] = "ok" if len(rows) > 0 else "ok_zero_jobs"
            return out

        if ats == "icims":
            # iCIMS is inconsistent; best-effort using current implementation
            api_url = f"https://{token}"
            rows = fetch_icims(str(token))
            add_attempt(Attempt("icims", api_url, 200 if rows is not None else None, True, jobs_count=len(rows)))
            out["final_status"] = "ok" if len(rows) > 0 else "ok_zero_jobs"
            return out

        out["final_status"] = "unknown_ats"
        return out

    except requests.RequestException as e:
        # try to capture status if available
        add_attempt(Attempt(ats, str(token), None, False, error=f"request_error:{type(e).__name__}:{e}"))
        out["final_status"] = "error"
        return out
    except Exception as e:
        add_attempt(Attempt(ats, str(token), None, False, error=f"error:{type(e).__name__}:{e}"))
        out["final_status"] = "error"
        return out


def main() -> None:
    start = time.time()

    if not INPUT_XLSX.exists():
        raise FileNotFoundError(
            f"Missing {INPUT_XLSX}. Commit it to the repo root (or adjust INPUT_XLSX)."
        )

    df = pd.read_excel(INPUT_XLSX)
    required = {"Company Name", "Careers Page URL"}
    if not required.issubset(df.columns):
        raise ValueError(f"{INPUT_XLSX} must contain columns: {sorted(required)}")

    # Unique companies for ATS audit (one careers URL per company)
    tmp = df[["Company Name", "Careers Page URL"]].dropna()
    company_urls = {}
    for c, u in tmp.itertuples(index=False):
        company = str(c).strip()
        url = str(u).strip()
        if company and url and company not in company_urls:
            company_urls[company] = url

    companies = list(company_urls.keys())
    print(f"[AUDIT] loaded_companies={len(companies)} input={INPUT_XLSX}")

    if len(companies) == 0:
        raise RuntimeError("Loaded 0 companies. Check that the Excel is committed and not empty.")

    results: dict[str, Any] = {}
    counts = {"ok": 0, "ok_zero_jobs": 0, "unknown_ats": 0, "error": 0}

    for i, company in enumerate(companies, start=1):
        if time.time() - start > MAX_RUNTIME_SECONDS:
            # Mark remaining as not run due to timeout
            remaining = companies[i-1:]
            for c in remaining:
                results[c] = {
                    "careers_url": company_urls[c],
                    "detected_ats": "not_run",
                    "attempts": [],
                    "final_status": "not_run_timeout",
                }
            break

        careers_url = company_urls[company]

        detected = _detect_from_url(careers_url)

        # Edge-case: try discover from HTML when URL isn't obviously an ATS board
        if detected is None:
            try:
                r = _safe_get(careers_url)
                html = r.text or ""
                discovered = _extract_token_from_html(html)
                if discovered:
                    # For workday, keep the original careers_url
                    if discovered[0] == "workday":
                        detected = ("workday", careers_url)
                    else:
                        detected = discovered
            except Exception:
                pass

        record = _attempt_api(company, careers_url, detected)
        results[company] = record
        st = record.get("final_status", "unknown_ats")
        if st in counts:
            counts[st] += 1
        elif st.startswith("ok"):
            counts["ok_zero_jobs"] += 1
        elif st == "unknown_ats":
            counts["unknown_ats"] += 1
        else:
            counts["error"] += 1

    payload = {
        "metadata": {
            "run_id_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "companies_scanned": len(companies),
            "max_runtime_seconds": MAX_RUNTIME_SECONDS,
            "http_timeout_seconds": HTTP_TIMEOUT,
            "summary": counts,
        },
        "results": results,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2))
    print(f"[AUDIT] wrote={OUTPUT_JSON} summary={counts}")


if __name__ == "__main__":
    main()
