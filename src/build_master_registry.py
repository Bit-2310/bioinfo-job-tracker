#!/usr/bin/env python3
"""
Build a master ATS registry from Bioinformatics_Job_Target_List.xlsx.

Goal:
- You provide company name + role titles + careers URL(s)
- We auto-detect ATS vendor + required slug/tenant/site
- We validate what we can (Greenhouse/Lever/Ashby/Workday)
- We write data/master_registry.json that the scraper can use

This is designed to be:
- Non-interactive (GitHub Actions safe)
- Never "fail fast" because one company is weird
- Deterministic output (stable ordering)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests


DEFAULT_INPUT = "Bioinformatics_Job_Target_List.xlsx"
DEFAULT_OUTPUT = "data/master_registry.json"

UA = "bioinfo-job-tracker/registry-builder (+https://github.com/Bit-2310/bioinfo-job-tracker)"


# -------------------------
# Regex footprints
# -------------------------
RE_GREENHOUSE = re.compile(r"boards\.greenhouse\.io/([A-Za-z0-9_-]+)", re.IGNORECASE)
RE_LEVER = re.compile(r"jobs\.lever\.co/([A-Za-z0-9_-]+)", re.IGNORECASE)
RE_ASHBY = re.compile(r"jobs\.ashbyhq\.com/([A-Za-z0-9_-]+)", re.IGNORECASE)

# Workday can appear as:
#  - https://<tenant>.wd5.myworkdayjobs.com/<site>
#  - https://<tenant>.wd5.myworkdayjobs.com/en-US/<site>
#  - https://jobs.myworkday.com/<tenant>/<site>  (less common)
RE_MYWORKDAY = re.compile(r"(?:https?://)?([A-Za-z0-9-]+\.(?:wd\d\.)?myworkdayjobs\.com)(/[^\s\"']+)?", re.IGNORECASE)
RE_JOBSMYWORKDAY = re.compile(r"(?:https?://)?jobs\.myworkday\.com/([A-Za-z0-9_-]+)/([A-Za-z0-9_-]+)", re.IGNORECASE)

RE_ICIMS = re.compile(r"icims\.com", re.IGNORECASE)


@dataclass
class AtsGreenhouse:
    has: int = 0
    validated: int = 0
    org: str = ""


@dataclass
class AtsLever:
    has: int = 0
    validated: int = 0
    org: str = ""


@dataclass
class AtsAshby:
    has: int = 0
    validated: int = 0
    org: str = ""


@dataclass
class AtsWorkday:
    has: int = 0
    validated: int = 0
    host: str = ""      # e.g. illumina.wd5.myworkdayjobs.com
    tenant: str = ""    # usually same as subdomain prefix
    site: str = ""      # first path segment or after locale


@dataclass
class AtsIcims:
    has: int = 0
    validated: int = 0
    host: str = ""


@dataclass
class CompanyRegistry:
    company_name: str
    roles: List[str]
    careers_urls: List[str]
    contact: str = ""
    ats: Dict[str, Any] = None


def _stable_unique(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        x = (x or "").strip()
        if not x:
            continue
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def load_targets_excel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input Excel: {path}")

    df = pd.read_excel(path, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    required = {"Company Name", "Target Role Title", "Careers Page URL"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input Excel missing columns: {sorted(missing)}")

    df = df[list(required)].copy()
    for c in required:
        df[c] = df[c].astype(str).str.strip()
    df = df[(df["Company Name"] != "") & (df["Company Name"].str.lower() != "nan")]
    df = df[(df["Careers Page URL"] != "") & (df["Careers Page URL"].str.lower() != "nan")]
    return df


def fetch_html(url: str, timeout: int = 15) -> str:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"},
            timeout=timeout,
            allow_redirects=True,
        )
        # Some pages block; still return text for footprint scan if present
        if resp.status_code >= 400:
            return resp.text or ""
        return resp.text or ""
    except Exception:
        return ""


def detect_from_html(html: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    gh = sorted(set(RE_GREENHOUSE.findall(html)))
    if gh:
        out["greenhouse_org"] = gh[0]

    lv = sorted(set(RE_LEVER.findall(html)))
    if lv:
        out["lever_org"] = lv[0]

    asb = sorted(set(RE_ASHBY.findall(html)))
    if asb:
        out["ashby_org"] = asb[0]

    # Workday: handle jobs.myworkday.com special case first
    jm = RE_JOBSMYWORKDAY.findall(html)
    if jm:
        tenant, site = jm[0]
        out["workday_host"] = "jobs.myworkday.com"
        out["workday_tenant"] = tenant
        out["workday_site"] = site
    else:
        mm = RE_MYWORKDAY.findall(html)
        if mm:
            host, path = mm[0]
            host = host.lower()
            site = ""
            tenant = host.split(".")[0] if host and host != "jobs" else ""
            if path:
                parts = [p for p in path.split("/") if p]
                if parts:
                    # drop locale segment if present
                    if parts[0].lower() in {"en-us", "en_us", "en"} and len(parts) >= 2:
                        site = parts[1]
                    else:
                        site = parts[0]
            if host:
                out["workday_host"] = host
            if tenant:
                out["workday_tenant"] = tenant
            if site:
                out["workday_site"] = site

    if RE_ICIMS.search(html):
        out["icims"] = True

    return out


# -------------------------
# Validators (best-effort)
# -------------------------
def validate_greenhouse(org: str, timeout: int = 15) -> bool:
    if not org:
        return False
    url = f"https://boards-api.greenhouse.io/v1/boards/{org}/jobs?content=true"
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=timeout)
        if r.status_code != 200:
            return False
        j = r.json()
        return isinstance(j, dict) and "jobs" in j
    except Exception:
        return False


def validate_lever(org: str, timeout: int = 15) -> bool:
    if not org:
        return False
    url = f"https://api.lever.co/v0/postings/{org}?mode=json"
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=timeout)
        if r.status_code != 200:
            return False
        j = r.json()
        return isinstance(j, list)
    except Exception:
        return False


def validate_ashby(org: str, timeout: int = 15) -> bool:
    """
    Ashby does not document a stable public API for job boards, but many boards expose a JSON endpoint.
    We try a common one. If it fails, we still keep `has=1` for routing and you can add a custom adapter later.
    """
    if not org:
        return False
    url = f"https://jobs.ashbyhq.com/api/nonusers/organization/{org}/jobs"
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=timeout)
        if r.status_code != 200:
            return False
        j = r.json()
        return isinstance(j, dict) and ("jobs" in j or "jobPosting" in j or "jobPostings" in j)
    except Exception:
        return False


def validate_workday(host: str, tenant: str, site: str, timeout: int = 15) -> bool:
    """
    Common public Workday "cxs" endpoint:
    https://{host}/wday/cxs/{tenant}/{site}/jobs
    """
    if not (host and tenant and site):
        return False

    # jobs.myworkday.com variant is much harder to validate without per-tenant pattern;
    # we mark as not validated (but keep tokens).
    if host.lower() == "jobs.myworkday.com":
        return False

    url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=timeout)
        if r.status_code != 200:
            return False
        j = r.json()
        return isinstance(j, dict) and ("jobPostings" in j or "total" in j or "items" in j)
    except Exception:
        return False


def build_registry(df: pd.DataFrame, max_seconds: int = 600) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns: (registry_list, audit_summary)
    """
    start = time.time()

    # group
    grouped: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        company = str(row["Company Name"]).strip()
        role = str(row["Target Role Title"]).strip()
        url = str(row["Careers Page URL"]).strip()
        if not company or not url:
            continue
        if company not in grouped:
            grouped[company] = {"roles": [], "urls": []}
        grouped[company]["roles"].append(role)
        grouped[company]["urls"].append(url)

    companies = sorted(grouped.keys(), key=lambda s: s.lower())

    registry: List[Dict[str, Any]] = []
    summary = {
        "companies_total": len(companies),
        "companies_scanned": 0,
        "hits": {"greenhouse": 0, "lever": 0, "ashby": 0, "workday": 0, "icims": 0},
        "validated": {"greenhouse": 0, "lever": 0, "ashby": 0, "workday": 0},
        "errors": 0,
        "duration_sec": 0.0,
    }

    for company in companies:
        if time.time() - start > max_seconds:
            break

        roles = _stable_unique(grouped[company]["roles"])
        urls = _stable_unique(grouped[company]["urls"])

        gh = AtsGreenhouse()
        lv = AtsLever()
        ab = AtsAshby()
        wd = AtsWorkday()
        ic = AtsIcims()

        detected_any = False
        first_html = ""
        for u in urls[:5]:  # cap per company
            html = fetch_html(u)
            if not first_html and html:
                first_html = html
            det = detect_from_html(html or "")
            if det.get("greenhouse_org") and not gh.org:
                gh.has = 1
                gh.org = det["greenhouse_org"]
                detected_any = True
            if det.get("lever_org") and not lv.org:
                lv.has = 1
                lv.org = det["lever_org"]
                detected_any = True
            if det.get("ashby_org") and not ab.org:
                ab.has = 1
                ab.org = det["ashby_org"]
                detected_any = True
            if det.get("workday_host") and not wd.host:
                wd.has = 1
                wd.host = det.get("workday_host", "")
                wd.tenant = det.get("workday_tenant", "")
                wd.site = det.get("workday_site", "")
                detected_any = True
            if det.get("icims") and not ic.has:
                ic.has = 1
                detected_any = True

            # early stop if we have something meaningful
            if gh.org or lv.org or ab.org or (wd.host and wd.site):
                break

        # If no ATS footprints in HTML, try a quick heuristic from URL itself
        if not detected_any:
            joined = " ".join(urls).lower()
            m = RE_GREENHOUSE.search(joined)
            if m:
                gh.has = 1
                gh.org = m.group(1)
            m = RE_LEVER.search(joined)
            if m:
                lv.has = 1
                lv.org = m.group(1)
            m = RE_ASHBY.search(joined)
            if m:
                ab.has = 1
                ab.org = m.group(1)

            # Workday from URL itself
            mm = RE_MYWORKDAY.findall(joined)
            if mm and not wd.host:
                host, path = mm[0]
                host = host.lower()
                tenant = host.split(".")[0]
                site = ""
                if path:
                    parts = [p for p in path.split("/") if p]
                    if parts:
                        if parts[0].lower() in {"en-us", "en_us", "en"} and len(parts) >= 2:
                            site = parts[1]
                        else:
                            site = parts[0]
                wd.has = 1
                wd.host = host
                wd.tenant = tenant
                wd.site = site

            if "icims" in joined:
                ic.has = 1

        # Validation (best effort)
        if gh.has and gh.org:
            if validate_greenhouse(gh.org):
                gh.validated = 1
            summary["hits"]["greenhouse"] += 1
            summary["validated"]["greenhouse"] += gh.validated

        if lv.has and lv.org:
            if validate_lever(lv.org):
                lv.validated = 1
            summary["hits"]["lever"] += 1
            summary["validated"]["lever"] += lv.validated

        if ab.has and ab.org:
            if validate_ashby(ab.org):
                ab.validated = 1
            summary["hits"]["ashby"] += 1
            summary["validated"]["ashby"] += ab.validated

        if wd.has and wd.host and wd.tenant and wd.site:
            if validate_workday(wd.host, wd.tenant, wd.site):
                wd.validated = 1
            summary["hits"]["workday"] += 1
            summary["validated"]["workday"] += wd.validated
        elif wd.has:
            summary["hits"]["workday"] += 1

        if ic.has:
            summary["hits"]["icims"] += 1

        reg = CompanyRegistry(
            company_name=company,
            roles=roles,
            careers_urls=urls,
            contact="",
            ats={
                "greenhouse": asdict(gh),
                "lever": asdict(lv),
                "ashby": asdict(ab),
                "workday": asdict(wd),
                "icims": asdict(ic),
            },
        )

        registry.append(asdict(reg))
        summary["companies_scanned"] += 1

    summary["duration_sec"] = round(time.time() - start, 2)
    return registry, summary


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=DEFAULT_INPUT, help="Excel file containing targets.")
    p.add_argument("--output", default=DEFAULT_OUTPUT, help="Output JSON registry path.")
    p.add_argument("--max-seconds", type=int, default=600, help="Time budget for the scan.")
    args = p.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    df = load_targets_excel(input_path)
    registry, summary = build_registry(df, max_seconds=args.max_seconds)

    payload = {
        "schema_version": 1,
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "input_file": str(input_path),
        "summary": summary,
        "companies": registry,
    }

    write_json(output_path, payload)
    print(f"[REGISTRY] input={input_path} companies_total={summary['companies_total']} scanned={summary['companies_scanned']} duration_sec={summary['duration_sec']}")
    print(f"[REGISTRY] hits={summary['hits']} validated={summary['validated']}")
    print(f"[REGISTRY] wrote {output_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[REGISTRY][FATAL] {repr(e)}", file=sys.stderr)
        sys.exit(2)
