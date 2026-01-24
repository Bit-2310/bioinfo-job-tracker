from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from core.identity import compute_canonical_job_id
from core.history import load_history, save_history
from core.dedupe import process_job
from core.output import write_latest
from core.runlog import log_line

from ingest.greenhouse import fetch_greenhouse
from ingest.lever import fetch_lever
from ingest.ashby import fetch_ashby
from ingest.icims import fetch_icims
from ingest.workday import fetch_workday


# -------------------------
# Inputs / outputs
# -------------------------
# Preferred input (v2): Bioinformatics_Job_Target_List.xlsx -> data/master_registry.json (auto-built in workflow)
REGISTRY_PATH = Path("data/master_registry.json")

# Legacy input (v1): fast, simple CSV (single column "company")
INPUT_CSV = Path("targets/companies.csv")

DATA_DIR = Path("data")
HISTORY_PATH = DATA_DIR / "jobs_history.csv"
LATEST_PATH = DATA_DIR / "jobs_latest.csv"
RUNLOG_PATH = DATA_DIR / "runs.log"


# -------------------------
# Filters (simple + pragmatic)
# -------------------------
US_STATE_ABBR = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"
}


def looks_us_location(loc: str) -> bool:
    if not loc:
        return True  # keep if unknown
    s = str(loc).strip().lower()
    if not s:
        return True
    if "united states" in s or "usa" in s or "u.s." in s:
        return True
    if "remote" in s and ("us" in s or "united states" in s):
        return True
    # state abbreviations (", CA" or " CA ")
    for ab in US_STATE_ABBR:
        if f", {ab.lower()}" in s or f" {ab.lower()} " in s or s.endswith(f" {ab.lower()}"):
            return True
    # common city+state format without comma: "boston ma"
    parts = s.split()
    if parts and parts[-1].upper() in US_STATE_ABBR:
        return True
    return False


def title_matches_roles(title: str, roles: List[str]) -> bool:
    if not roles:
        return True
    t = (title or "").strip().lower()
    if not t:
        return False
    for r in roles:
        r2 = (r or "").strip().lower()
        if not r2:
            continue
        if r2 in t:
            return True
    return False


# -------------------------
# Registry loading
# -------------------------
def load_registry(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        companies = payload.get("companies", [])
        if isinstance(companies, list):
            return companies
        return []
    except Exception:
        return []


def slugify(company: str) -> str:
    return (
        str(company).lower()
        .replace(" ", "")
        .replace(".", "")
        .replace(",", "")
        .replace("-", "")
        .replace("&", "and")
    )


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    registry = load_registry(REGISTRY_PATH)

    # Fallback mode (legacy)
    if not registry:
        if not INPUT_CSV.exists():
            raise FileNotFoundError(
                "Missing registry and legacy input.\n\n"
                "Expected either:\n"
                "  - data/master_registry.json (recommended)\n"
                "or:\n"
                "  - targets/companies.csv (legacy)\n"
            )

        targets = pd.read_csv(INPUT_CSV)
        if "company" not in targets.columns:
            raise ValueError("Input missing required column: 'company'")

        companies = (
            targets["company"]
            .dropna()
            .astype(str)
            .str.strip()
            .loc[lambda s: s != ""]
            .unique()
            .tolist()
        )
        companies = sorted(set(companies))
        registry = [
            {
                "company_name": c,
                "roles": [],
                "careers_urls": [],
                "contact": "",
                "ats": {
                    "greenhouse": {"has": 1, "validated": 0, "org": slugify(c)},
                    "lever": {"has": 1, "validated": 0, "org": slugify(c)},
                    "ashby": {"has": 1, "validated": 0, "org": slugify(c)},
                    "icims": {"has": 1, "validated": 0, "host": ""},
                    "workday": {"has": 0, "validated": 0, "host": "", "tenant": "", "site": ""},
                },
            }
            for c in companies
        ]
        log_line(RUNLOG_PATH, f"[WARN] registry missing; using legacy slugify mode companies={len(registry)}")

    history = load_history(HISTORY_PATH)

    all_jobs: List[Dict[str, Any]] = []
    fetched_counts = {"greenhouse": 0, "lever": 0, "ashby": 0, "icims": 0, "workday": 0}
    error_counts = {"greenhouse": 0, "lever": 0, "ashby": 0, "icims": 0, "workday": 0}

    for item in registry:
        company_name = str(item.get("company_name", "")).strip()
        roles = item.get("roles") or []
        ats = item.get("ats") or {}

        # Greenhouse
        gh = ats.get("greenhouse") or {}
        if gh.get("has") and gh.get("org"):
            org = str(gh.get("org"))
            try:
                rows = fetch_greenhouse(org)
                for r in rows:
                    r["company"] = company_name or r.get("company", "")
                rows = [r for r in rows if title_matches_roles(r.get("job_title", ""), roles) and looks_us_location(r.get("location", ""))]
                fetched_counts["greenhouse"] += len(rows)
                all_jobs += rows
            except Exception as e:
                error_counts["greenhouse"] += 1
                log_line(RUNLOG_PATH, f"[ERROR] greenhouse company={company_name} org={org} err={repr(e)}")

        # Lever
        lv = ats.get("lever") or {}
        if lv.get("has") and lv.get("org"):
            org = str(lv.get("org"))
            try:
                rows = fetch_lever(org)
                for r in rows:
                    r["company"] = company_name or r.get("company", "")
                rows = [r for r in rows if title_matches_roles(r.get("job_title", ""), roles) and looks_us_location(r.get("location", ""))]
                fetched_counts["lever"] += len(rows)
                all_jobs += rows
            except Exception as e:
                error_counts["lever"] += 1
                log_line(RUNLOG_PATH, f"[ERROR] lever company={company_name} org={org} err={repr(e)}")

        # Ashby
        ab = ats.get("ashby") or {}
        if ab.get("has") and ab.get("org"):
            org = str(ab.get("org"))
            try:
                rows = fetch_ashby(org)
                for r in rows:
                    r["company"] = company_name or r.get("company", "")
                rows = [r for r in rows if title_matches_roles(r.get("job_title", ""), roles) and looks_us_location(r.get("location", ""))]
                fetched_counts["ashby"] += len(rows)
                all_jobs += rows
            except Exception as e:
                error_counts["ashby"] += 1
                log_line(RUNLOG_PATH, f"[ERROR] ashby company={company_name} org={org} err={repr(e)}")

        # iCIMS (best-effort; usually needs per-company config)
        ic = ats.get("icims") or {}
        if ic.get("has") and (ic.get("host") or company_name):
            slug = str(ic.get("host") or slugify(company_name))
            try:
                rows = fetch_icims(slug)
                for r in rows:
                    r["company"] = company_name or r.get("company", "")
                rows = [r for r in rows if title_matches_roles(r.get("job_title", ""), roles) and looks_us_location(r.get("location", ""))]
                fetched_counts["icims"] += len(rows)
                all_jobs += rows
            except Exception as e:
                error_counts["icims"] += 1
                log_line(RUNLOG_PATH, f"[ERROR] icims company={company_name} key={slug} err={repr(e)}")

        # Workday
        wd = ats.get("workday") or {}
        if wd.get("has") and wd.get("host") and wd.get("tenant") and wd.get("site"):
            host = str(wd.get("host"))
            tenant = str(wd.get("tenant"))
            site = str(wd.get("site"))
            try:
                rows = fetch_workday(host, tenant, site)
                for r in rows:
                    r["company"] = company_name or r.get("company", "")
                rows = [r for r in rows if title_matches_roles(r.get("job_title", ""), roles) and looks_us_location(r.get("location", ""))]
                fetched_counts["workday"] += len(rows)
                all_jobs += rows
            except Exception as e:
                error_counts["workday"] += 1
                log_line(RUNLOG_PATH, f"[ERROR] workday company={company_name} host={host} tenant={tenant} site={site} err={repr(e)}")

    # Dedupe + history update
    new_rows = []
    dup_count = 0
    skipped_bad = 0

    for job in all_jobs:
        title = str(job.get("job_title", "")).strip()
        url = str(job.get("job_url", "")).strip()
        if not title or not url:
            skipped_bad += 1
            continue

        job["canonical_job_id"] = compute_canonical_job_id(
            job.get("company", ""),
            title,
            job.get("location", ""),
            url,
        )

        status, record = process_job(job, history)
        if status == "new":
            history = history._append(record, ignore_index=True)
            new_rows.append(record)
        else:
            dup_count += 1

    save_history(HISTORY_PATH, history)
    write_latest(LATEST_PATH, new_rows)

    log_line(
        RUNLOG_PATH,
        f"[OK] companies={len(registry)} fetched={sum(fetched_counts.values())} "
        f"new={len(new_rows)} dup={dup_count} skipped_bad={skipped_bad} "
        f"fetched_by_source={fetched_counts} errors={error_counts}"
    )


if __name__ == "__main__":
    main()
