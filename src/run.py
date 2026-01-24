from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from core.ats_router import detect_ats, extract_org_slug
from core.identity import compute_canonical_job_id
from core.history import load_history, save_history
from core.dedupe import process_job
from core.output import write_latest
from core.runlog import log_line

from ingest.greenhouse import fetch_greenhouse
from ingest.lever import fetch_lever
from ingest.ashby import fetch_ashby
from ingest.workday import fetch_workday


# Canonical input file (repo root). Required columns:
# Company Name, Target Role Title, Careers Page URL
INPUT_XLSX = Path("Bioinformatics_Job_Target_List.xlsx")

DATA_DIR = Path("data")
HISTORY_PATH = DATA_DIR / "jobs_history.csv"
LATEST_PATH = DATA_DIR / "jobs_latest.csv"         # targeted (matches target roles)
RAW_LATEST_PATH = DATA_DIR / "jobs_raw_latest.csv"  # all fetched (for debugging)
RUNLOG_PATH = DATA_DIR / "runs.log"


def _norm(s: str) -> str:
    return str(s or "").strip()


def _to_list(x: object) -> list[str]:
    if x is None:
        return []
    if isinstance(x, (list, tuple, set)):
        return [str(i) for i in x]
    return [str(x)]


def load_targets(path: Path) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    """Load company rows and build a map: company -> target role keywords."""
    df = pd.read_excel(path)
    # Normalize column names (users sometimes edit the sheet)
    df.columns = [str(c).strip() for c in df.columns]

    required = {"Company Name", "Careers Page URL"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Input Excel missing required columns: {sorted(missing)}. "
            "Expected at least: Company Name, Careers Page URL"
        )

    df = df.dropna(subset=["Company Name", "Careers Page URL"]).copy()
    df["Company Name"] = df["Company Name"].astype(str).str.strip()
    df["Careers Page URL"] = df["Careers Page URL"].astype(str).str.strip()

    if "Target Role Title" not in df.columns:
        df["Target Role Title"] = ""
    else:
        df["Target Role Title"] = df["Target Role Title"].fillna("").astype(str).str.strip()

    # Build role keyword map (company -> list of target role titles)
    role_map: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        company = _norm(row["Company Name"])
        role = _norm(row.get("Target Role Title", ""))
        if not company:
            continue
        role_map.setdefault(company, [])
        if role and role.lower() not in {r.lower() for r in role_map[company]}:
            role_map[company].append(role)

    # Deduplicate companies by keeping the first URL per company.
    df = df.drop_duplicates(subset=["Company Name"], keep="first")
    return df, role_map


def match_target_roles(job_title: str, target_roles: Iterable[str]) -> bool:
    title = _norm(job_title).lower()
    if not title:
        return False
    roles = [r.strip().lower() for r in target_roles if str(r).strip()]
    if not roles:
        return True  # no targets defined => keep all (debug-friendly)
    return any(r in title for r in roles)


def fetch_company_jobs(company: str, careers_url: str) -> tuple[list[dict], str, str | None]:
    """Route a company to the correct ATS API and return normalized job dicts.

    Returns: (jobs, ats, org_slug)
    """
    ats = detect_ats(careers_url)
    org = extract_org_slug(ats, careers_url)

    if ats == "greenhouse":
        if not org:
            return [], ats, org
        rows = fetch_greenhouse(org)
    elif ats == "lever":
        if not org:
            return [], ats, org
        rows = fetch_lever(org)
    elif ats == "ashby":
        if not org:
            return [], ats, org
        rows = fetch_ashby(org)
    elif ats == "workday":
        rows = fetch_workday(careers_url)
    elif ats == "icims":
        # iCIMS does not offer a stable public JSON API; skipping is safer than throwing.
        return [], ats, org
    else:
        return [], ats, org

    # Ensure company name is preserved (not slug)
    for r in rows:
        r["company"] = company
    return rows, ats, org


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not INPUT_XLSX.exists():
        raise FileNotFoundError(
            "Missing input file: Bioinformatics_Job_Target_List.xlsx\n"
            "Place it in the repo root (same folder as README.md)."
        )

    targets_df, role_map = load_targets(INPUT_XLSX)
    companies = targets_df["Company Name"].tolist()

    history = load_history(HISTORY_PATH)

    fetched_counts = {"greenhouse": 0, "lever": 0, "ashby": 0, "workday": 0, "icims": 0, "unknown": 0}
    error_counts = {k: 0 for k in fetched_counts}
    all_jobs: list[dict] = []

    for _, row in targets_df.iterrows():
        company = _norm(row["Company Name"])
        careers_url = _norm(row["Careers Page URL"])

        if not company or not careers_url:
            continue

        try:
            rows, ats, org = fetch_company_jobs(company, careers_url)
            fetched_counts[ats] = fetched_counts.get(ats, 0) + len(rows)
            all_jobs += rows
        except Exception as e:
            ats = detect_ats(careers_url)
            error_counts[ats] = error_counts.get(ats, 0) + 1
            log_line(RUNLOG_PATH, f"[ERROR] ats={ats} company={company} url={careers_url} err={repr(e)}")

    # Write raw output (debug)
    raw_df = pd.DataFrame(all_jobs)
    if not raw_df.empty:
        raw_df.to_csv(RAW_LATEST_PATH, index=False)
    else:
        # Always write a file so the site doesn't break.
        pd.DataFrame(columns=["company", "job_title", "location", "posting_date", "job_url", "source"]).to_csv(
            RAW_LATEST_PATH, index=False
        )

    # Targeted filtering
    targeted_jobs = [
        j
        for j in all_jobs
        if match_target_roles(j.get("job_title", ""), role_map.get(j.get("company", ""), []))
    ]

    # Dedupe + history update (targeted)
    new_rows = []
    dup_count = 0
    skipped_bad = 0

    for job in targeted_jobs:
        title = _norm(job.get("job_title", ""))
        url = _norm(job.get("job_url", ""))
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
        f"[OK] companies={len(companies)} fetched={sum(fetched_counts.values())} "
        f"new={len(new_rows)} dup={dup_count} skipped_bad={skipped_bad} "
        f"fetched_by_source={fetched_counts} errors={error_counts}"
    )


if __name__ == "__main__":
    main()
