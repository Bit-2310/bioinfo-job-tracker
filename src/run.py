"""Entry point for the job tracker.

Key change from v0:
- Input is now an Excel sheet (Bioinformatics_Job_Target_List.xlsx)
  with: Company Name | Target Role Title | Careers Page URL
- We infer ATS + board token from the Careers Page URL (instead of guessing a slug).
- We filter to US locations (incl. Remote) and to titles that match target-role keywords.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

from core.identity import compute_canonical_job_id
from core.history import load_history, save_history
from core.dedupe import process_job
from core.output import write_latest
from core.runlog import log_line
from core.targets import load_targets
from core.filters import title_matches_targets, is_us_location
from core.ats import detect_ats

from ingest.greenhouse import fetch_greenhouse
from ingest.lever import fetch_lever
from ingest.ashby import fetch_ashby
from ingest.icims import fetch_icims


INPUT_XLSX = Path("Bioinformatics_Job_Target_List.xlsx")
DATA_DIR = Path("data")
HISTORY_PATH = DATA_DIR / "jobs_history.csv"
LATEST_PATH = DATA_DIR / "jobs_latest.csv"
RUNLOG_PATH = DATA_DIR / "runs.log"


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not INPUT_XLSX.exists():
        raise FileNotFoundError(
            f"Missing input file: {INPUT_XLSX}\n"
            "Place Bioinformatics_Job_Target_List.xlsx in the repo root."
        )

    targets_df = load_targets(INPUT_XLSX)
    history = load_history(HISTORY_PATH)

    all_jobs: list[dict] = []
    fetched_counts: dict[str, int] = {"greenhouse": 0, "lever": 0, "ashby": 0, "icims": 0}
    error_counts: dict[str, int] = {"greenhouse": 0, "lever": 0, "ashby": 0, "icims": 0}

    # Fetch
    for row in targets_df.itertuples(index=False):
        company = str(row.company).strip()
        target_role = str(row.target_role).strip()
        careers_url = str(row.careers_url).strip()

        ats = detect_ats(careers_url)
        if not ats:
            log_line(RUNLOG_PATH, f"[SKIP] company={company} reason=unknown_ats url={careers_url}")
            continue

        try:
            if ats[0] == "greenhouse":
                rows = fetch_greenhouse(board_token=ats[1], company_name=company, target_role=target_role)
                fetched_counts["greenhouse"] += len(rows)
                all_jobs += rows
            elif ats[0] == "lever":
                rows = fetch_lever(board_token=ats[1], company_name=company, target_role=target_role)
                fetched_counts["lever"] += len(rows)
                all_jobs += rows
            elif ats[0] == "ashby":
                rows = fetch_ashby(board_token=ats[1], company_name=company, target_role=target_role)
                fetched_counts["ashby"] += len(rows)
                all_jobs += rows
            elif ats[0] == "icims":
                rows = fetch_icims(host=ats[1], company_name=company, target_role=target_role)
                fetched_counts["icims"] += len(rows)
                all_jobs += rows
        except Exception as e:
            error_counts[ats[0]] += 1
            log_line(RUNLOG_PATH, f"[ERROR] ats={ats[0]} company={company} err={repr(e)}")

    # Filter + Dedupe + history update
    new_rows: list[dict] = []
    dup_count = 0
    skipped_bad = 0
    skipped_non_us = 0
    skipped_no_match = 0

    for job in all_jobs:
        title = str(job.get("job_title", "")).strip()
        url = str(job.get("job_url", "")).strip()
        loc = str(job.get("location", "")).strip()
        target_role = str(job.get("target_role", "")).strip()

        if not title or not url:
            skipped_bad += 1
            continue

        if not is_us_location(loc):
            skipped_non_us += 1
            continue

        if not title_matches_targets(title, target_role):
            skipped_no_match += 1
            continue

        job["canonical_job_id"] = compute_canonical_job_id(
            job.get("company", ""),
            title,
            loc,
            url,
        )
        job["date_scraped"] = datetime.now(timezone.utc).date().isoformat()

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
        f"[OK] targets={len(targets_df)} fetched={sum(fetched_counts.values())} "
        f"new={len(new_rows)} dup={dup_count} skipped_bad={skipped_bad} "
        f"skipped_non_us={skipped_non_us} skipped_no_match={skipped_no_match} "
        f"fetched_by_source={fetched_counts} errors={error_counts}",
    )


if __name__ == "__main__":
    main()
