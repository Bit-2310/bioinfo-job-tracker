from pathlib import Path

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


## Inputs
# Preferred (v2): Excel file with columns:
#   Company Name | Target Role Title | Careers Page URL
# Fallback (v1): targets/companies.csv with a single column: company
INPUT_XLSX = Path("Bioinformatics_Job_Target_List.xlsx")
INPUT_CSV = Path("targets/companies.csv")
DATA_DIR = Path("data")
HISTORY_PATH = DATA_DIR / "jobs_history.csv"
LATEST_PATH = DATA_DIR / "jobs_latest.csv"
RUNLOG_PATH = DATA_DIR / "runs.log"


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

    # v2: Excel-driven targets (company + careers url)
    company_to_careers_url = {}
    if INPUT_XLSX.exists():
        targets = pd.read_excel(INPUT_XLSX)
        required = {"Company Name", "Careers Page URL"}
        if not required.issubset(set(targets.columns)):
            raise ValueError(
                f"{INPUT_XLSX} must contain columns: {sorted(required)}"
            )
        tmp = targets[["Company Name", "Careers Page URL"]].dropna()
        for company, url in tmp.itertuples(index=False):
            c = str(company).strip()
            u = str(url).strip()
            if c and u and c not in company_to_careers_url:
                company_to_careers_url[c] = u
        companies = sorted(company_to_careers_url.keys())
    else:
        # v1: simple CSV
        if not INPUT_CSV.exists():
            raise FileNotFoundError(
                "Missing input file. Provide either:\n"
                "- Bioinformatics_Job_Target_List.xlsx\n"
                "- targets/companies.csv\n"
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
    history = load_history(HISTORY_PATH)

    all_jobs = []
    fetched_counts = {"greenhouse": 0, "lever": 0, "ashby": 0, "icims": 0, "workday": 0}
    error_counts = {"greenhouse": 0, "lever": 0, "ashby": 0, "icims": 0, "workday": 0}

    # Jobright removed (no API access).

    # ATS sources (verification + coverage)
    for company in companies:
        slug = slugify(company)

        careers_url = company_to_careers_url.get(company, "") if "company_to_careers_url" in locals() else ""
        if careers_url and "myworkdayjobs.com" in str(careers_url).lower():
            try:
                rows = fetch_workday(str(careers_url), company=company)
                fetched_counts["workday"] += len(rows)
                all_jobs += rows
            except Exception as e:
                error_counts["workday"] += 1
                log_line(RUNLOG_PATH, f"[ERROR] workday company={company} url={careers_url} err={repr(e)}")

        try:
            rows = fetch_greenhouse(slug)
            fetched_counts["greenhouse"] += len(rows)
            all_jobs += rows
        except Exception as e:
            error_counts["greenhouse"] += 1
            log_line(RUNLOG_PATH, f"[ERROR] greenhouse slug={slug} err={repr(e)}")

        try:
            rows = fetch_lever(slug)
            fetched_counts["lever"] += len(rows)
            all_jobs += rows
        except Exception as e:
            error_counts["lever"] += 1
            log_line(RUNLOG_PATH, f"[ERROR] lever slug={slug} err={repr(e)}")

        try:
            rows = fetch_ashby(slug)
            fetched_counts["ashby"] += len(rows)
            all_jobs += rows
        except Exception as e:
            error_counts["ashby"] += 1
            log_line(RUNLOG_PATH, f"[ERROR] ashby slug={slug} err={repr(e)}")

        try:
            rows = fetch_icims(slug)
            fetched_counts["icims"] += len(rows)
            all_jobs += rows
        except Exception as e:
            error_counts["icims"] += 1
            log_line(RUNLOG_PATH, f"[ERROR] icims slug={slug} err={repr(e)}")

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
        f"[OK] companies={len(companies)} fetched={sum(fetched_counts.values())} "
        f"new={len(new_rows)} dup={dup_count} skipped_bad={skipped_bad} "
        f"fetched_by_source={fetched_counts} errors={error_counts}"
    )


if __name__ == "__main__":
    main()
