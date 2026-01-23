# Bioinformatics Job Scraper v1 (Simple + Reliable)

This repo builds a minimal job ingestion pipeline with **hard deduplication** and an **append-only history**.

It pulls jobs from:
- Jobright (API) — discovery
- Greenhouse, Lever, Ashby, iCIMS — ATS coverage (best-effort)

It writes:
- `data/jobs_history.csv` (source of truth; stable + idempotent)
- `data/jobs_latest.csv` (only *new* jobs from the current run)
- `data/runs.log` (audit trail)

## Input (required)

Create this file:

`targets/companies.csv`

Required columns:
- `company`

Example:

```csv
company
Illumina
10x Genomics
Guardant Health
```

Why CSV: faster, less fragile than Excel, and GitHub-friendly.

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export JOBRIGHT_API_KEY="YOUR_KEY"
python src/run.py
```

If you do not set `JOBRIGHT_API_KEY`, the pipeline will still run (ATS-only) and will log a warning.

## GitHub Actions

Workflow: `.github/workflows/scrape.yml`

Add repo secret:
- `JOBRIGHT_API_KEY`

Run once manually:
- GitHub → Actions → `job-scraper` → Run workflow

Then it runs daily on schedule.

## Notes / expected behavior

- First run: `jobs_latest.csv` likely non-empty.
- Second run: `new` should be near 0; `dup` will be high (correct).
- If ATS slug doesn’t match, that ATS fetch returns 0 (logged only on errors).

## Output schema (history)

`data/jobs_history.csv` columns:
- canonical_job_id
- company
- job_title
- location
- remote_or_hybrid
- posting_date
- job_url
- first_seen
- last_seen
- sources_seen
