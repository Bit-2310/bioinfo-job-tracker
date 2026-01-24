# Bioinformatics Job Tracker (v1)

Minimal job ingestion pipeline that runs on **GitHub Actions**, keeps an **append-only history**, and writes a small "latest" CSV you can browse via `index.html`.

## Sources (v1)

- Greenhouse (API)
- Lever (API)
- Ashby (API)
- iCIMS (best-effort)

## Input (canonical)

Place **Bioinformatics_Job_Target_List.xlsx** in the repo root.

Required columns:
- `Company Name`
- `Careers Page URL`

(`Target Role Title` can exist in the sheet, but the runner only needs the two columns above.)

Fallback (older): `targets/companies.csv` (single column: `company`).

## Outputs

Files in `data/`:
- `jobs_history.csv`: append-only history (first_seen, last_seen, sources_seen)
- `jobs_latest.csv`: only jobs first seen in the latest run
- `runs.log`: one-line audit summary per run

## Run (GitHub Actions)

1. Push the repo.
2. Open the **Actions** tab.
3. Run the `job-scraper` workflow manually, or let the schedule run daily.

No secrets are required in v1.

## One-time ATS/API audit (recommended first)

This repo includes a **one-time** workflow to scan all companies in your Excel input and record which ATS/API calls succeed.

1. Actions → **ATS Audit (one-time)** → Run workflow
2. After it finishes, check:
   - `data/ats_audit_baseline.json`

What it gives you:
- Per company: detected ATS, API URL used, HTTP status, and a small job count signal.
- A baseline to decide what connector to improve next (usually Workday edge cases and iCIMS).

## View

Open `index.html` (or enable GitHub Pages) to view `data/jobs_latest.csv` as a searchable table.
