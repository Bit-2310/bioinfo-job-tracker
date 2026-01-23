# Bioinformatics Job Tracker (v1)

Minimal job ingestion pipeline that runs on **GitHub Actions**, keeps an **append-only history**, and writes a small "latest" CSV you can browse via `index.html`.

## Sources (v1)

- Greenhouse (API)
- Lever (API)
- Ashby (API)
- iCIMS (best-effort)

## Input

Put your canonical target list Excel file in the **repo root**:

- `Bioinformatics_Job_Target_List.xlsx`

Expected columns:
- `Company Name`
- `Target Role Title`
- `Careers Page URL`

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

## View

Open `index.html` (or enable GitHub Pages) to view `data/jobs_latest.csv` as a searchable table.
