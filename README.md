# Bioinformatics Job Tracker

Job ingestion pipeline that runs on **GitHub Actions**, keeps an **append-only history**, and writes a small "latest" CSV you can browse via `index.html`.

## What changed (v2)

Instead of guessing ATS slugs from the company name (which is usually wrong), we build a **master ATS registry** from your Excel target list:

`Bioinformatics_Job_Target_List.xlsx` ➜ `data/master_registry.json` ➜ scraper routes each company to the right API.

## Sources

- Greenhouse (public API)
- Lever (public API)
- Workday (common public `cxs` endpoint, best-effort)
- Ashby (best-effort; endpoint varies)
- iCIMS (best-effort; usually needs per-company config)

## Input (recommended)

Commit this file at repo root:

- `Bioinformatics_Job_Target_List.xlsx`

Required columns:
- `Company Name`
- `Target Role Title`
- `Careers Page URL`

The workflow will auto-generate:
- `data/master_registry.json`

## Output

Files in `data/`:
- `master_registry.json`: detected ATS + tokens per company
- `jobs_history.csv`: append-only history (first_seen, last_seen, sources_seen)
- `jobs_latest.csv`: only jobs first seen in the latest run
- `runs.log`: one-line audit summary per run

## Run

Push the repo and let GitHub Actions run (Mon–Sat, 4x/day). No secrets needed.

## Local run

```bash
pip install -r requirements.txt
python src/build_master_registry.py --input Bioinformatics_Job_Target_List.xlsx --output data/master_registry.json
python src/run.py
```

## View

Open `index.html` (or enable GitHub Pages) to view `data/jobs_latest.csv` as a searchable table.
