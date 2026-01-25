# Context Summary (Bioinfo Job Tracker)

## What this repo does
- Builds a company list (bioinformatics/biotech) and detects ATS/job board providers.
- Produces `data/targeted_list.json` as the working dataset for job pulls.
- Validates ATS endpoints and runs one-off job pull tests.
- Intended to run locally now and later via GitHub Actions.

## Current key datasets (after user moved files)
- Active:
  - `data/targeted_list.json`
  - `data/targeted_list_validation.json`
  - `data/targeted_list_newlist.json`
  - `data/job_pull_test.json`
- Archived (moved by user):
  - `data/archive/*` (companies lists, reports, old inputs)
  - `scripts/archive/*` (older scripts)

## Current stats (latest)
- `data/targeted_list.json`: 347 entries, 0 duplicates.
  - API breakdown: careers_url 297, greenhouse 32, workday 13, icims 3, lever 1, smartrecruiters 1.
- Validation sample (250): 194 success, 56 fail. Failures mainly careers_url/greenhouse/workday.
- One-off job pull test:
  - 199 processed; 185 ok, 14 http_error.
  - jobs_count available: 61 (mostly link heuristics + GH/SmartRecruiters/Lever).

## Pipelines used
- Curate companies (expanded):
  - `scripts/archive/curate_top100_us_bioinformatics.py` (now archived)
  - new: `scripts/curate_top100_us_bioinformatics.py` (expanded to 300 using BioPharmGuy + Wikipedia pharma lists).
- ATS detection:
  - `scripts/archive/company_api_collector.py` (older)
  - current: `scripts/company_api_collector.py`
- Enrichment from companies list (parallel via Bing RSS search):
  - `scripts/tighten_ats_from_careers.py` (archived)
  - `scripts/enrich_targeted_from_companies.py`
- Enrichment from newlist:
  - `scripts/enrich_targeted_from_newlist.py`
- Repair failed validations:
  - `scripts/repair_failed_validations.py`
- Validation:
  - `scripts/archive/validate_targeted_list.py`
- One-off job pull:
  - `scripts/job_pull_test.py`

## Notes on limitations
- Many companies only have `careers_url` (no ATS API detected).
- Workday often fails due to tenant/site path issues or blocking.
- Static HTML parsing can’t see JS-rendered job listings.
- Bing RSS search sometimes returns irrelevant results; some wrong careers URLs were corrected by fallback.

## Latest key outputs
- `data/companies.csv` replaced with curated top-300 list.
- `data/targeted_list.json` includes all companies from `data/companies.csv` and `data/archive/newlist.csv`.
- `data/companies_enrichment_report.json`, `data/ats_tighten_report.json`, `data/validation_repair_report.json` exist for audit.

## GitHub Actions considerations
- Prefer running light validation + job pulls in CI.
- Heavier enrichment steps (web search, ATS repair) may be optional via flags/env vars.

