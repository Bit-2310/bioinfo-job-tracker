# Context Summary (Bioinfo Job Tracker)

## Current pipeline status
- Main job pull script: `scripts/pull_jobs.py` (updated with improved matching, ATS pagination, and filters).
- UI: `index.html` with tabs (Jobs, Targeted, Filter, Outputs, Unfiltered). Search now works on current tab and CSV parsing is robust.
- GitHub Actions workflow: `.github/workflows/scrape.yml` runs merge + pull_jobs, logs outputs, commits artifacts.

## Key datasets (active)
- `data/targeted_list.json`
- `data/targeted_list_validation.json`
- `data/target_sponsor.json` (manually verified subset only)
- `data/targeted_list_biotech_reference_verified.json`
- `data/targeted_list_combined.json` (merged via `scripts/merge_targets.py`)
- `data/jobs_filter.json` (updated experience filter: max 3 years, exclude 5+ years)
- `data/jobs_unfiltered.jsonl`, `data/jobs_filtered.jsonl`, `data/jobs_latest.csv`, `data/jobs_history.csv`

## Merge helper
- `scripts/merge_targets.py` merges four lists into `data/targeted_list_combined.json` (dedupe by company_name + api_url).

## Matching / filtering logic updates
- Hybrid matcher (word boundaries for <=3 chars, substring for >=4, phrase match for multi-word) with normalization.
- Location include is soft; non-US hard block uses location field only (no longer scans full description).
- Added US location normalization (City, ST; DC; US/USA/Remote/Hybrid).
- Added pipeline-business guard: exclude titles with PIPELINE + (COMMERCIAL/MARKET ACCESS/STRATEGY/OPERATIONS/SALES/MARKETING).
- Expanded title soft includes (bioinformatics/computational/genomics terms).
- Fallback scoring keyword lists if missing in filter JSON.

## ATS pull improvements
- Greenhouse: appends `content=true` to get full descriptions.
- SmartRecruiters: paginates with `offset/limit` and uses `country=us`.

## GitHub Actions schedule (EST converted to UTC)
- 08:30 EST -> 13:30 UTC (Mon–Sat)
- 12:30 EST -> 17:30 UTC (Mon–Sat)
- 17:50 EST -> 22:50 UTC (Mon–Sat)
- 21:30 EST -> 02:30 UTC (Tue–Sun) to avoid Sunday EST run

## Repo cleanup
- `data/archive/` and `scripts/archive/` were zipped into `data/archive.zip` and `scripts/archive.zip`, then removed.

## Sponsor list
- `data/target_sponsor_candidates.json` created from sponsorship CSV prefilter.
- `data/target_sponsor.json` currently contains only **verified** entries (manual additions). Many unverified auto entries were removed.

## Biotech reference
- `scripts/archive/enrich_targeted_from_companies.py` modified to support biotech reference file and `--bioinfo-only`, outputs to `data/targeted_list_biotech_reference.json` and report.
- Verified subset saved to `data/targeted_list_biotech_reference_verified.json` (currently ~23 entries).

## Latest run snapshot (last checked)
- Unfiltered: ~2550–2777 jobs
- Filtered: ~6 jobs
- Bioinfo-titled misses: 44 (often due to age >7 days or seniority exclusions)

## Known issues to revisit
- Filter still strict; consider tuning seniority/age if recall too low.
- Sponsor verification is slow; manual batches in progress.
- `pull_jobs.py` uses combined list; ensure combined file exists before runs.

