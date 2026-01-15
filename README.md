# Bioinformatics Job Tracker

SQLite + GitHub Actions + GitHub Pages.

## What it does
1. **Discover sources**: finds careers/ATS links for companies in `db/jobs.db` (table: `companies`)
   - Runs in **small batches** each scheduled run
   - Remembers progress with a cursor (`state_kv.discover_cursor`)
   - Prints progress every `verify.log_every` companies so Actions doesn't look frozen
2. **Validate sources**: checks links and deactivates broken ones
3. **Track roles**: pulls postings (Greenhouse + Lever supported)
4. **Export JSON**: writes `docs/data/*.json` for the dashboard

## GitHub Pages
Enable Pages and set **Folder: /docs** on branch `main`.

## Local run
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

PYTHONPATH=. python src/verify/discover_sources.py
PYTHONPATH=. python src/verify/validate_sources.py
PYTHONPATH=. python src/track/fetch_roles.py
PYTHONPATH=. python src/export/build_json.py

# Quick DB sanity check
PYTHONPATH=. python src/demo/preview_db.py
```
