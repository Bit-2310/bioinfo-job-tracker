# Bioinformatics Job Tracker

SQLite + GitHub Actions + GitHub Pages.

This repo keeps a **living list of bioinformatics-relevant job openings** by:
1) discovering company career pages (safe, not “scrape everything”),
2) validating the discovered sources,
3) pulling job postings,
4) exporting JSON for a static dashboard.

The dashboard is served from **GitHub Pages** (`/docs`).

---

## What you get

**Dashboard (GitHub Pages)**
- New roles (last 24h)
- Active roles
- Company rankings
- Analytics (source counts, priority groups)
- Workflow viewer (shows `.github/workflows/track.yml` inside the site)

**Database (SQLite)**
- `db/jobs.db` stores companies, sources, roles, and run history.

**Automation (GitHub Actions)**
- Runs 3–4 times/day (UTC schedule)
- Progress logs printed every N companies
- Batch processing (cursor-based), so long lists don’t stall

---

## Architecture

```mermaid
flowchart TD
  A[Base company list
    (Excel -> companies table)] -->|seed| DB[(SQLite: db/jobs.db)]

  DB --> B[Discover sources
    src/verify/discover_sources.py]
  B -->|writes| DB

  DB --> C[Validate sources
    src/verify/validate_sources.py]
  C -->|marks active/inactive| DB

  DB --> D[Track roles
    src/track/fetch_roles.py]
  D -->|upsert roles| DB

  DB --> E[Export JSON
    src/export/build_json.py]
  DB --> F[Build analytics
    src/export/build_analytics.py]

  E --> SITE[docs/data/*.json]
  F --> SITE

  SITE --> UI[GitHub Pages dashboard
    docs/index.html]

  subgraph GA[GitHub Actions: .github/workflows/track.yml]
    B --> C --> D --> E --> F
  end
```

---

## Data files (what the site reads)

These are written by the workflow into `docs/data/`:

- `new_roles.json`
- `active_roles.json`
- `company_rankings.json`
- `metadata.json`
- `source_analytics.json`
- `run_summary.json`
- `company_priority.json`
- `track.yml` (copied from `.github/workflows/track.yml` each run)

If some files are missing on the first run, the dashboard will still load and show “—”.

---

## How batching works

**Problem:** you have thousands of companies, and discovery can take too long.

**Solution:** `discover_sources.py` uses a **cursor** stored in SQLite (`state_kv` table). Each run processes a batch (default 100). Next run continues from the last cursor.

Key settings (in `src/config/settings.yml` under `verify:`):
- `limit_default`: companies per discovery run
- `max_workers`: thread workers for discovery
- `log_every`: print progress every N companies
- `max_discovery_minutes`: hard time budget to avoid runs hanging

---

## Setup (local)

### 1) Create env

```bash
conda create -n jobtracker python=3.11 -y
conda activate jobtracker
pip install -r requirements.txt
```

### 2) Run pipeline locally

```bash
export PYTHONPATH=.
python src/verify/discover_sources.py
python src/verify/validate_sources.py
python src/track/fetch_roles.py
python src/export/build_json.py
python src/export/build_analytics.py
```

### 3) View dashboard locally

Easiest:

```bash
python -m http.server -d docs 8000
```

Open: `http://localhost:8000`

---

## GitHub Pages

In repo **Settings → Pages**:
- Source: **Deploy from a branch**
- Branch: `main`
- Folder: `/docs`

Note: Pages content is public even if the repo is private.

---

## GitHub Actions

Workflow: `.github/workflows/track.yml`

It:
1. installs deps
2. discovers sources (batched)
3. validates
4. tracks roles
5. exports JSON + analytics
6. copies workflow file into `docs/data/track.yml`
7. commits updates

---

## Pros, cons, and how we handle them

### Pros
- Free hosting (Pages)
- Simple storage (SQLite)
- Repeatable automation (Actions)
- Static site (fast, no backend to maintain)

### Cons + Fixes
- **Career sites vary a lot** (Workday/custom portals)
  - Fix: keep a `source_type` and add parsers incrementally.
- **Discovery can stall** (slow networks)
  - Fix: time budget + batch cursor + retries/timeouts.
- **Rate limits / blocking**
  - Fix: polite pauses, fewer concurrent workers, cache stable sources.

---

## Next upgrades (optional)

- Per-company scoring over time (trend-based priority)
- Add a “Watchlist” tab (companies you care about)
- Add email/Discord notifications (only for new roles)
- Add structured keywords per role family and skill tags
