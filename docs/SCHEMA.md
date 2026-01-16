# Database schema (jobs.db)

This repo uses a single SQLite database at `db/jobs.db`.

The DB is designed around one principle:

**The curated target list (Group 1/2) is the source of truth for what we track.**

## Core tables

### `companies`
One row per company in your curated target list.

Columns
- `company_id` (PK)
- `employer_name` (display name)
- `employer_name_norm` (normalized name; used for de-duping)
- `created_at`

### `company_classification`
Stores your *priority group* per company.

Columns
- `company_id` (PK, FK -> companies)
- `group` (1 or 2)
- `source_note` (where the label came from, e.g. `priority_csv`)
- `updated_at`

### `company_job_sources`
One row per trackable job source (Greenhouse, Lever, etc.) for a company.

Columns
- `source_id` (PK)
- `company_id` (FK)
- `source_type` (`greenhouse`, `lever`, ...)
- `careers_url` (human-facing URL)
- `is_active` (1/0)
- `last_checked_at`
- `created_at`

### `roles`
One row per role/job posting.

Identity strategy
- Prefer `(company_id, source_type, source_job_id)` when available.
- Otherwise fall back to `(company_id, apply_url_canonical)`.

Key columns
- `status` (`active` or `closed`)
- `first_seen_at`, `last_seen_at` (used for new/closed logic)
- `match_score`, `role_family` (used for ranking)

### `runs`
One row per pipeline run (high-level counters).

### `source_runs`
One row per source per run.

This is the main debugging table when the dashboard is empty.

## Enrichment tables

### `company_signals`
Optional non-authoritative signals.

Example
- `signal_key=h1b_group` (imported from the FY'25 spreadsheet)

These signals should not redefine your priority groups.
