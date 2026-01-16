"""Import the curated target company list (priority Groups 1 and 2).

This is the source of truth for which companies the tracker cares about.

CSV format (repo root):
  data/priority_companies.csv

Required columns:
  company, group

Where group is 1 or 2.

What this script does
1) Upserts companies into `companies` (normalized by `employer_name_norm`).
2) Upserts the priority group into `company_classification`.

Optional: --prune
If you pass --prune, the script will delete companies (and their sources/roles)
that are NOT in the CSV list. This is useful to clean a DB that was previously
polluted by bulk imports.

Run:
  PYTHONPATH=. python src/import/import_priority_companies.py --csv data/priority_companies.csv --db db/jobs.db
  PYTHONPATH=. python src/import/import_priority_companies.py --csv data/priority_companies.csv --db db/jobs.db --prune
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path

from src.utils.db import connect, ensure_tables


def ensure_company_classification_schema(cur) -> None:
    """SQLite-safe migrations for older DBs.

    The pipeline evolved to store a couple metadata fields on company_classification.
    Older jobs.db files may not have them, so we add them if missing.
    """
    cur.execute("PRAGMA table_info(company_classification)")
    existing_cols = {row[1] for row in cur.fetchall()}

    if "source_note" not in existing_cols:
        cur.execute(
            "ALTER TABLE company_classification "
            "ADD COLUMN source_note TEXT"
        )

    if "updated_at" not in existing_cols:
        # Keep it nullable for backward-compatibility; we'll write datetime('now') going forward.
        cur.execute(
            "ALTER TABLE company_classification "
            "ADD COLUMN updated_at TEXT"
        )


def norm_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


@dataclass
class Row:
    company: str
    group: int


def read_rows(path: Path) -> list[Row]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []

        rows: list[Row] = []
        for r in reader:
            company = (r.get("company") or "").strip()
            grp_raw = (r.get("group") or "").strip()
            if not company:
                continue
            try:
                grp = int(grp_raw)
            except Exception:
                raise ValueError(f"Invalid group for '{company}': {grp_raw!r}")
            if grp not in (1, 2):
                raise ValueError(f"Group must be 1 or 2 for '{company}', got {grp}")
            rows.append(Row(company=company, group=grp))
        return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to data/priority_companies.csv")
    ap.add_argument("--db", default="db/jobs.db", help="SQLite db path")
    ap.add_argument(
        "--prune",
        action="store_true",
        help="Delete companies/sources/roles not present in the CSV (dangerous, but useful for cleanup)",
    )
    ap.add_argument(
        "--min-rows",
        type=int,
        default=5,
        help="Minimum non-empty rows required (safety check, default: 5)",
    )
    args = ap.parse_args()

    csv_path = Path(args.csv)
    rows = read_rows(csv_path)
    if len(rows) < args.min_rows:
        raise SystemExit(
            f"priority_import: refusing to run because {csv_path} has {len(rows)} rows (< min_rows={args.min_rows})."
        )

    keep_norms = {norm_name(r.company) for r in rows}

    upserted = 0
    with connect(args.db) as con:
        ensure_tables(con)
        cur = con.cursor()
        ensure_company_classification_schema(cur)

        for r in rows:
            n = norm_name(r.company)
            # Be robust to older schemas that don't enforce a UNIQUE constraint on employer_name_norm.
            row = cur.execute(
                "SELECT company_id, employer_name FROM companies WHERE employer_name_norm=?",
                (n,),
            ).fetchone()
            if row:
                cid = row[0]
                if (row[1] or "") != r.company:
                    cur.execute(
                        "UPDATE companies SET employer_name=? WHERE company_id=?",
                        (r.company, cid),
                    )
            else:
                cur.execute(
                    "INSERT INTO companies (employer_name, employer_name_norm) VALUES (?, ?)",
                    (r.company, n),
                )
                cid = cur.lastrowid

            cur.execute(
                """INSERT INTO company_classification (company_id, `group`, source_note, updated_at)
                   VALUES (?, ?, 'priority_csv', datetime('now'))
                   ON CONFLICT(company_id) DO UPDATE SET
                     `group`=excluded.`group`,
                     source_note=excluded.source_note,
                     updated_at=datetime('now')
                """,
                (cid, int(r.group)),
            )
            upserted += 1

        deleted_companies = 0
        deleted_sources = 0
        deleted_roles = 0

        if args.prune:
            # Find company_ids to keep
            q_marks = ",".join(["?"] * len(keep_norms))
            keep_ids = [
                row[0]
                for row in cur.execute(
                    f"SELECT company_id FROM companies WHERE employer_name_norm IN ({q_marks})",
                    tuple(keep_norms),
                ).fetchall()
            ]

            if keep_ids:
                keep_marks = ",".join(["?"] * len(keep_ids))

                # Delete dependents first (FK-safe order)
                deleted_roles = cur.execute(
                    f"DELETE FROM roles WHERE company_id NOT IN ({keep_marks})",
                    tuple(keep_ids),
                ).rowcount

                # source_runs references both company_job_sources and companies
                cur.execute(
                    f"DELETE FROM source_runs WHERE company_id NOT IN ({keep_marks})",
                    tuple(keep_ids),
                )

                # Some DBs may have H-1B enrichment rows that reference companies
                cur.execute(
                    f"DELETE FROM h1b_employer_sites WHERE company_id NOT IN ({keep_marks})",
                    tuple(keep_ids),
                )

                deleted_sources = cur.execute(
                    f"DELETE FROM company_job_sources WHERE company_id NOT IN ({keep_marks})",
                    tuple(keep_ids),
                ).rowcount
                cur.execute(
                    f"DELETE FROM company_signals WHERE company_id NOT IN ({keep_marks})",
                    tuple(keep_ids),
                )
                cur.execute(
                    f"DELETE FROM company_classification WHERE company_id NOT IN ({keep_marks})",
                    tuple(keep_ids),
                )
                deleted_companies = cur.execute(
                    f"DELETE FROM companies WHERE company_id NOT IN ({keep_marks})",
                    tuple(keep_ids),
                ).rowcount
            else:
                # Should not happen due to min_rows check, but keep safe.
                raise SystemExit("priority_import: prune requested but keep list resolved to 0 company_ids")

    print(
        f"priority_import: rows={len(rows)} upserted={upserted} prune={args.prune} "
        f"deleted_companies={deleted_companies} deleted_sources={deleted_sources} deleted_roles={deleted_roles}",
        flush=True,
    )


if __name__ == "__main__":
    main()
