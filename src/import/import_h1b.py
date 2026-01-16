"""Import FY'25 H-1B sponsorship spreadsheet as an *enrichment signal*.

Important
This repo treats priority groups (Group 1/2) as a *curated target list*.
H-1B information is optional and should NOT redefine your priority groups.

This importer stores the sponsorship grouping into the `company_signals` table
under:
  signal_key = "h1b_group"
  signal_value = "1" | "2" | "3"

By default, we only enrich companies that already exist in `companies`.
Use --create-missing if you explicitly want to create new companies from the
spreadsheet (not recommended for the job-tracker use case).

Run (from repo root):
  PYTHONPATH=. python src/import/import_h1b.py --excel "<file.xlsx>" --db db/jobs.db
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path
from typing import Iterable, Iterator

import pandas as pd

from src.utils.db import connect, ensure_tables


def norm_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def find_header_row(df: pd.DataFrame) -> int | None:
    target = "Employer (Petitioner) Name".lower()
    for i in range(min(len(df), 60)):
        row = [str(x).strip().lower() for x in df.iloc[i].tolist()]
        if target in row:
            return i
    return None


def iter_records(excel_path: Path, sheets: Iterable[str]) -> Iterator[tuple[str, int]]:
    xl = pd.ExcelFile(excel_path)

    requested = [s for s in sheets if s]
    # Map requested sheets to actual sheet names (handles trailing spaces)
    sheet_map: dict[str, str] = {}
    for s in requested:
        s_clean = s.strip().lower()
        for actual in xl.sheet_names:
            if actual.strip().lower() == s_clean:
                sheet_map[s] = actual
                break

    def as_int(x) -> int:
        try:
            if pd.isna(x):
                return 0
            return int(float(x))
        except Exception:
            return 0

    for s in requested:
        actual = sheet_map.get(s)
        if not actual:
            continue

        raw = pd.read_excel(xl, sheet_name=actual, header=None)
        hdr_i = find_header_row(raw)
        if hdr_i is None:
            continue

        headers = raw.iloc[hdr_i].tolist()
        df = raw.iloc[hdr_i + 1 :].copy()
        df.columns = headers

        if "Employer (Petitioner) Name" not in df.columns:
            continue

        df = df.dropna(subset=["Employer (Petitioner) Name"])

        for _, r in df.iterrows():
            name = str(r.get("Employer (Petitioner) Name", "")).strip()
            if not name:
                continue

            init = as_int(r.get("Initial Approval")) + as_int(r.get("Initial Denial"))
            cont = as_int(r.get("Continuing Approval")) + as_int(r.get("Continuing Denial"))

            if init > 0:
                group = 1
            elif cont > 0:
                group = 2
            else:
                group = 3

            yield (name, group)


def upsert_company(con: sqlite3.Connection, employer_name: str) -> int:
    n = norm_name(employer_name)
    con.execute(
        """INSERT INTO companies (employer_name, employer_name_norm)
           VALUES (?, ?)
           ON CONFLICT(employer_name_norm) DO UPDATE SET employer_name=excluded.employer_name
        """,
        (employer_name.strip(), n),
    )
    row = con.execute("SELECT company_id FROM companies WHERE employer_name_norm=?", (n,)).fetchone()
    return int(row[0])


def set_signal(con: sqlite3.Connection, company_id: int, key: str, value: str) -> None:
    con.execute(
        """INSERT INTO company_signals (company_id, signal_key, signal_value, updated_at)
           VALUES (?, ?, ?, datetime('now'))
           ON CONFLICT(company_id, signal_key) DO UPDATE SET
             signal_value=excluded.signal_value,
             updated_at=datetime('now')
        """,
        (company_id, key, value),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", required=True, help="Path to FY'25 sponsorship Excel file")
    ap.add_argument("--db", default="db/jobs.db", help="SQLite db path")
    ap.add_argument(
        "--sheets",
        default="BioTechnology ,Health Care & Public Health",
        help="Comma-separated sheet names to read",
    )
    ap.add_argument(
        "--create-missing",
        action="store_true",
        help="Create new companies from Excel (not recommended for the curated tracker)",
    )
    args = ap.parse_args()

    excel_path = Path(args.excel)
    sheets = [s for s in (args.sheets or "").split(",")]

    imported = 0
    skipped = 0
    created = 0

    with connect(args.db) as con:
        ensure_tables(con)
        cur = con.cursor()

        for name, group in iter_records(excel_path, sheets):
            n = norm_name(name)
            row = cur.execute(
                "SELECT company_id FROM companies WHERE employer_name_norm=?", (n,)
            ).fetchone()

            if row:
                cid = int(row[0])
            else:
                if not args.create_missing:
                    skipped += 1
                    continue
                cid = upsert_company(con, name)
                created += 1

            set_signal(con, cid, "h1b_group", str(int(group)))
            imported += 1

    print(
        f"h1b_import: enriched={imported} skipped_missing={skipped} created={created} db={args.db}",
        flush=True,
    )


if __name__ == "__main__":
    main()
