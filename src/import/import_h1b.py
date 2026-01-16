"""Import FY'25 H-1B sponsorship spreadsheet into jobs.db.

Purpose
- Populate / refresh company sponsorship group classification used by the dashboard.
- Group logic (matches repo README wording):
  - Group 1: active sponsors (new petitions in FY'25)
  - Group 2: past sponsors (only renewals / no new filings in FY'25)
  - Group 3: non-sponsors

The provided spreadsheet has a few preamble rows before the header row. We
detect the header row by looking for the "Employer (Petitioner) Name" column.

Run (from repo root):
  PYTHONPATH=. python src/import/import_h1b.py --excel "<file.xlsx>" --db db/jobs.db

By default, it reads these sheets if present:
- "BioTechnology "
- "Health Care & Public Health"

No assumptions beyond the sheet contents; we only import what is present.
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path
from typing import Iterable, Iterator

import pandas as pd


def norm_name(name: str) -> str:
    """Normalize employer names for de-duping."""
    s = (name or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def ensure_core_tables(con: sqlite3.Connection) -> None:
    # companies table (matches existing schema)
    con.execute(
        """CREATE TABLE IF NOT EXISTS companies (
          company_id INTEGER PRIMARY KEY,
          employer_name TEXT NOT NULL,
          employer_name_norm TEXT NOT NULL UNIQUE,
          primary_industry TEXT,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );"""
    )

    con.execute(
        """CREATE TABLE IF NOT EXISTS company_classification (
          company_id INTEGER PRIMARY KEY,
          "group" INTEGER NOT NULL,
          FOREIGN KEY (company_id) REFERENCES companies(company_id)
        );"""
    )


def find_header_row(df: pd.DataFrame) -> int | None:
    target = "Employer (Petitioner) Name".lower()
    for i in range(min(len(df), 50)):
        row = [str(x).strip().lower() for x in df.iloc[i].tolist()]
        if target in row:
            return i
    return None


def iter_records(excel_path: Path, sheets: Iterable[str]) -> Iterator[tuple[str, int]]:
    xl = pd.ExcelFile(excel_path)

    # Map requested sheets to actual sheet names (handles trailing spaces)
    requested = [s for s in sheets if s]
    sheet_map: dict[str, str] = {}
    for s in requested:
        s_clean = s.strip().lower()
        for actual in xl.sheet_names:
            if actual.strip().lower() == s_clean:
                sheet_map[s] = actual
                break

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

        # Drop empty rows
        if "Employer (Petitioner) Name" not in df.columns:
            continue

        df = df.dropna(subset=["Employer (Petitioner) Name"])

        for _, r in df.iterrows():
            name = str(r.get("Employer (Petitioner) Name", "")).strip()
            if not name:
                continue

            # Determine group using counts:
            # - new petitions indicated by any Initial Approval/Denial
            # - renewals indicated by Continuing Approval/Denial
            def as_int(x) -> int:
                try:
                    if pd.isna(x):
                        return 0
                    return int(float(x))
                except Exception:
                    return 0

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
    row = con.execute(
        "SELECT company_id FROM companies WHERE employer_name_norm = ?", (n,)
    ).fetchone()
    return int(row[0])


def upsert_classification(con: sqlite3.Connection, company_id: int, group: int) -> None:
    con.execute(
        """INSERT INTO company_classification (company_id, "group")
           VALUES (?, ?)
           ON CONFLICT(company_id) DO UPDATE SET "group"=excluded."group"
        """,
        (company_id, int(group)),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", required=True, help="Path to the FY'25 sponsorship Excel file")
    ap.add_argument("--db", default="db/jobs.db", help="SQLite db path (default: db/jobs.db)")
    ap.add_argument(
        "--sheets",
        default="BioTechnology ,Health Care & Public Health",
        help="Comma-separated sheet names to read (default matches provided file)",
    )
    args = ap.parse_args()

    excel_path = Path(args.excel)
    db_path = Path(args.db)
    sheets = [s for s in (args.sheets or "").split(",")]

    con = sqlite3.connect(db_path)
    try:
        ensure_core_tables(con)

        imported = 0
        for name, group in iter_records(excel_path, sheets):
            cid = upsert_company(con, name)
            upsert_classification(con, cid, group)
            imported += 1

        con.commit()
        print(f"Imported {imported} company classifications into {db_path}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
