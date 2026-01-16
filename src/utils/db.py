import sqlite3
from contextlib import contextmanager
from typing import Iterator


def _has_column(con: sqlite3.Connection, table: str, column: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def _add_column_if_missing(con: sqlite3.Connection, table: str, column: str, col_def: str) -> None:
    """SQLite-safe 'add column if missing'."""
    if _has_column(con, table, column):
        return
    con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_def}")

@contextmanager
def connect(db_path: str) -> Iterator[sqlite3.Connection]:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON;")
    try:
        yield con
        con.commit()
    finally:
        con.close()

def ensure_tables(con: sqlite3.Connection) -> None:
    # Core entities (so a fresh clone can initialize a DB)
    con.execute(
        """CREATE TABLE IF NOT EXISTS companies (
          company_id INTEGER PRIMARY KEY,
          employer_name TEXT NOT NULL,
          employer_name_norm TEXT,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          UNIQUE(employer_name)
        );"""
    )

    con.execute(
        """CREATE TABLE IF NOT EXISTS company_classification (
          company_id INTEGER PRIMARY KEY,
          `group` INTEGER NOT NULL,
          source_note TEXT,
          updated_at TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY(company_id) REFERENCES companies(company_id)
        );"""
    )

    # Optional enrichment signals (non-authoritative hints).
    # Example: H-1B sponsorship group, NAICS bucket, etc.
    con.execute(
        """CREATE TABLE IF NOT EXISTS company_signals (
          company_id INTEGER NOT NULL,
          signal_key TEXT NOT NULL,
          signal_value TEXT NOT NULL,
          updated_at TEXT NOT NULL DEFAULT (datetime('now')),
          PRIMARY KEY(company_id, signal_key),
          FOREIGN KEY(company_id) REFERENCES companies(company_id)
        );"""
    )

    con.execute(
        """CREATE TABLE IF NOT EXISTS company_job_sources (
          source_id INTEGER PRIMARY KEY,
          company_id INTEGER NOT NULL,
          source_type TEXT NOT NULL,
          careers_url TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 1,
          last_checked_at TEXT,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          UNIQUE(company_id, careers_url),
          FOREIGN KEY(company_id) REFERENCES companies(company_id)
        );"""
    )

    con.execute(
        """CREATE TABLE IF NOT EXISTS roles (
          role_id INTEGER PRIMARY KEY,
          company_id INTEGER NOT NULL,
          title TEXT NOT NULL,
          location TEXT,
          remote_type TEXT,
          employment_type TEXT,
          seniority TEXT,
          role_family TEXT,
          match_score REAL NOT NULL DEFAULT 0.0,
          posted_at TEXT,
          posted_date TEXT,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          apply_url TEXT NOT NULL,
          apply_url_canonical TEXT,
          source_job_id TEXT,
          source_type TEXT NOT NULL,
          source_id INTEGER,
          description TEXT,
          UNIQUE(company_id, apply_url),
          FOREIGN KEY(company_id) REFERENCES companies(company_id),
          FOREIGN KEY(source_id) REFERENCES company_job_sources(source_id)
        );"""
    )

    con.execute(
        """CREATE TABLE IF NOT EXISTS runs (
          run_id INTEGER PRIMARY KEY,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          companies_checked INTEGER NOT NULL DEFAULT 0,
          roles_seen INTEGER NOT NULL DEFAULT 0,
          new_roles INTEGER NOT NULL DEFAULT 0,
          updated_roles INTEGER NOT NULL DEFAULT 0,
          closed_roles INTEGER NOT NULL DEFAULT 0,
          errors TEXT
        );"""
    )

    # Per-source run telemetry (debuggability + prevents false closures)
    con.execute(
        """CREATE TABLE IF NOT EXISTS source_runs (
          source_run_id INTEGER PRIMARY KEY,
          run_id INTEGER NOT NULL,
          source_id INTEGER NOT NULL,
          company_id INTEGER NOT NULL,
          source_type TEXT NOT NULL,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          status TEXT NOT NULL,
          roles_seen INTEGER NOT NULL DEFAULT 0,
          new_roles INTEGER NOT NULL DEFAULT 0,
          updated_roles INTEGER NOT NULL DEFAULT 0,
          error TEXT,
          FOREIGN KEY(run_id) REFERENCES runs(run_id),
          FOREIGN KEY(source_id) REFERENCES company_job_sources(source_id),
          FOREIGN KEY(company_id) REFERENCES companies(company_id)
        );"""
    )

    # Backward/forward compatible migrations
    _add_column_if_missing(con, "roles", "apply_url_canonical", "TEXT")
    _add_column_if_missing(con, "roles", "source_job_id", "TEXT")

    con.execute("CREATE INDEX IF NOT EXISTS idx_roles_first_seen ON roles(first_seen_at);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_roles_company_status ON roles(company_id, status);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_roles_canonical ON roles(company_id, apply_url_canonical);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_source_runs_run ON source_runs(run_id);")
