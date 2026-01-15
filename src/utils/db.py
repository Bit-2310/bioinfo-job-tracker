import sqlite3
from contextlib import contextmanager
from typing import Iterator

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

    con.execute("CREATE INDEX IF NOT EXISTS idx_roles_first_seen ON roles(first_seen_at);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_roles_company_status ON roles(company_id, status);")
