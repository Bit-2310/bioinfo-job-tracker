"""build_analytics.py

Creates lightweight analytics JSON files for the GitHub Pages dashboard.

Why this exists
--------------
GitHub Actions logs are annoying to open multiple times a day. This script
summarizes the database into small JSON files the static site can read.

Outputs (written to docs/data/)
------------------------------
run_summary.json
  High level run health (timestamps + counts).

source_analytics.json
  Counts of discovered sources, broken down by type.

company_priority.json
  A simple priority model (tiered) based on how often companies post roles.

Note
----
This is intentionally “simple and robust”. We avoid heavyweight charts here;
the UI can render tables and small bar charts from these JSON files.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

from src.utils.db import connect, ensure_tables


SETTINGS = yaml.safe_load(open("src/config/settings.yml"))
DB_PATH = SETTINGS["db_path"]
OUT_DIR = Path("docs/data")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class SourceAnalytics:
    total_companies: int
    companies_with_sources: int
    sources_total: int
    sources_by_type: dict
    cursor: int


def get_discover_cursor(con) -> int:
    try:
        cur = con.execute("SELECT v FROM state_kv WHERE k='discover_cursor'")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0


def compute_source_analytics(con) -> SourceAnalytics:
    total_companies = con.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    companies_with_sources = con.execute(
        "SELECT COUNT(DISTINCT company_id) FROM company_job_sources WHERE is_active=1"
    ).fetchone()[0]
    sources_total = con.execute(
        "SELECT COUNT(*) FROM company_job_sources WHERE is_active=1"
    ).fetchone()[0]

    rows = con.execute(
        """
        SELECT source_type, COUNT(*)
        FROM company_job_sources
        WHERE is_active=1
        GROUP BY source_type
        ORDER BY COUNT(*) DESC
        """
    ).fetchall()
    sources_by_type = {r[0] or "unknown": int(r[1]) for r in rows}
    cursor = get_discover_cursor(con)

    return SourceAnalytics(
        total_companies=int(total_companies),
        companies_with_sources=int(companies_with_sources),
        sources_total=int(sources_total),
        sources_by_type=sources_by_type,
        cursor=int(cursor),
    )


def compute_run_summary(con) -> dict:
    # runs table may not exist in older DBs. If missing, fall back to “now”.
    last_run = None
    try:
        row = con.execute(
            "SELECT started_at, finished_at, status FROM runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        if row:
            last_run = {"started_at": row[0], "finished_at": row[1], "status": row[2]}
    except Exception:
        last_run = None

    roles_total = con.execute("SELECT COUNT(*) FROM roles").fetchone()[0]
    active_total = con.execute("SELECT COUNT(*) FROM roles WHERE is_active=1").fetchone()[0]
    companies_ranked = 0
    try:
        companies_ranked = con.execute("SELECT COUNT(*) FROM company_rankings").fetchone()[0]
    except Exception:
        companies_ranked = 0

    return {
        "generated_at": utc_now_iso(),
        "last_run": last_run,
        "counts": {
            "roles_total": int(roles_total),
            "active_roles": int(active_total),
            "ranked_companies": int(companies_ranked),
        },
    }


def compute_company_priority(con) -> list[dict]:
    """Return a list of companies with a tier based on role activity.

    Tiering is intentionally simple:
      - Tier 1 (hot): new roles in last 7d >= 3
      - Tier 2 (warm): new roles in last 7d >= 1
      - Tier 3 (cold): active roles > 0 but no new roles in 7d
      - Tier 4 (unknown): no active roles yet

    Score is used for sorting and future scheduling.
    """

    # new roles in last 7d based on first_seen
    rows = con.execute(
        """
        SELECT
          c.employer_name,
          SUM(CASE WHEN r.first_seen >= datetime('now','-7 days') THEN 1 ELSE 0 END) AS new_7d,
          SUM(CASE WHEN r.is_active=1 THEN 1 ELSE 0 END) AS active_roles,
          AVG(COALESCE(r.match_score, 0)) AS avg_match
        FROM companies c
        LEFT JOIN roles r ON r.company_id = c.company_id
        GROUP BY c.company_id
        """
    ).fetchall()

    out = []
    for name, new_7d, active_roles, avg_match in rows:
        new_7d = int(new_7d or 0)
        active_roles = int(active_roles or 0)
        avg_match = float(avg_match or 0.0)

        # simple weighted score
        score = (new_7d * 5) + (active_roles * 2) + (avg_match * 1)

        if new_7d >= 3:
            tier = "Tier 1"
            label = "Hot"
        elif new_7d >= 1:
            tier = "Tier 2"
            label = "Warm"
        elif active_roles > 0:
            tier = "Tier 3"
            label = "Cold"
        else:
            tier = "Tier 4"
            label = "Unknown"

        out.append(
            {
                "company": name,
                "new_roles_7d": new_7d,
                "active_roles": active_roles,
                "avg_match_score": round(avg_match, 3),
                "score": round(score, 3),
                "tier": tier,
                "label": label,
            }
        )

    out.sort(key=lambda x: (x["tier"], -x["score"], x["company"] or ""))
    return out


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with connect(DB_PATH) as con:
        ensure_tables(con)

        source_ana = compute_source_analytics(con)
        run_summary = compute_run_summary(con)
        priority = compute_company_priority(con)

    write_json(OUT_DIR / "source_analytics.json", asdict(source_ana))
    write_json(OUT_DIR / "run_summary.json", run_summary)
    write_json(OUT_DIR / "company_priority.json", {"generated_at": utc_now_iso(), "companies": priority})

    print(
        f"[analytics] wrote source_analytics.json, run_summary.json, company_priority.json (companies={len(priority)})",
        flush=True,
    )


if __name__ == "__main__":
    main()
