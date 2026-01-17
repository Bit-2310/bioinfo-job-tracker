"""bioinfo-job-tracker JSON exporter.

This script builds the JSON files consumed by the GitHub Pages dashboard
(`docs/`). It is written against the *current* SQLite schema used by this
repo (see `src/utils/db.py`).

Run from repo root:

  PYTHONPATH=. python src/export/build_json.py

Outputs (written to `docs/data/`):
- meta.json
- new_roles.json
- active_roles.json
- company_rankings.json
- group_summary.json
 - top_picks.json

Notes
- "New" roles are based on `roles.first_seen_at` and the configured
  `export.new_window_hours` in `src/config/settings.yml`.
- "Active" roles are `roles.status = 'active'`.
- Priority groups come from `company_classification`.
  Only groups (1,2) are included in dashboard outputs.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import yaml

SETTINGS = yaml.safe_load(open("src/config/settings.yml"))
DB_PATH = SETTINGS["db_path"]
NEW_WINDOW_HOURS = int(SETTINGS.get("export", {}).get("new_window_hours", 24))

OUT_DIR = Path("docs/data")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def get_last_run_ts(con: sqlite3.Connection) -> str:
    try:
        row = con.execute(
            "SELECT COALESCE(finished_at, started_at) FROM runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass
    return utc_now_iso()
def fetch_roles(con: sqlite3.Connection, where_sql: str, params: tuple) -> list[dict]:
    """Fetch roles joined with company + classification.

    where_sql can start with 'WHERE' or 'AND' or be empty.
    This function always ensures the final SQL has exactly one WHERE clause.
    """
    where_sql = (where_sql or "").strip()
    if where_sql:
        if where_sql.upper().startswith("WHERE"):
            where_sql = "AND " + where_sql[5:].strip()
        elif not where_sql.upper().startswith("AND"):
            where_sql = "AND " + where_sql

    rows = con.execute(
        f"""
        SELECT
          r.role_id,
          c.employer_name,
          r.title,
          r.location,
          r.role_family,
          r.match_score,
          r.posted_at,
          r.first_seen_at,
          r.last_seen_at,
          r.status,
          r.apply_url,
          r.source_type,
          cl."group" AS grp
        FROM roles r
        JOIN companies c ON c.company_id = r.company_id
        JOIN company_classification cl ON cl.company_id = r.company_id
        WHERE cl."group" IN (1,2)
        {where_sql}
        ORDER BY r.first_seen_at DESC
        """,
        params,
    ).fetchall()

    out: list[dict] = []
    for (
        role_id,
        employer_name,
        title,
        location,
        role_family,
        match_score,
        posted_at,
        first_seen_at,
        last_seen_at,
        status,
        apply_url,
        source_type,
        grp,
    ) in rows:
        out.append(
            {
                "role_id": int(role_id),
                "company": employer_name,
                "title": title,
                "location": location,
                "role_family": role_family,
                "match_score": float(match_score or 0.0),
                "posted_at": posted_at,
                "first_seen_at": first_seen_at,
                "last_seen_at": last_seen_at,
                "status": status,
                "apply_url": apply_url,
                "source_type": source_type,
                "group": int(grp),
            }
        )
    return out

def build_company_rankings(active_roles: list[dict], top_n: int = 10) -> list[dict]:
    counts: dict[str, int] = {}
    for r in active_roles:
        counts[r["company"]] = counts.get(r["company"], 0) + 1
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:top_n]
    return [{"company": c, "count": n} for c, n in ranked]


def build_group_summary(con: sqlite3.Connection) -> dict:
    # Company counts per group
    counts = {1: 0, 2: 0}
    try:
        for g, n in con.execute(
            'SELECT "group", COUNT(*) FROM company_classification GROUP BY "group"'
        ).fetchall():
            if g in counts:
                counts[int(g)] = int(n)
    except Exception:
        pass

    # Example company names per group
    examples = {"group1": [], "group2": []}
    for g in (1, 2):
        try:
            rows = con.execute(
                """
                SELECT c.employer_name
                FROM company_classification cl
                JOIN companies c ON c.company_id = cl.company_id
                WHERE cl."group" = ?
                ORDER BY c.employer_name
                LIMIT 6
                """,
                (g,),
            ).fetchall()
            examples[f"group{g}"] = [r[0] for r in rows]
        except Exception:
            examples[f"group{g}"] = []

    return {
        "group1": int(counts[1]),
        "group2": int(counts[2]),
        "examples": examples,
    }


def build_top_picks(active_roles: list[dict], limit: int = 25) -> list[dict]:
    """Select the best roles to apply to today.

    Heuristic:
    - Prefer group 1 + group 2
    - Sort by match_score desc, then most recent first_seen_at
    """
    picked = [r for r in active_roles if r.get("group") in (1, 2)]
    # Sort by score desc, then recency desc.
    picked.sort(
        key=lambda r: (
            float(r.get("match_score") or 0.0),
            str(r.get("first_seen_at") or ""),
        ),
        reverse=True,
    )
    return picked[:limit]


def main() -> None:
    con = sqlite3.connect(DB_PATH)
    try:
        last_run = get_last_run_ts(con)

        new_roles = fetch_roles(
            con,
            "WHERE r.first_seen_at >= datetime('now', ?)",
            (f"-{NEW_WINDOW_HOURS} hours",),
        )
        active_roles = fetch_roles(
            con,
            "WHERE r.status='active'",
            (),
        )

        rankings = build_company_rankings(active_roles)
        group_summary = build_group_summary(con)
        top_picks = build_top_picks(active_roles, limit=25)

        meta = {
            "last_run": last_run,
            "new_window_hours": NEW_WINDOW_HOURS,
            "counts": {
                "new_roles": len(new_roles),
                "active_roles": len(active_roles),
                "ranked_companies": len(rankings),
            },
        }

        write_json(OUT_DIR / "meta.json", meta)
        write_json(OUT_DIR / "new_roles.json", {"meta": meta, "roles": new_roles})
        write_json(OUT_DIR / "active_roles.json", {"meta": meta, "roles": active_roles})
        write_json(OUT_DIR / "company_rankings.json", {"meta": meta, "companies": rankings})
        write_json(OUT_DIR / "group_summary.json", group_summary)
        write_json(OUT_DIR / "top_picks.json", {"meta": meta, "roles": top_picks})

        print("✅ JSON exports complete.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
