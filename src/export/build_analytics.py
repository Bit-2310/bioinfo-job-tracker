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
    """Build a compact summary of the most recent pipeline run.

    The DB schema stores run start/end timestamps but does not include a
    dedicated status column. We infer status based on whether finished_at
    is NULL.

    Active roles are stored using roles.status='active' (not an is_active
    boolean).
    """

    last_run = None
    try:
        row = con.execute(
            "SELECT started_at, finished_at FROM runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        if row:
            started_at, finished_at = row
            status = "running" if finished_at is None else "success"
            last_run = {
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
            }
    except Exception:
        last_run = None

    roles_total = con.execute("SELECT COUNT(*) FROM roles").fetchone()[0]
    active_total = con.execute(
        "SELECT COUNT(*) FROM roles WHERE status='active'"
    ).fetchone()[0]
    companies_ranked = con.execute(
        "SELECT COUNT(DISTINCT company_id) FROM roles"
    ).fetchone()[0]

    # Per-source telemetry for the last run (if present)
    src_stats = {
        "sources_success": 0,
        "sources_fail": 0,
        "roles_seen": 0,
        "new_roles": 0,
        "updated_roles": 0,
    }
    try:
        row = con.execute("SELECT run_id FROM runs ORDER BY run_id DESC LIMIT 1").fetchone()
        if row:
            last_run_id = int(row[0])
            s = con.execute(
                """
                SELECT
                  SUM(CASE WHEN status='success' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='fail' THEN 1 ELSE 0 END),
                  SUM(roles_seen),
                  SUM(new_roles),
                  SUM(updated_roles)
                FROM source_runs
                WHERE run_id=?
                """,
                (last_run_id,),
            ).fetchone()
            if s:
                src_stats = {
                    "sources_success": int(s[0] or 0),
                    "sources_fail": int(s[1] or 0),
                    "roles_seen": int(s[2] or 0),
                    "new_roles": int(s[3] or 0),
                    "updated_roles": int(s[4] or 0),
                }
    except Exception:
        pass

    return {
        "generated_at": utc_now_iso(),
        "last_run": last_run,
        "counts": {
            "roles_total": int(roles_total),
            "active_roles": int(active_total),
            "ranked_companies": int(companies_ranked),
        },
        "sources": src_stats,
    }


def compute_company_priority(con) -> list[dict]:
    rows = con.execute(
        """
        SELECT
          c.employer_name,
          SUM(CASE WHEN r.first_seen_at >= datetime('now','-7 days') THEN 1 ELSE 0 END) AS new_7d,
          SUM(CASE WHEN r.status='active' THEN 1 ELSE 0 END) AS active_roles,
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
        score = (new_7d * 5) + (active_roles * 2) + (avg_match * 1)

        if new_7d >= 3:
            tier, label = "Tier 1", "Hot"
        elif new_7d >= 1:
            tier, label = "Tier 2", "Warm"
        elif active_roles > 0:
            tier, label = "Tier 3", "Cold"
        else:
            tier, label = "Tier 4", "Unknown"

        out.append({
            "company": name,
            "new_roles_7d": new_7d,
            "active_roles": active_roles,
            "avg_match_score": round(avg_match, 3),
            "score": round(score, 3),
            "tier": tier,
            "label": label,
        })

    out.sort(key=lambda x: (x["tier"], -x["score"], x["company"] or ""))
    return out


def compute_group_analytics(con) -> dict:
    counts = {1: 0, 2: 0, 3: 0}
    roles_by_group = {1: 0, 2: 0, 3: 0}

    for g, n in con.execute(
        'SELECT "group", COUNT(*) FROM company_classification GROUP BY "group"'
    ):
        if g in counts:
            counts[int(g)] = int(n)

    for g, n in con.execute(
        """
        SELECT cl."group", COUNT(*)
        FROM roles r
        JOIN company_classification cl ON r.company_id = cl.company_id
        WHERE r.status='active'
        GROUP BY cl."group"
        """
    ):
        if g in roles_by_group:
            roles_by_group[int(g)] = int(n)

    return {
        "generated_at": utc_now_iso(),
        "group_counts": {f"group{g}": counts[g] for g in counts},
        "roles_by_group": {f"group{g}": roles_by_group[g] for g in roles_by_group},
    }


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
        group_data = compute_group_analytics(con)

    write_json(OUT_DIR / "source_analytics.json", asdict(source_ana))
    write_json(OUT_DIR / "run_summary.json", run_summary)
    write_json(OUT_DIR / "company_priority.json", {"generated_at": utc_now_iso(), "companies": priority})
    # "visa_group_analytics.json" is kept for backward compatibility with older dashboards.
    # Backward compatible filename (older dashboards expected this)
    write_json(OUT_DIR / "visa_group_analytics.json", group_data)
    # Preferred name
    write_json(OUT_DIR / "priority_group_analytics.json", group_data)
    write_json(OUT_DIR / "priority_group_analytics.json", group_data)

    print("[analytics] wrote all summary files including visa_group_analytics.json")


if __name__ == "__main__":
    main()
