import re
import random
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Tuple

import yaml

from src.utils.db import connect, ensure_tables
from src.utils.http import get
from src.utils.url import canonicalize_url

SETTINGS = yaml.safe_load(open("src/config/settings.yml"))
KW = yaml.safe_load(open("src/config/keywords.yml"))

DB_PATH = SETTINGS["db_path"]
TIMEOUT = SETTINGS["http_timeout_sec"]
RETRIES = SETTINGS["http_retries"]
CLOSE_AFTER_DAYS = SETTINGS["track"]["mark_closed_after_days_not_seen"]
MAX_SOURCES_PER_RUN = int(SETTINGS["track"].get("max_sources_per_run", 250))
GROUP2_RESCAN_HOURS = int(SETTINGS["track"].get("group2_rescan_hours", 24))

GREENHOUSE_BOARD_RE = re.compile(r"boards\.greenhouse\.io/([^/?#]+)", re.I)
LEVER_COMPANY_RE = re.compile(r"jobs\.lever\.co/([^/?#]+)", re.I)

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def role_family(title: str, desc: str) -> str:
    t = (title or "").lower()
    d = (desc or "").lower()
    rules = KW.get("role_family_rules", {})
    for fam, cfg in rules.items():
        if any(k.lower() in t for k in cfg.get("title_any", [])):
            return fam
        if any(k.lower() in d for k in cfg.get("desc_any", [])):
            return fam
    return "other"

def score(title: str, desc: str, fam: str) -> float:
    t = (title or "").lower()
    d = (desc or "").lower()
    s = 0.0
    boost = KW.get("score_rules", {}).get("title_boost", {})
    s += float(boost.get(fam, 0))
    for k, w in KW.get("score_rules", {}).get("desc_positive", []):
        if k.lower() in d:
            s += float(w)
    for k, w in KW.get("score_rules", {}).get("desc_negative", []):
        if k.lower() in d:
            s += float(w)
    if "bioinformatics" in t: s += 2
    if "genomics" in t or "genomic" in t: s += 1
    return s

def fetch_greenhouse(url: str):
    m = GREENHOUSE_BOARD_RE.search(url or "")
    if not m: return []
    board = m.group(1)
    api = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
    r = get(api, timeout=TIMEOUT, retries=RETRIES)
    if not r or r.status_code != 200: return []
    data = r.json()
    out = []
    for j in data.get("jobs", []):
        out.append({
            "source_job_id": str(j.get("id")) if j.get("id") is not None else None,
            "title": j.get("title"),
            "location": (j.get("location") or {}).get("name"),
            "apply_url": j.get("absolute_url"),
            "posted_at": j.get("updated_at") or j.get("created_at"),
            "description": j.get("content") or ""
        })
    return out

def fetch_lever(url: str):
    m = LEVER_COMPANY_RE.search(url or "")
    if not m: return []
    company = m.group(1)
    api = f"https://api.lever.co/v0/postings/{company}?mode=json"
    r = get(api, timeout=TIMEOUT, retries=RETRIES)
    if not r or r.status_code != 200: return []
    out = []
    for j in r.json():
        desc = j.get("descriptionPlain") or j.get("description") or ""
        created_ms = j.get("createdAt")
        posted_at = None
        if isinstance(created_ms, (int, float)):
            posted_at = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc).replace(microsecond=0).isoformat()
        out.append({
            "source_job_id": str(j.get("id")) if j.get("id") is not None else None,
            "title": j.get("text"),
            "location": (j.get("categories") or {}).get("location"),
            "apply_url": j.get("hostedUrl") or j.get("applyUrl"),
            "posted_at": posted_at,
            "description": desc
        })
    return out


def _pick_sources(cur) -> List[Tuple[int, int, str, str, int, str]]:
    """Pick sources to scan based on priority group.

    - Group 1: always scan
    - Group 2: scan if not checked in GROUP2_RESCAN_HOURS
    - Group 3+: ignored (not part of the curated target universe)
    """
    cur.execute(
        """
        SELECT s.source_id, s.company_id, s.source_type, s.careers_url,
               cl.`group` AS grp,
               s.last_checked_at
        FROM company_job_sources s
        JOIN company_classification cl ON cl.company_id = s.company_id
        WHERE s.is_active=1
          AND cl.`group` IN (1, 2)
        ORDER BY grp ASC
        """
    )
    rows = cur.fetchall()

    group1 = []
    group2 = []
    cutoff_g2 = datetime.now(timezone.utc) - timedelta(hours=GROUP2_RESCAN_HOURS)
    for source_id, company_id, stype, url, grp, last_checked_at in rows:
        if grp == 1:
            group1.append((source_id, company_id, stype, url, grp, last_checked_at))
        elif grp == 2:
            eligible = True
            if last_checked_at:
                try:
                    ts = datetime.fromisoformat(last_checked_at.replace("Z", "+00:00"))
                    eligible = ts <= cutoff_g2
                except Exception:
                    eligible = True
            if eligible:
                group2.append((source_id, company_id, stype, url, grp, last_checked_at))
    picked = group1 + group2
    return picked[:MAX_SOURCES_PER_RUN]

def main():
    started = now_iso()
    companies_checked = 0
    roles_seen = 0
    new_roles = 0
    updated_roles = 0

    with connect(DB_PATH) as con:
        ensure_tables(con)
        cur = con.cursor()
        cur.execute("INSERT INTO runs (started_at) VALUES (?)", (started,))
        run_id = cur.lastrowid

        sources = _pick_sources(cur)
        now = now_iso()

        successful_companies = set()

        for source_id, company_id, stype, url, grp, last_checked_at in sources:
            companies_checked += 1

            source_started = now_iso()
            cur.execute(
                """INSERT INTO source_runs (run_id, source_id, company_id, source_type, started_at, status)
                   VALUES (?, ?, ?, ?, ?, 'running')""",
                (run_id, source_id, company_id, stype, source_started),
            )
            source_run_id = cur.lastrowid

            jobs: List[Dict[str, Any]] = []
            source_error = None
            try:
                if stype == "greenhouse":
                    jobs = fetch_greenhouse(url)
                elif stype == "lever":
                    jobs = fetch_lever(url)
                else:
                    jobs = []
            except Exception as e:
                source_error = str(e)
                jobs = []

            s_roles_seen = 0
            s_new = 0
            s_updated = 0

            for j in jobs:
                roles_seen += 1
                s_roles_seen += 1
                apply_url = j.get("apply_url")
                if not apply_url:
                    continue

                apply_url_canon = canonicalize_url(apply_url)

                title = j.get("title") or ""
                loc = j.get("location") or ""
                desc = j.get("description") or ""

                fam = role_family(title, desc)
                ms = score(title, desc, fam)

                source_job_id = j.get("source_job_id")

                # Prefer stable id when available
                if source_job_id:
                    cur.execute(
                        "SELECT role_id FROM roles WHERE company_id=? AND source_type=? AND source_job_id=?",
                        (company_id, stype, str(source_job_id)),
                    )
                else:
                    cur.execute(
                        "SELECT role_id FROM roles WHERE company_id=? AND apply_url_canonical=?",
                        (company_id, apply_url_canon),
                    )
                ex = cur.fetchone()

                if ex:
                    cur.execute("""
                        UPDATE roles
                        SET title=?, location=?, role_family=?, match_score=?,
                            posted_at=COALESCE(?, posted_at),
                            last_seen_at=?, status='active',
                            apply_url=?, apply_url_canonical=?, source_job_id=?,
                            source_type=?, source_id=?, description=?
                        WHERE role_id=?
                    """, (title, loc, fam, ms, j.get("posted_at"), now, apply_url, apply_url_canon, source_job_id, stype, source_id, desc, ex[0]))
                    updated_roles += 1
                    s_updated += 1
                else:
                    cur.execute("""
                        INSERT INTO roles (
                          company_id, title, location, role_family, match_score,
                          posted_at, posted_date, first_seen_at, last_seen_at, status,
                          apply_url, apply_url_canonical, source_job_id,
                          source_type, source_id, description
                        ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, 'active', ?, ?, ?, ?, ?, ?)
                    """, (company_id, title, loc, fam, ms, j.get("posted_at"), now, now, apply_url, apply_url_canon, source_job_id, stype, source_id, desc))
                    new_roles += 1
                    s_new += 1

            # Mark source run outcome
            status = 'success' if source_error is None else 'fail'
            cur.execute(
                """UPDATE source_runs
                   SET finished_at=?, status=?, roles_seen=?, new_roles=?, updated_roles=?, error=?
                   WHERE source_run_id=?""",
                (now_iso(), status, s_roles_seen, s_new, s_updated, source_error, source_run_id),
            )

            if status == 'success':
                successful_companies.add(company_id)
                cur.execute(
                    "UPDATE company_job_sources SET last_checked_at=? WHERE source_id=?",
                    (now_iso(), source_id),
                )

        # Close stale roles ONLY for companies we successfully scanned this run
        closed_roles = 0
        if successful_companies:
            placeholders = ",".join(["?"] * len(successful_companies))
            params = list(successful_companies) + [CLOSE_AFTER_DAYS]
            cur.execute(
                f"""
                UPDATE roles
                SET status='closed'
                WHERE status='active'
                  AND company_id IN ({placeholders})
                  AND julianday('now') - julianday(last_seen_at) > ?
                """,
                params,
            )
            closed_roles = cur.rowcount

        cur.execute("""
            UPDATE runs
            SET finished_at=?, companies_checked=?, roles_seen=?, new_roles=?, updated_roles=?, closed_roles=?
            WHERE run_id=?
        """, (now_iso(), companies_checked, roles_seen, new_roles, updated_roles, closed_roles, run_id))

    print(f"companies_checked={companies_checked} roles_seen={roles_seen} new={new_roles} updated={updated_roles} closed={closed_roles}")

if __name__ == "__main__":
    main()
