import re
from datetime import datetime, timezone
import yaml

from src.utils.db import connect, ensure_tables
from src.utils.http import get

SETTINGS = yaml.safe_load(open("src/config/settings.yml"))
KW = yaml.safe_load(open("src/config/keywords.yml"))

DB_PATH = SETTINGS["db_path"]
TIMEOUT = SETTINGS["http_timeout_sec"]
RETRIES = SETTINGS["http_retries"]
CLOSE_AFTER_DAYS = SETTINGS["track"]["mark_closed_after_days_not_seen"]

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
        out.append({
            "title": j.get("text"),
            "location": (j.get("categories") or {}).get("location"),
            "apply_url": j.get("hostedUrl") or j.get("applyUrl"),
            "posted_at": j.get("createdAt"),
            "description": desc
        })
    return out

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

        cur.execute("""
            SELECT s.source_id, s.company_id, s.source_type, s.careers_url
            FROM company_job_sources s
            WHERE s.is_active=1
        """)
        sources = cur.fetchall()
        now = now_iso()

        for source_id, company_id, stype, url in sources:
            companies_checked += 1

            if stype == "greenhouse":
                jobs = fetch_greenhouse(url)
            elif stype == "lever":
                jobs = fetch_lever(url)
            else:
                # Workday/custom adapters can be added later
                continue

            for j in jobs:
                roles_seen += 1
                apply_url = j.get("apply_url")
                if not apply_url:
                    continue

                title = j.get("title") or ""
                loc = j.get("location") or ""
                desc = j.get("description") or ""

                fam = role_family(title, desc)
                ms = score(title, desc, fam)

                cur.execute("SELECT role_id FROM roles WHERE company_id=? AND apply_url=?", (company_id, apply_url))
                ex = cur.fetchone()

                if ex:
                    cur.execute("""
                        UPDATE roles
                        SET title=?, location=?, role_family=?, match_score=?,
                            posted_at=COALESCE(?, posted_at),
                            last_seen_at=?, status='active',
                            source_type=?, source_id=?, description=?
                        WHERE role_id=?
                    """, (title, loc, fam, ms, j.get("posted_at"), now, stype, source_id, desc, ex[0]))
                    updated_roles += 1
                else:
                    cur.execute("""
                        INSERT INTO roles (
                          company_id, title, location, role_family, match_score,
                          posted_at, posted_date, first_seen_at, last_seen_at, status,
                          apply_url, source_type, source_id, description
                        ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, 'active', ?, ?, ?, ?)
                    """, (company_id, title, loc, fam, ms, j.get("posted_at"), now, now, apply_url, stype, source_id, desc))
                    new_roles += 1

        # Close stale roles
        cur.execute("""
            UPDATE roles
            SET status='closed'
            WHERE status='active'
              AND julianday('now') - julianday(last_seen_at) > ?
        """, (CLOSE_AFTER_DAYS,))
        closed_roles = cur.rowcount

        cur.execute("""
            UPDATE runs
            SET finished_at=?, companies_checked=?, roles_seen=?, new_roles=?, updated_roles=?, closed_roles=?
            WHERE run_id=?
        """, (now_iso(), companies_checked, roles_seen, new_roles, updated_roles, closed_roles, run_id))

    print(f"companies_checked={companies_checked} roles_seen={roles_seen} new={new_roles} updated={updated_roles} closed={closed_roles}")

if __name__ == "__main__":
    main()
