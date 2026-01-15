import re
import time
from urllib.parse import quote

from bs4 import BeautifulSoup
import yaml

from src.utils.db import connect, ensure_tables
from src.utils.http import get

SETTINGS = yaml.safe_load(open("src/config/settings.yml"))

DB_PATH = SETTINGS["db_path"]
TIMEOUT = SETTINGS["http_timeout_sec"]
RETRIES = SETTINGS["http_retries"]
MAX_RESULTS = SETTINGS["verify"]["max_search_results"]
PAUSE = SETTINGS["verify"]["per_company_pause_sec"]
LIMIT_DEFAULT = SETTINGS["verify"]["limit_default"]

GREENHOUSE_RE = re.compile(r"(boards\.greenhouse\.io|api\.greenhouse\.io)", re.I)
LEVER_RE = re.compile(r"(jobs\.lever\.co|api\.lever\.co)", re.I)
WORKDAY_RE = re.compile(r"(myworkdayjobs\.com)", re.I)

def detect_source_type(url: str) -> str:
    u = (url or "").lower()
    if GREENHOUSE_RE.search(u): return "greenhouse"
    if LEVER_RE.search(u): return "lever"
    if WORKDAY_RE.search(u): return "workday"
    return "custom"

def ddg_search(query: str) -> list[str]:
    q = quote(query)
    url = f"https://duckduckgo.com/html/?q={q}"
    r = get(url, timeout=TIMEOUT, retries=RETRIES)
    if not r or r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, "lxml")
    links = []
    for a in soup.select("a.result__a"):
        href = a.get("href")
        if href:
            links.append(href)
    # de-dupe
    out, seen = [], set()
    for l in links:
        if l not in seen:
            seen.add(l)
            out.append(l)
    return out[:MAX_RESULTS]

def choose_best(links: list[str]) -> str | None:
    # Prefer known ATS pages
    for l in links:
        low = l.lower()
        if "boards.greenhouse.io" in low or "jobs.lever.co" in low or "myworkdayjobs.com" in low:
            return l
    # Otherwise prefer careers/jobs pages
    for l in links:
        low = l.lower()
        if any(k in low for k in ["/careers", "/jobs", "careers.", "jobs."]):
            return l
    return links[0] if links else None

def main(limit: int = LIMIT_DEFAULT):
    with connect(DB_PATH) as con:
        ensure_tables(con)
        cur = con.cursor()

        # Only discover for companies that currently have no sources
        cur.execute("""
            SELECT c.company_id, c.employer_name
            FROM companies c
            LEFT JOIN company_job_sources s ON s.company_id=c.company_id
            WHERE s.company_id IS NULL
            ORDER BY c.company_id
            LIMIT ?
        """, (limit,))
        companies = cur.fetchall()

        inserted = 0
        for company_id, employer_name in companies:
            time.sleep(PAUSE)

            links = ddg_search(f"{employer_name} careers jobs greenhouse lever workday")
            best = choose_best(links)
            if not best:
                continue

            stype = detect_source_type(best)
            cur.execute("""
                INSERT INTO company_job_sources (company_id, source_type, careers_url, is_active, created_at)
                VALUES (?, ?, ?, 1, datetime('now'))
                ON CONFLICT(company_id, careers_url) DO UPDATE SET
                  source_type=excluded.source_type,
                  is_active=1
            """, (company_id, stype, best))
            inserted += 1

        print(f"Inserted/updated sources: {inserted} / attempted {len(companies)}")

if __name__ == "__main__":
    main()
