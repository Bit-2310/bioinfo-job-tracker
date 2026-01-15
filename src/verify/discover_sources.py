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
MAX_MINUTES = SETTINGS["verify"].get("max_discovery_minutes", 8)
LOG_EVERY = SETTINGS["verify"].get("log_every", 20)

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
    # Small pause to be polite and reduce rate-limit risk.
    r = get(url, timeout=TIMEOUT, retries=RETRIES, pause=0.0)
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

        # Cursor-based batching: we remember the last company_id we attempted,
        # so each scheduled run keeps moving forward and does not restart from the top.
        # If we reach the end, we wrap around.
        cur.execute(
            """CREATE TABLE IF NOT EXISTS state_kv (
                 k TEXT PRIMARY KEY,
                 v TEXT NOT NULL,
                 updated_at TEXT NOT NULL DEFAULT (datetime('now'))
               );"""
        )
        cur.execute("SELECT v FROM state_kv WHERE k='discover_cursor'")
        row = cur.fetchone()
        cursor = int(row[0]) if row and str(row[0]).isdigit() else 0

        def fetch_batch(after_company_id: int):
            cur.execute(
                """
                SELECT c.company_id, c.employer_name
                FROM companies c
                LEFT JOIN company_job_sources s ON s.company_id=c.company_id
                WHERE s.company_id IS NULL
                  AND c.company_id > ?
                ORDER BY c.company_id
                LIMIT ?
                """,
                (after_company_id, limit),
            )
            return cur.fetchall()

        companies = fetch_batch(cursor)
        if not companies:
            # Wrap around (run finished a full pass)
            cursor = 0
            companies = fetch_batch(cursor)

        inserted = 0
        processed = 0
        started = time.monotonic()
        last_company_id = cursor

        print(
            f"[discover] starting batch: limit={limit}, cursor={cursor}, max_minutes={MAX_MINUTES}, log_every={LOG_EVERY}",
            flush=True,
        )

        for company_id, employer_name in companies:
            processed += 1
            last_company_id = company_id

            # Hard stop so the job never runs forever.
            elapsed_min = (time.monotonic() - started) / 60.0
            if elapsed_min >= float(MAX_MINUTES):
                print(
                    f"[discover] time budget reached ({elapsed_min:.1f}m). stopping early.",
                    flush=True,
                )
                break

            if PAUSE:
                time.sleep(PAUSE)

            # Progress log (every N companies) so Actions doesn't look frozen.
            if processed == 1 or processed % int(LOG_EVERY) == 0 or processed == len(companies):
                print(
                    f"[discover] progress: {processed}/{len(companies)} | last_id={company_id} | inserted={inserted}",
                    flush=True,
                )

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

        # Persist cursor for next run.
        cur.execute(
            """INSERT INTO state_kv(k, v, updated_at)
                 VALUES('discover_cursor', ?, datetime('now'))
                 ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=datetime('now');""",
            (str(last_company_id),),
        )

        print(
            f"[discover] done: inserted={inserted}, processed={processed}, next_cursor={last_company_id}",
            flush=True,
        )

if __name__ == "__main__":
    main()
