import re
import time
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote

from bs4 import BeautifulSoup
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils.db import connect, ensure_tables
from src.utils.http import get

SETTINGS = yaml.safe_load(open("src/config/settings.yml"))

DB_PATH = SETTINGS["db_path"]
TIMEOUT = SETTINGS["http_timeout_sec"]
RETRIES = SETTINGS["http_retries"]

VERIFY = SETTINGS.get("verify", {})
MAX_RESULTS = int(VERIFY.get("max_search_results", 8))
PAUSE = float(VERIFY.get("per_company_pause_sec", 0.1))
LIMIT_DEFAULT = int(VERIFY.get("limit_default", 100))
LOG_EVERY = int(VERIFY.get("log_every", 20))
MAX_MINUTES = float(VERIFY.get("max_discovery_minutes", 8))
MAX_WORKERS = int(VERIFY.get("max_workers", 3))

GREENHOUSE_RE = re.compile(r"(boards\.greenhouse\.io|api\.greenhouse\.io)", re.I)
LEVER_RE = re.compile(r"(jobs\.lever\.co|api\.lever\.co)", re.I)
WORKDAY_RE = re.compile(r"(myworkdayjobs\.com)", re.I)

@dataclass
class DiscoverResult:
    company_id: int
    employer_name: str
    best_url: str | None
    source_type: str | None
    ok: bool
    reason: str | None = None

def detect_source_type(url: str) -> str:
    u = (url or "").lower()
    if GREENHOUSE_RE.search(u):
        return "greenhouse"
    if LEVER_RE.search(u):
        return "lever"
    if WORKDAY_RE.search(u):
        return "workday"
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

def discover_one(company_id: int, employer_name: str) -> DiscoverResult:
    try:
        # Polite pause per task (still applies even with threads)
        if PAUSE:
            time.sleep(PAUSE)

        links = ddg_search(f"{employer_name} careers jobs greenhouse lever workday")
        best = choose_best(links)
        if not best:
            return DiscoverResult(company_id, employer_name, None, None, False, "no_links")

        stype = detect_source_type(best)
        return DiscoverResult(company_id, employer_name, best, stype, True, None)
    except Exception as e:
        return DiscoverResult(company_id, employer_name, None, None, False, f"exception:{type(e).__name__}")

def ensure_state_table(con):
    con.execute(
        """CREATE TABLE IF NOT EXISTS state_kv (
          k TEXT PRIMARY KEY,
          v TEXT NOT NULL,
          updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );"""
    )

def get_cursor(cur) -> int:
    cur.execute("SELECT v FROM state_kv WHERE k='discover_cursor'")
    row = cur.fetchone()
    if not row:
        return 0
    try:
        return int(row[0])
    except Exception:
        return 0

def set_cursor(cur, v: int):
    cur.execute(
        """INSERT INTO state_kv (k, v, updated_at)
           VALUES ('discover_cursor', ?, datetime('now'))
           ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=datetime('now')""",
        (str(v),),
    )

def main(limit: int = LIMIT_DEFAULT):
    t0 = time.time()
    deadline = t0 + (MAX_MINUTES * 60)

    with connect(DB_PATH) as con:
        ensure_tables(con)
        ensure_state_table(con)
        cur = con.cursor()

        # Cursor batching: continue from last cursor
        cursor = get_cursor(cur)

        # Select next batch of companies that DON'T have sources, starting after cursor
        cur.execute(
            """
            SELECT company_id, employer_name
            FROM companies
            WHERE company_id > ?
              AND company_id NOT IN (SELECT company_id FROM company_job_sources)
            ORDER BY company_id
            LIMIT ?
            """,
            (cursor, limit),
        )
        batch = cur.fetchall()

        # If we hit the end, wrap around from 0
        if not batch:
            cur.execute(
                """
                SELECT company_id, employer_name
                FROM companies
                WHERE company_id NOT IN (SELECT company_id FROM company_job_sources)
                ORDER BY company_id
                LIMIT ?
                """,
                (limit,),
            )
            batch = cur.fetchall()
            cursor = 0

        print(
            f"[discover] start batch size={len(batch)} limit={limit} workers={MAX_WORKERS} "
            f"cursor={cursor} time_budget_min={MAX_MINUTES}",
            flush=True,
        )

        inserted = 0
        processed = 0
        last_company_id = cursor

        # Run network discovery in threads, write results in main thread (safe for sqlite)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(discover_one, cid, name) for cid, name in batch]

            for fut in as_completed(futures):
                if time.time() > deadline:
                    print("[discover] time budget reached, stopping early", flush=True)
                    break

                res = fut.result()
                processed += 1
                last_company_id = max(last_company_id, res.company_id)

                if res.ok and res.best_url and res.source_type:
                    cur.execute(
                        """
                        INSERT INTO company_job_sources (company_id, source_type, careers_url, is_active, created_at)
                        VALUES (?, ?, ?, 1, datetime('now'))
                        ON CONFLICT(company_id, careers_url) DO UPDATE SET
                          source_type=excluded.source_type,
                          is_active=1
                        """,
                        (res.company_id, res.source_type, res.best_url),
                    )
                    inserted += 1

                if processed % LOG_EVERY == 0:
                    elapsed = time.time() - t0
                    print(
                        f"[discover] progress {processed}/{len(batch)} inserted={inserted} "
                        f"last_id={last_company_id} elapsed_sec={int(elapsed)}",
                        flush=True,
                    )

        # Update cursor to the last seen company id so next run continues
        set_cursor(cur, last_company_id)

        elapsed = time.time() - t0
        print(
            f"[discover] done processed={processed} inserted={inserted} next_cursor={last_company_id} "
            f"elapsed_sec={int(elapsed)}",
            flush=True,
        )

if __name__ == "__main__":
    main()
