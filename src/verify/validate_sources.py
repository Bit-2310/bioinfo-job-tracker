import yaml
from src.utils.db import connect, ensure_tables
from src.utils.http import get

SETTINGS = yaml.safe_load(open("src/config/settings.yml"))
DB_PATH = SETTINGS["db_path"]
TIMEOUT = SETTINGS["http_timeout_sec"]
RETRIES = SETTINGS["http_retries"]

def main(limit: int = 2000):
    with connect(DB_PATH) as con:
        ensure_tables(con)
        cur = con.cursor()

        cur.execute("""
            SELECT source_id, careers_url
            FROM company_job_sources
            WHERE is_active=1
            ORDER BY source_id
            LIMIT ?
        """, (limit,))
        rows = cur.fetchall()

        ok = 0
        bad = 0
        for source_id, url in rows:
            r = get(url, timeout=TIMEOUT, retries=RETRIES, pause=0.2)
            if not r or r.status_code >= 400:
                cur.execute(
                    "UPDATE company_job_sources SET is_active=0, last_checked_at=datetime('now') WHERE source_id=?",
                    (source_id,)
                )
                bad += 1
            else:
                cur.execute(
                    "UPDATE company_job_sources SET last_checked_at=datetime('now') WHERE source_id=?",
                    (source_id,)
                )
                ok += 1

        print(f"Validated ok={ok}, deactivated={bad}")

if __name__ == "__main__":
    main()
