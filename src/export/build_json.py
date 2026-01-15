import os, json
from datetime import datetime, timezone, timedelta
import yaml

from src.utils.db import connect, ensure_tables

SETTINGS = yaml.safe_load(open("src/config/settings.yml"))
DB_PATH = SETTINGS["db_path"]
HOURS = SETTINGS["export"]["new_window_hours"]

def iso(dt):
    return dt.replace(microsecond=0).isoformat()

def main():
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=HOURS)

    with connect(DB_PATH) as con:
        ensure_tables(con)
        cur = con.cursor()

        cur.execute("""
          SELECT c.employer_name, r.title, r.location, r.first_seen_at, r.posted_at, r.apply_url, r.role_family, r.match_score, r.source_type
          FROM roles r JOIN companies c ON c.company_id=r.company_id
          WHERE r.first_seen_at >= ?
          ORDER BY r.first_seen_at DESC
          LIMIT 1500
        """, (iso(start),))
        new_rows = cur.fetchall()

        cur.execute("""
          SELECT c.employer_name, r.title, r.location, r.first_seen_at, r.posted_at, r.apply_url, r.role_family, r.match_score, r.source_type
          FROM roles r JOIN companies c ON c.company_id=r.company_id
          WHERE r.status='active'
          ORDER BY r.last_seen_at DESC
          LIMIT 8000
        """)
        active_rows = cur.fetchall()

        cur.execute("""
          SELECT c.employer_name,
                 SUM(CASE WHEN r.status='active' THEN 1 ELSE 0 END) as active_roles,
                 SUM(CASE WHEN r.first_seen_at >= ? THEN 1 ELSE 0 END) as new_roles_24h,
                 AVG(r.match_score) as avg_score
          FROM companies c
          LEFT JOIN roles r ON r.company_id=c.company_id
          GROUP BY c.company_id
          HAVING active_roles > 0
          ORDER BY (new_roles_24h*5 + active_roles*2 + avg_score) DESC
          LIMIT 1000
        """, (iso(start),))
        rank_rows = cur.fetchall()

    new_roles=[{
        "company": a, "title": b, "location": (c_loc or ""),
        "first_seen": fs, "posted_at": pa, "apply_url": url,
        "role_family": fam, "match_score": float(ms), "source": src
    } for (a,b,c_loc,fs,pa,url,fam,ms,src) in new_rows]

    active_roles=[{
        "company": a, "title": b, "location": (c_loc or ""),
        "first_seen": fs, "posted_at": pa, "apply_url": url,
        "role_family": fam, "match_score": float(ms), "source": src
    } for (a,b,c_loc,fs,pa,url,fam,ms,src) in active_rows]

    rankings=[]
    rnk=1
    for name, active_cnt, new_cnt, avg_sc in rank_rows:
        sc = float(new_cnt)*5 + float(active_cnt)*2 + (float(avg_sc) if avg_sc is not None else 0.0)
        rankings.append({
            "rank": rnk,
            "company": name,
            "score": round(sc, 2),
            "active_roles": int(active_cnt or 0),
            "new_roles_24h": int(new_cnt or 0),
            "avg_match_score": round(float(avg_sc or 0.0), 2)
        })
        rnk += 1

    meta = {
        "last_run": iso(now),
        "new_window_hours": HOURS,
        "counts": {"new_roles": len(new_roles), "active_roles": len(active_roles), "ranked_companies": len(rankings)}
    }

    os.makedirs("site/data", exist_ok=True)
    json.dump({"meta": meta, "roles": new_roles}, open("site/data/new_roles.json","w"), indent=2)
    json.dump({"meta": meta, "roles": active_roles}, open("site/data/active_roles.json","w"), indent=2)
    json.dump({"meta": meta, "companies": rankings}, open("site/data/company_rankings.json","w"), indent=2)
    json.dump(meta, open("site/data/meta.json","w"), indent=2)

if __name__ == "__main__":
    main()
