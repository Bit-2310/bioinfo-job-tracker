import requests
from datetime import datetime, timezone


def fetch_lever(company_slug: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return []

    jobs: list[dict] = []
    for j in r.json():
        created = j.get("createdAt")
        posting_date = ""
        if created:
            try:
                posting_date = datetime.fromtimestamp(created / 1000, tz=timezone.utc).date().isoformat()
            except Exception:
                posting_date = ""

        jobs.append(
            {
                "company": company_slug,
                "job_title": j.get("text", ""),
                "location": (j.get("categories") or {}).get("location", ""),
                "remote_or_hybrid": "",
                "posting_date": posting_date,
                "job_url": j.get("hostedUrl", ""),
                "source": "lever",
            }
        )
    return jobs
