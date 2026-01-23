import requests


def fetch_greenhouse(company_slug: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return []

    data = r.json()
    jobs: list[dict] = []

    for j in data.get("jobs", []):
        jobs.append(
            {
                "company": company_slug,
                "job_title": j.get("title", ""),
                "location": (j.get("location") or {}).get("name", "") if isinstance(j.get("location"), dict) else (j.get("location") or ""),
                "remote_or_hybrid": "",
                "posting_date": j.get("updated_at", ""),
                "job_url": j.get("absolute_url", ""),
                "source": "greenhouse",
            }
        )
    return jobs
