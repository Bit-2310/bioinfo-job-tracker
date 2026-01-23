import requests


def fetch_ashby(company_slug: str) -> list[dict]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company_slug}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return []

    jobs: list[dict] = []
    for j in r.json().get("jobs", []):
        jobs.append(
            {
                "company": company_slug,
                "job_title": j.get("title", ""),
                "location": j.get("location") or "",
                "remote_or_hybrid": "",
                "posting_date": j.get("publishedAt", ""),
                "job_url": j.get("jobUrl", ""),
                "source": "ashby",
            }
        )
    return jobs
