import requests


def fetch_icims(company_slug: str) -> list[dict]:
    url = f"https://{company_slug}.icims.com/jobs/search"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return []

    try:
        data = r.json()
    except ValueError:
        return []

    jobs: list[dict] = []
    for j in data.get("jobs", []):
        jobs.append(
            {
                "company": company_slug,
                "job_title": j.get("title", ""),
                "location": j.get("location", ""),
                "remote_or_hybrid": "",
                "posting_date": j.get("postedDate", ""),
                "job_url": j.get("jobUrl", ""),
                "source": "icims",
            }
        )
    return jobs
