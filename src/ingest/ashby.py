import requests


def fetch_ashby(board_token: str, company_name: str, target_role: str) -> list[dict]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{board_token}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return []

    jobs: list[dict] = []
    for j in r.json().get("jobs", []):
        jobs.append(
            {
                "company": company_name,
                "target_role": target_role,
                "job_title": j.get("title", ""),
                "location": j.get("location") or "",
                "remote_or_hybrid": "",
                "posting_date": j.get("publishedAt", ""),
                "job_url": j.get("jobUrl", ""),
                "source": "ashby",
            }
        )
    return jobs
