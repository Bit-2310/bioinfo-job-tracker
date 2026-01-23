import requests


def fetch_greenhouse(board_token: str, company_name: str, target_role: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return []

    data = r.json()
    jobs: list[dict] = []

    for j in data.get("jobs", []):
        jobs.append(
            {
                "company": company_name,
                "target_role": target_role,
                "job_title": j.get("title", ""),
                "location": (j.get("location") or {}).get("name", "") if isinstance(j.get("location"), dict) else (j.get("location") or ""),
                "remote_or_hybrid": "",
                "posting_date": j.get("updated_at", ""),
                "job_url": j.get("absolute_url", ""),
                "source": "greenhouse",
            }
        )
    return jobs
