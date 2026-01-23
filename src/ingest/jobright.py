import requests

JOBRIGHT_ENDPOINT = "https://api.jobright.ai/jobs/search"

KEYWORDS = [
    "bioinformatics",
    "computational biology",
    "genomics",
    "single cell",
    "rna-seq",
    "ngs",
    "omics",
]


def fetch_jobright_jobs(api_key: str) -> list[dict]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    jobs: list[dict] = []

    for kw in KEYWORDS:
        r = requests.get(
            JOBRIGHT_ENDPOINT,
            headers=headers,
            params={"query": kw, "limit": 100},
            timeout=30,
        )
        if r.status_code != 200:
            continue

        data = r.json()

        for j in data.get("jobs", []):
            title = (j.get("title") or "").strip()
            url = (j.get("url") or "").strip()
            if not title or not url:
                continue

            jobs.append(
                {
                    "company": (j.get("company") or "").strip(),
                    "job_title": title,
                    "location": (j.get("location") or "").strip(),
                    "remote_or_hybrid": (j.get("workplace_type") or "").strip(),
                    "posting_date": j.get("posted_at", ""),
                    "job_url": url,
                    "source": "jobright",
                }
            )

    return jobs
