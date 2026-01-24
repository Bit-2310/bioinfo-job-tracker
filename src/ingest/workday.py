import requests


def fetch_workday(host: str, tenant: str, site: str) -> list[dict]:
    """
    Fetch jobs from a Workday tenant using the common public "cxs" endpoint.

    Endpoint:
      https://{host}/wday/cxs/{tenant}/{site}/jobs

    Notes:
    - Workday payload structure varies slightly by tenant, but usually includes "jobPostings".
    - This function normalizes fields to the standard schema used by the pipeline.
    """
    if not (host and tenant and site):
        return []

    # jobs.myworkday.com is not reliably supported by this endpoint.
    if host.lower() == "jobs.myworkday.com":
        return []

    url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
    r = requests.get(url, timeout=30, headers={"Accept": "application/json"})
    if r.status_code != 200:
        return []

    try:
        data = r.json()
    except Exception:
        return []

    postings = data.get("jobPostings") or data.get("items") or []
    jobs: list[dict] = []

    for j in postings:
        # Typical Workday fields:
        # - title
        # - externalPath or externalUrl
        # - locationsText or locations
        title = j.get("title") or j.get("jobTitle") or ""
        loc = j.get("locationsText") or j.get("location") or ""
        posted = j.get("postedOn") or j.get("postedDate") or j.get("startDate") or ""
        url_path = j.get("externalPath") or ""
        ext_url = j.get("externalUrl") or ""
        if not ext_url and url_path:
            # externalPath often begins with /...; join to host
            ext_url = f"https://{host}{url_path}"

        jobs.append(
            {
                "company": tenant,
                "job_title": title,
                "location": loc,
                "remote_or_hybrid": "",
                "posting_date": posted,
                "job_url": ext_url,
                "source": "workday",
            }
        )

    return jobs
