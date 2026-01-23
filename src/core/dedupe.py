from datetime import datetime, timezone


def process_job(job: dict, history_df):
    """
    job must contain:
      canonical_job_id
      company
      job_title
      location
      remote_or_hybrid
      posting_date
      job_url
      source
    """

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    existing = history_df[history_df["canonical_job_id"] == job["canonical_job_id"]]

    if existing.empty:
        return "new", {
            "canonical_job_id": job["canonical_job_id"],
            "company": job.get("company", ""),
            "target_role": job.get("target_role", ""),
            "job_title": job.get("job_title", ""),
            "location": job.get("location", ""),
            "remote_or_hybrid": job.get("remote_or_hybrid", ""),
            "posting_date": job.get("posting_date", ""),
            "job_url": job.get("job_url", ""),
            "first_seen": now,
            "last_seen": now,
            "sources_seen": job.get("source", ""),
        }

    if len(existing) != 1:
        raise RuntimeError(
            f"History corruption: {len(existing)} rows for canonical_job_id={job['canonical_job_id']}"
        )

    idx = existing.index[0]
    history_df.at[idx, "last_seen"] = now

    sources = set(str(history_df.at[idx, "sources_seen"]).split("|"))
    sources.add(job.get("source", ""))
    sources.discard("")
    history_df.at[idx, "sources_seen"] = "|".join(sorted(sources))

    return "duplicate", None
