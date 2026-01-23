import hashlib
import re
from urllib.parse import urlparse


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower().strip())


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")


def compute_canonical_job_id(company: str, job_title: str, location: str, job_url: str) -> str:
    raw = (
        normalize(company)
        + "|"
        + normalize(job_title)
        + "|"
        + normalize(location)
        + "|"
        + canonicalize_url(job_url)
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
