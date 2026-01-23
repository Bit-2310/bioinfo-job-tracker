from __future__ import annotations

import re


US_STATE_ABBRS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY",
    "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
    "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}


DEFAULT_KEYWORDS = {
    # core
    "bioinformatics",
    "computational biology",
    "computational biologist",
    "genomics",
    "computational genomics",
    "genomic",
    "omics",
    "transcriptomics",
    "single cell",
    "scRNA",
    "rna-seq",
    "rnaseq",
    "ngs",
    "variant",
    "sequencing",
    # data
    "data scientist",
    "machine learning",
    "ml",
    "ai",
}


def is_us_location(location: str) -> bool:
    """Best-effort US-only filter.

    We accept:
    - 'Remote' (assumed US unless it explicitly says otherwise)
    - Strings mentioning 'United States' / 'USA'
    - City, ST patterns ('Boston, MA', 'San Francisco, CA')
    - Common 'US Remote' variants
    """

    loc = (location or "").strip()
    if not loc:
        return False

    l = loc.lower()
    if "remote" in l:
        # Exclude explicit non-US
        if any(x in l for x in ["uk", "united kingdom", "canada", "india", "europe", "emea", "apac"]):
            return False
        return True

    if "united states" in l or "usa" in l or "u.s." in l or "us" == l:
        return True

    # City, ST (two-letter) heuristic
    m = re.search(r"\b([A-Z]{2})\b", loc)
    if m and m.group(1) in US_STATE_ABBRS:
        return True

    # Some ATS use 'US' / 'U.S.' without the full phrase
    if re.search(r"\bU\.?S\.?\b", loc):
        return True

    return False


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def title_matches_targets(job_title: str, target_role: str) -> bool:
    """Filter job titles to likely matches.

    Strategy:
    - Always allow if title contains the exact target role tokens (loosely).
    - Otherwise allow if it hits any DEFAULT_KEYWORDS.
    """

    t = _normalize(job_title)
    tr = _normalize(target_role)

    if not t:
        return False

    # Loose target-role match (all meaningful words must appear)
    role_words = [w for w in re.split(r"[^a-z0-9]+", tr) if len(w) >= 4]
    if role_words and all(w in t for w in role_words):
        return True

    # Keyword fall-back
    for kw in DEFAULT_KEYWORDS:
        if kw in t:
            return True

    return False
