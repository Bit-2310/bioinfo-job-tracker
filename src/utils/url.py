from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode


DROP_QUERY_PREFIXES = (
    "utm_",
)

DROP_QUERY_KEYS = {
    "gh_src",
    "lever-source",
    "lever-source[]",
    "source",
    "ref",
}


def canonicalize_url(url: str) -> str:
    """Return a stable URL for deduping.

    Removes common tracking query params and normalizes trivial variants.
    """
    if not url:
        return ""
    parts = urlsplit(url.strip())
    # Drop fragments
    fragment = ""

    # Filter query params
    kept = []
    for k, v in parse_qsl(parts.query, keep_blank_values=True):
        kl = (k or "").lower()
        if kl in DROP_QUERY_KEYS:
            continue
        if any(kl.startswith(p) for p in DROP_QUERY_PREFIXES):
            continue
        kept.append((k, v))
    query = urlencode(kept, doseq=True)

    # Normalize path: remove trailing slash except root
    path = parts.path or ""
    if path.endswith("/") and path != "/":
        path = path[:-1]

    return urlunsplit((parts.scheme, parts.netloc, path, query, fragment))
