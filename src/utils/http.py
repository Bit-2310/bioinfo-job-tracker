import time
from typing import Optional

import requests


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BioinfoJobTracker/1.0)"
}


def get(
    url: str,
    timeout: int = 15,
    retries: int = 2,
    pause: float = 0.0,
) -> Optional[requests.Response]:
    """Small, polite HTTP helper.

    - Adds a stable User-Agent
    - Retries a couple times for flaky networks
    - Optional pause to reduce rate-limit risk
    """

    for i in range(retries + 1):
        try:
            if pause:
                time.sleep(pause)
            return requests.get(url, headers=HEADERS, timeout=timeout)
        except Exception:
            # Backoff: 0.6s, 1.2s, 1.8s...
            time.sleep(0.6 * (i + 1))
    return None
