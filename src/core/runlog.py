from pathlib import Path
from datetime import datetime, timezone


def log_line(path: Path, line: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{ts}Z {line}\n")
