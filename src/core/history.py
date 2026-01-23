from pathlib import Path
import pandas as pd


HISTORY_COLUMNS = [
    "canonical_job_id",
    "company",
    "target_role",
    "job_title",
    "location",
    "remote_or_hybrid",
    "posting_date",
    "job_url",
    "first_seen",
    "last_seen",
    "sources_seen",
]


def load_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=HISTORY_COLUMNS)
    df = pd.read_csv(path)
    for col in HISTORY_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[HISTORY_COLUMNS]


def save_history(path: Path, df: pd.DataFrame):
    # atomic write to prevent partial/corrupt history on crash
    tmp = path.with_suffix(".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)
