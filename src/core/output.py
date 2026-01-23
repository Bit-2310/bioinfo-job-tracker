from pathlib import Path
import pandas as pd


def _to_datetime_series(s: pd.Series) -> pd.Series:
    # best-effort parse
    return pd.to_datetime(s, errors="coerce", utc=True)


def write_latest(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    if not df.empty:
        # Prefer true posting date; fall back to scrape date if missing.
        if "posting_date" in df.columns:
            df["_posting_dt"] = _to_datetime_series(df["posting_date"])
        else:
            df["_posting_dt"] = pd.NaT

        if "date_scraped" in df.columns:
            df["_scraped_dt"] = _to_datetime_series(df["date_scraped"])
        else:
            df["_scraped_dt"] = pd.NaT

        df["_sort_dt"] = df["_posting_dt"].fillna(df["_scraped_dt"])
        df = df.sort_values(["_sort_dt"], ascending=False).drop(columns=["_posting_dt", "_scraped_dt", "_sort_dt"], errors="ignore")
    df.to_csv(path, index=False)
