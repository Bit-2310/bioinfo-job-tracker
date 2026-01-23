from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = {
    "Company Name": "company",
    "Target Role Title": "target_role",
    "Careers Page URL": "careers_url",
}


def load_targets(xlsx_path: Path) -> pd.DataFrame:
    """Load the canonical target list.

    Expects columns:
      - Company Name
      - Target Role Title
      - Careers Page URL
    """

    df = pd.read_excel(xlsx_path)
    missing = [c for c in REQUIRED_COLUMNS.keys() if c not in df.columns]
    if missing:
        raise ValueError(
            "Target list is missing required columns: " + ", ".join(missing)
        )

    df = df[list(REQUIRED_COLUMNS.keys())].rename(columns=REQUIRED_COLUMNS)
    df["company"] = df["company"].astype(str).str.strip()
    df["target_role"] = df["target_role"].astype(str).str.strip()
    df["careers_url"] = df["careers_url"].astype(str).str.strip()

    df = df.dropna(subset=["company", "target_role", "careers_url"]).copy()
    df = df.loc[(df["company"] != "") & (df["target_role"] != "") & (df["careers_url"] != "")]
    df = df.drop_duplicates().reset_index(drop=True)
    return df
