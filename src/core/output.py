from pathlib import Path
import pandas as pd


def write_latest(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)
