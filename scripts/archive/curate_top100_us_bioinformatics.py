#!/usr/bin/env python3
"""Curate top 100 US bioinformatics/biotech companies from public sources."""

from __future__ import annotations

import argparse
import csv
import re
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup

USER_AGENT = "bioinfo-job-tracker/0.4"

BIOPHARMGUY_BIOINFO_URL = "https://biopharmguy.com/links/company-by-name-bioinformatics.php"
BIOPHARMGUY_BIOTECH_URL = "https://biopharmguy.com/links/company-by-name-biotech.php"
WIKIPEDIA_LARGEST_BIOMEDICAL_URL = (
    "https://en.wikipedia.org/wiki/List_of_largest_biomedical_companies_by_revenue"
)
WIKIPEDIA_LARGEST_PHARMA_URL = (
    "https://en.wikipedia.org/wiki/List_of_largest_pharmaceutical_companies"
)
BIOINFORMATICS_KEYWORDS = [
    "bioinformatics",
    "genomics",
    "computational",
    "sequencing",
    "omics",
    "genetic",
    "proteomics",
    "transcriptomics",
    "ngs",
]

US_STATES = set(
    "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT "
    "NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC"
    .split()
)


@dataclass
class SourceEntry:
    name: str
    source: str
    score: float


def normalize_name(value: str) -> str:
    value = value.lower().strip()
    value = value.replace("&", "and")
    value = re.sub(r"\b(the|inc|incorporated|corp|corporation|co|company|llc|ltd|plc|gmbh|ag|sa|sarl|bv|kg|lp|llp)\b", "", value)
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value


def clean_text(value: str) -> str:
    value = re.sub(r"\[[^\]]+\]", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def fetch_biopharmguy_us(timeout: int) -> list[SourceEntry]:
    resp = requests.get(
        BIOPHARMGUY_BIOINFO_URL, headers={"User-Agent": USER_AGENT}, timeout=timeout
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    entries = []
    for tr in soup.find_all("tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all("td")]
        if len(cols) < 2:
            continue
        name = cols[0].strip()
        location = cols[1].strip()
        if not name:
            continue
        state = location.split(" - ")[0].strip()
        if state in US_STATES or state in {"US", "USA", "United States"}:
            entries.append(SourceEntry(name=name, source="biopharmguy_bioinformatics", score=1.0))
    return entries


def fetch_biopharmguy_biotech_keywords(timeout: int) -> list[SourceEntry]:
    resp = requests.get(
        BIOPHARMGUY_BIOTECH_URL, headers={"User-Agent": USER_AGENT}, timeout=timeout
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    entries = []
    for tr in soup.find_all("tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all("td")]
        if len(cols) < 3:
            continue
        name, location, desc = cols[0].strip(), cols[1].strip(), cols[2].strip()
        if not name:
            continue
        state = location.split(" - ")[0].strip()
        if state not in US_STATES:
            continue
        desc_lower = desc.lower()
        if any(keyword in desc_lower for keyword in BIOINFORMATICS_KEYWORDS):
            entries.append(SourceEntry(name=name, source="biopharmguy_biotech_keywords", score=1.0))
    return entries


def fetch_biopharmguy_biotech_us(timeout: int) -> list[SourceEntry]:
    resp = requests.get(
        BIOPHARMGUY_BIOTECH_URL, headers={"User-Agent": USER_AGENT}, timeout=timeout
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    entries = []
    for tr in soup.find_all("tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all("td")]
        if len(cols) < 2:
            continue
        name, location = cols[0].strip(), cols[1].strip()
        if not name:
            continue
        state = location.split(" - ")[0].strip()
        if state not in US_STATES:
            continue
        entries.append(SourceEntry(name=name, source="biopharmguy_biotech_us", score=0.5))
    return entries


def fetch_wikipedia_largest_biomedical_us(timeout: int) -> list[SourceEntry]:
    resp = requests.get(
        WIKIPEDIA_LARGEST_BIOMEDICAL_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    target_table = None
    header_map = {}
    for table in soup.find_all("table", class_="wikitable"):
        headers = [clean_text(h.get_text(strip=True)) for h in table.find_all("th")]
        if "Company" in headers and "Country" in headers:
            header_map = {name: idx for idx, name in enumerate(headers)}
            target_table = table
            break

    if not target_table:
        raise RuntimeError("Unable to find biomedical revenue table on Wikipedia.")

    company_idx = header_map["Company"]
    country_idx = header_map["Country"]
    rank_idx = header_map.get("Rank")

    entries = []
    for row in target_table.find_all("tr")[1:]:
        cells = [clean_text(c.get_text(strip=True)) for c in row.find_all(["td", "th"])]
        if len(cells) <= max(company_idx, country_idx):
            continue
        company = cells[company_idx]
        country = cells[country_idx]

        if country not in {"United States", "USA", "US", "U.S."}:
            continue
        if not company:
            continue

        score = 1.0
        if rank_idx is not None and rank_idx < len(cells):
            try:
                score = 1000 - int(cells[rank_idx])
            except ValueError:
                score = 1.0

        entries.append(
            SourceEntry(name=company, source="wikipedia_largest_biomedical_us", score=score)
        )

    return entries


def fetch_wikipedia_largest_pharma_us(timeout: int) -> list[SourceEntry]:
    resp = requests.get(
        WIKIPEDIA_LARGEST_PHARMA_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    target_table = None
    header_map = {}
    for table in soup.find_all("table", class_="wikitable"):
        headers = [clean_text(h.get_text(strip=True)) for h in table.find_all("th")]
        if "Company" in headers and "Country" in headers:
            header_map = {name: idx for idx, name in enumerate(headers)}
            target_table = table
            break

    if not target_table:
        raise RuntimeError("Unable to find pharma revenue table on Wikipedia.")

    company_idx = header_map["Company"]
    country_idx = header_map["Country"]
    rank_idx = header_map.get("Rank")

    entries = []
    for row in target_table.find_all("tr")[1:]:
        cells = [clean_text(c.get_text(strip=True)) for c in row.find_all(["td", "th"])]
        if len(cells) <= max(company_idx, country_idx):
            continue
        company = cells[company_idx]
        country = cells[country_idx]

        if country not in {"United States", "USA", "US", "U.S."}:
            continue
        if not company:
            continue

        score = 1.0
        if rank_idx is not None and rank_idx < len(cells):
            try:
                score = 1000 - int(cells[rank_idx])
            except ValueError:
                score = 1.0

        entries.append(
            SourceEntry(name=company, source="wikipedia_largest_pharma_us", score=score)
        )

    return entries


def select_top(entries: list[SourceEntry], limit: int) -> list[SourceEntry]:
    # stable sort by score (desc), then name
    return sorted(entries, key=lambda e: (-e.score, e.name.lower()))[:limit]


def write_csv(path: Path, names: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["company_name"])
        for name in names:
            writer.writerow([name])


def write_sources(path: Path, rows: Iterable[SourceEntry]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["company_name", "source", "score"])
        for entry in rows:
            writer.writerow([entry.name, entry.source, entry.score])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Curate top 100 US bioinformatics companies.")
    parser.add_argument("--output", default="data/companies_top100.csv", help="Output CSV path.")
    parser.add_argument(
        "--output-sources",
        default="data/companies_top100_sources.csv",
        help="Output CSV path for source tracking.",
    )
    parser.add_argument("--limit", type=int, default=100, help="Number of companies to output.")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    biopharm = fetch_biopharmguy_us(args.timeout)
    biotech_keywords = fetch_biopharmguy_biotech_keywords(args.timeout)
    biomedical_us = fetch_wikipedia_largest_biomedical_us(args.timeout)
    pharma_us = fetch_wikipedia_largest_pharma_us(args.timeout)
    biotech_us = fetch_biopharmguy_biotech_us(args.timeout)

    selected = OrderedDict()
    sources = []

    for entry in biopharm:
        key = normalize_name(entry.name)
        if key not in selected:
            selected[key] = entry.name
            sources.append(entry)

    for entry in select_top(biomedical_us, args.limit):
        key = normalize_name(entry.name)
        if key in selected:
            continue
        selected[key] = entry.name
        sources.append(entry)
        if len(selected) >= args.limit:
            break

    for entry in select_top(pharma_us, args.limit):
        key = normalize_name(entry.name)
        if key in selected:
            continue
        selected[key] = entry.name
        sources.append(entry)
        if len(selected) >= args.limit:
            break

    for entry in select_top(biotech_keywords, args.limit):
        key = normalize_name(entry.name)
        if key in selected:
            continue
        selected[key] = entry.name
        sources.append(entry)
        if len(selected) >= args.limit:
            break

    for entry in select_top(biotech_us, args.limit):
        key = normalize_name(entry.name)
        if key in selected:
            continue
        selected[key] = entry.name
        sources.append(entry)
        if len(selected) >= args.limit:
            break

    if len(selected) < args.limit:
        raise SystemExit(f"Only found {len(selected)} unique US companies.")

    names = list(selected.values())[: args.limit]
    write_csv(Path(args.output), names)
    write_sources(Path(args.output_sources), sources)

    print(f"Wrote {len(names)} companies to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
