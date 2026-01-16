# Bioinfo Job Tracker

Track and analyze bioinformatics job postings from curated company sources, with an integrated dashboard and smart analytics.

---

## 🚀 What This Does

- Collects and tracks bioinformatics job postings
- Prioritizes companies by **your curated job-fit priority groups**:
  - **Group 1**: Highest priority companies (best fit)
  - **Group 2**: Medium priority companies (good fit, lower urgency)
- Outputs clean frontend JSON to power a GitHub Pages dashboard
- Auto-builds analytics like job counts, top companies, source breakdown

---

## 🧱 Project Structure

```bash
bioinfo-job-tracker/
├── db/jobs.db                 # SQLite DB with companies, roles, classification
├── docs/                     # GitHub Pages dashboard
│   └── data/                 # Frontend JSON data files
├── src/
│   ├── config/settings.yml   # DB path config
│   ├── export/               # JSON + dashboard builders
│   │   ├── build_json.py
│   │   ├── build_analytics.py
│   │   └── generate_site.py
│   └── track/                # Source crawler
├── .github/workflows/track.yml  # CI job runner
└── README.md
```

---

## ✅ Required setup (curated target list)

Create your curated target list in:

`data/priority_companies.csv`

Format:
```csv
company,group
Biogen,1
10x Genomics,1
Mount Sinai,2
```

Only Groups 1 and 2 are tracked by the pipeline.

## 🧪 Local usage

```bash
# 1) Load priority companies (optionally prune an old DB)
PYTHONPATH=. python src/import/import_priority_companies.py --csv data/priority_companies.csv --db db/jobs.db --prune

# 2) Discover + validate sources for those companies
PYTHONPATH=. python src/verify/discover_sources.py
PYTHONPATH=. python src/verify/validate_sources.py

# 3) Track roles
PYTHONPATH=. python src/track/fetch_roles.py

# 4) Export dashboard JSON
PYTHONPATH=. python src/export/build_json.py
PYTHONPATH=. python src/export/build_analytics.py
```

Then push to GitHub → dashboard updates via GitHub Pages.

---

## 🎯 Priority Grouping

Groups are used as *priority tiers* for scanning and ranking.

| Group | Meaning                                | Default scanning |
|-------|-----------------------------------------|-----------------|
| G1    | High-fit, highest priority companies    | ✅ Every run      |
| G2    | Good fit but lower urgency              | ⚙️ Every ~24h     |

H-1B sponsorship is treated as an optional signal stored in `company_signals`.

### Example output (`group_summary.json`):
```json
{
  "group1": 183,
  "group2": 112,
  "examples": {
    "group1": ["Biogen", "St. Jude", "Cleveland Clinic"],
    "group2": ["MedGenome", "BeyondSpring Pharma"]
  }
}
```

---

## 🧠 Dashboard JSON Files

| File                         | Description                                 |
|------------------------------|---------------------------------------------|
| `new_roles.json`            | Fresh roles from latest run                 |
| `active_roles.json`         | All still-live roles                        |
| `run_summary.json`          | Time, counts, status of latest pipeline run |
| `source_analytics.json`     | Sources per company + types                 |
| `company_priority.json`     | Tiered ranking of most active companies     |
| `group_summary.json`        | Priority group counts + examples            |
| `priority_group_analytics.json` | Roles per group (G1/G2 breakdown)       |

All files are updated automatically after each run.

---

## 🔁 Mermaid Flowchart

```mermaid
flowchart TD
    A[Companies classified into Group 1 or 2]
    B[Fetch job roles from source websites]
    C[Save to SQLite database]
    D[Build JSON: roles, rankings, analytics]
    E[Deploy static site (GitHub Pages)]

    A --> B --> C --> D --> E
```
---

## 📱 Mobile Ready
- Responsive card layout
- Touch-friendly toggle
- Readable charts on small screens

---

## 🧩 Next Phases
- [ ] Improve discovery/verification so sources grow steadily (less reliance on DDG HTML)
- [ ] Add Workday adapter (major coverage win)
- [ ] Add seed source import (`data/sources_seed.csv`) for deterministic sources
- [ ] Expand email/alert integrations

---

## 📎 Credits
- Optional sponsorship signal can be derived from the USCIS FY’25 Disclosure Dataset
- Dashboard built with Chart.js + GitHub Pages
- Inspired by real-world job hunting challenges in bioinformatics
