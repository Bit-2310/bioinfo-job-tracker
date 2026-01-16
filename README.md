# Bioinfo Job Tracker

Track and analyze bioinformatics job postings from curated company sources, with an integrated dashboard and smart analytics.

---

## 🚀 What This Does

- Collects and tracks bioinformatics job postings
- Prioritizes companies by **your job-fit priority groups**:
  - **Group 1**: Highest priority companies (best fit for Pranava)
  - **Group 2**: Medium priority companies (good fit, lower urgency)
  - **Group 3**: Low priority / exploration companies
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

## 🧪 Usage

```bash
# Step 1 — Crawl jobs (from pre-discovered sources)
python src/track/run_batch.py  # or run.py

# Step 2 — Build dashboard JSON
python src/export/build_json.py
python src/export/build_analytics.py
python src/export/generate_site.py
```

Then push to GitHub → dashboard updates via GitHub Pages.

---

## 🎯 Priority Grouping

The three groups are used as *priority tiers* for scanning and for the dashboard. H-1B sponsorship is a helpful signal, but it is not what defines the groups.

| Group | Meaning (for Pranava)                               | Default scanning |
|-------|------------------------------------------------------|-----------------|
| G1    | High-fit, high priority companies                    | ✅ Every run      |
| G2    | Good fit but lower urgency                           | ⚙️ Every ~24h     |
| G3    | Low priority / exploration                           | 🧪 Sampled        |

You can toggle inclusion of Group 2 in dashboard tables via checkbox.

### Example output (`group_summary.json`):
```json
{
  "group1": 183,
  "group2": 112,
  "group3": 4972,
  "examples": {
    "group1": ["Biogen", "St. Jude", "Cleveland Clinic"],
    "group2": ["MedGenome", "BeyondSpring Pharma"],
    "group3": ["City of Chicago", "CVS Pharmacy"]
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
| `priority_group_analytics.json` | Roles per group (G1/G2/G3 breakdown)    |
| `visa_group_analytics.json` | Backward-compatible alias                    |

All files are updated automatically after each run.

---

## 🔁 Mermaid Flowchart

```mermaid
flowchart TD
    A[Companies classified into Group 1, 2, or 3]
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
