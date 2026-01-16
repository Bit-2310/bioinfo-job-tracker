# Bioinfo Job Tracker

Track and analyze bioinformatics job postings from curated company sources, with an integrated dashboard, sponsor-aware filtering, and smart analytics.

---

## 🚀 What This Does

- Collects and tracks bioinformatics job postings
- Prioritizes companies by **H-1B sponsorship activity**:
  - **Group 1**: Active sponsors (new petitions in FY’25)
  - **Group 2**: Past sponsors (only renewals, no new filings in FY’25)
  - **Group 3**: Non-sponsors (excluded from tracking)
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

## 📊 Visa Sponsorship Grouping (FY’25)

We use the FY’25 H-1B disclosure dataset to score and classify companies:

| Group | Description                              | Behavior         |
|-------|------------------------------------------|------------------|
| G1    | Active sponsors (new H-1Bs approved/filed) | ✅ Always tracked |
| G2    | Past sponsors (only renewals, no new)    | ⚙️ Optional       |
| G3    | No record of H-1B filings                | 🚫 Skipped        |

Toggle inclusion of Group 2 in the dashboard via checkbox.

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
| `group_summary.json`        | Visa sponsor group counts + examples        |
| `visa_group_analytics.json` | Roles per group (G1/G2/G3 breakdown)        |

All files are updated automatically after each run.

---

## 🔁 Mermaid Flowchart

```mermaid
flowchart TD
    A[Companies classified by visa group (G1/G2/G3)]
    B[Fetch job roles from source sites]
    C[Save to jobs.db]
    D[Build dashboard JSON: roles, rankings, groups]
    E[Static site deploy (GitHub Pages)]

    A --> B --> C --> D --> E
```

---

## 📱 Mobile Ready
- Responsive card layout
- Touch-friendly toggle
- Readable charts on small screens

---

## 🧩 To Do (Next Phases)
- [ ] Score companies based on role frequency (ranking inside groups)
- [ ] Add Workday JSON parser (for better job extraction)
- [ ] Expand email/alert integrations

---

## 📎 Credits
- H-1B Data via [USCIS FY’25 Disclosure Dataset]
- Dashboard built with Chart.js + GitHub Pages
- Inspired by real-world job hunting challenges in bioinformatics
