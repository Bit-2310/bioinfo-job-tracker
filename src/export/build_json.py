import sqlite3
import json
from pathlib import Path
from collections import defaultdict

OUTPUT_DIR = Path("docs/data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path("db/jobs.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

roles = []
source_counts = {"linkedin": 0, "company": 0}
company_counts = {}
group_counts = defaultdict(int)
group_company_counts = defaultdict(lambda: defaultdict(int))

for row in c.execute("""
  SELECT r.title, c.name, r.location, r.posted, r.url, r.source, cl."group"
  FROM roles r
  JOIN company_job_sources s ON r.source_id = s.source_id
  JOIN companies c ON s.company_id = c.company_id
  JOIN company_classification cl ON cl.company_id = c.company_id
  WHERE r.date_scraped = (
    SELECT MAX(date_scraped) FROM roles
  )
"""):
  title, company, location, posted, url, source, group = row
  roles.append({
    "title": title,
    "company": company,
    "location": location,
    "posted": posted,
    "url": url,
    "source": source,
    "group": group
  })
  source_counts[source] += 1
  company_counts[company] = company_counts.get(company, 0) + 1
  group_counts[group] += 1
  group_company_counts[group][company] += 1

# Save new_roles.json
with open(OUTPUT_DIR / "new_roles.json", "w") as f:
  json.dump({"roles": roles}, f, indent=2)

# Save top company rankings
ranking = sorted(company_counts.items(), key=lambda x: -x[1])[:10]
with open(OUTPUT_DIR / "company_rankings.json", "w") as f:
  json.dump([{"company": c, "count": n} for c, n in ranking], f, indent=2)

# Build examples dynamically by top activity
examples = {}
for g in [1, 2, 3]:
  top = sorted(group_company_counts[g].items(), key=lambda x: -x[1])[:6]
  examples[f"group{g}"] = [name for name, _ in top]

# Save group summary
summary = {
  "group1": group_counts[1],
  "group2": group_counts[2],
  "group3": group_counts[3],
  "examples": examples
}
with open(OUTPUT_DIR / "group_summary.json", "w") as f:
  json.dump(summary, f, indent=2)

print("✅ JSON exports complete.")
