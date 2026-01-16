/*
  Bioinformatics Job Tracker (static dashboard)

  Data files produced by the workflow:
  - docs/data/new_roles.json
  - docs/data/active_roles.json
  - docs/data/company_rankings.json
  - docs/data/metadata.json
  - docs/data/source_analytics.json (optional)
  - docs/data/company_priority.json (optional)
  - docs/data/run_summary.json (optional)

  This page is intentionally “no build tools”: plain HTML/CSS/JS.
*/

const DATA_DIR = "./data";

function $(id) {
  return document.getElementById(id);
}

function fmtInt(n) {
  try {
    return new Intl.NumberFormat().format(Number(n) || 0);
  } catch {
    return String(n ?? 0);
  }
}

function safeText(s) {
  return (s ?? "").toString();
}

function toLower(s) {
  return safeText(s).toLowerCase();
}

function parseISO(s) {
  if (!s) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

function relTime(iso) {
  const d = parseISO(iso);
  if (!d) return "—";
  const diffMs = Date.now() - d.getTime();
  const mins = Math.floor(diffMs / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 48) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

async function loadJson(path, fallback = null) {
  try {
    const r = await fetch(path, { cache: "no-store" });
    if (!r.ok) return fallback;
    return await r.json();
  } catch {
    return fallback;
  }
}

async function loadText(path, fallback = "") {
  try {
    const r = await fetch(path, { cache: "no-store" });
    if (!r.ok) return fallback;
    return await r.text();
  } catch {
    return fallback;
  }
}

function makeTable(columns, rows, options = {}) {
  const { emptyText = "No data yet." } = options;
  if (!rows || rows.length === 0) {
    return `<div class="empty">${emptyText}</div>`;
  }

  const head = columns
    .map((c) => `<th>${safeText(c.label)}</th>`)
    .join("");

  const body = rows
    .map((row) => {
      const tds = columns
        .map((c) => {
          const v = row[c.key];
          return `<td>${c.render ? c.render(v, row) : safeText(v)}</td>`;
        })
        .join("");
      return `<tr>${tds}</tr>`;
    })
    .join("");

  return `
    <div class="table-wrap">
      <table>
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function roleFamily(roleTitle) {
  const t = toLower(roleTitle);
  if (t.includes("bioinformatics") || t.includes("bio-informatics")) return "bioinformatics";
  if (t.includes("computational biology") || t.includes("comp bio") || t.includes("comp-bio")) return "comp_bio";
  if (t.includes("genomics") || t.includes("genomic")) return "genomics";
  if (t.includes("data scientist") || t.includes("machine learning") || t.includes("ml")) return "ds_bio";
  return "other";
}

function normalizeRole(r) {
  return {
    company: r.company || r.employer_name || "",
    title: r.title || r.role || "",
    location: r.location || "",
    link: r.link || r.url || "",
    first_seen: r.first_seen || r.firstSeen || r.seen_at || "",
    is_active: r.is_active ?? r.active ?? 1,
    match_score: Number(r.match_score ?? r.score ?? 0),
    source_type: r.source_type || r.source || "",
    family: r.family || roleFamily(r.title || r.role || ""),
  };
}

function applyFilters(roles, q, family) {
  const qq = toLower(q);
  return roles.filter((r) => {
    if (family && family !== "all" && r.family !== family) return false;
    if (!qq) return true;
    return (
      toLower(r.company).includes(qq) ||
      toLower(r.title).includes(qq) ||
      toLower(r.location).includes(qq)
    );
  });
}

function applySort(roles, sort) {
  const copy = [...roles];
  if (sort === "score") {
    copy.sort((a, b) => (b.match_score || 0) - (a.match_score || 0));
    return copy;
  }
  if (sort === "company") {
    copy.sort((a, b) => safeText(a.company).localeCompare(safeText(b.company)));
    return copy;
  }
  // newest
  copy.sort((a, b) => {
    const da = parseISO(a.first_seen)?.getTime() || 0;
    const db = parseISO(b.first_seen)?.getTime() || 0;
    return db - da;
  });
  return copy;
}

function computeSourceCounts(allRoles) {
  const counts = {};
  for (const r of allRoles) {
    const k = r.source_type || "unknown";
    counts[k] = (counts[k] || 0) + 1;
  }
  return counts;
}

function computeTopCompanies(allRoles, limit = 10) {
  const by = new Map();
  for (const r of allRoles) {
    const k = r.company || "";
    if (!k) continue;
    const v = by.get(k) || 0;
    by.set(k, v + 1);
  }
  const arr = [...by.entries()].map(([company, count]) => ({ company, count }));
  arr.sort((a, b) => b.count - a.count);
  return arr.slice(0, limit);
}

function renderScheduleFromYaml(yamlText) {
  const lines = yamlText.split("\n");
  const crons = [];
  for (const ln of lines) {
    const m = ln.match(/-\s+cron:\s+"([^"]+)"/);
    if (m) crons.push(m[1]);
  }
  const el = $("workflow-schedule");
  if (!el) return;
  el.innerHTML = crons.length
    ? crons.map((c) => `<span class="chip">${safeText(c)}</span>`).join("")
    : `<span class="muted">No schedule found.</span>`;
}

let chartSources = null;
let chartTop = null;

function renderCharts({ sourceCounts, topCompanies }) {
  const sourcesEl = $("chart-sources");
  const topEl = $("chart-top");

  if (sourcesEl && window.Chart) {
    const labels = Object.keys(sourceCounts);
    const values = labels.map((k) => sourceCounts[k]);
    if (chartSources) chartSources.destroy();
    chartSources = new Chart(sourcesEl, {
      type: "bar",
      data: {
        labels,
        datasets: [{ label: "Roles", data: values }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: "rgba(232,238,246,0.8)" }, grid: { color: "rgba(232,238,246,0.08)" } },
          y: { ticks: { color: "rgba(232,238,246,0.8)" }, grid: { color: "rgba(232,238,246,0.08)" } },
        },
      },
    });
  }

  if (topEl && window.Chart) {
    const labels = topCompanies.map((x) => x.company);
    const values = topCompanies.map((x) => x.count);
    if (chartTop) chartTop.destroy();
    chartTop = new Chart(topEl, {
      type: "bar",
      data: {
        labels,
        datasets: [{ label: "Roles", data: values }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: "rgba(232,238,246,0.8)", maxRotation: 90, minRotation: 40 }, grid: { color: "rgba(232,238,246,0.08)" } },
          y: { ticks: { color: "rgba(232,238,246,0.8)" }, grid: { color: "rgba(232,238,246,0.08)" } },
        },
      },
    });
  }
}

function computePriorityFromRankings(rankings) {
  // Rankings file already has a score. We map to tiers.
  const out = rankings.map((r) => {
    const score = Number(r.score ?? 0);
    const tier = score >= 30 ? "Tier 1" : score >= 15 ? "Tier 2" : score >= 5 ? "Tier 3" : "Tier 4";
    const tier_label = tier === "Tier 1" ? "Hot" : tier === "Tier 2" ? "Warm" : tier === "Tier 3" ? "Cold" : "Unknown";
    return {
      company: r.company,
      active_roles: r.active_roles ?? 0,
      new_roles: r.new_roles ?? 0,
      avg_score: Math.round((Number(r.avg_score ?? 0)) * 10) / 10,
      score,
      tier,
      tier_label,
    };
  });
  out.sort((a, b) => b.score - a.score);
  return out;
}

function updateAnalyticsCards({ meta, sourceAnalytics, runSummary, allRoles, priorityRows }) {
  const lastRun = runSummary?.last_run || meta?.last_run || "";
  const windowHours = runSummary?.new_window_hours ?? meta?.new_window_hours ?? 24;

  const sourcesTotal = sourceAnalytics?.counts?.total_sources ?? sourceAnalytics?.total_sources ?? null;
  const companiesWithSources = sourceAnalytics?.counts?.companies_with_sources ?? sourceAnalytics?.companies_with_sources ?? null;
  const totalCompanies = sourceAnalytics?.counts?.total_companies ?? sourceAnalytics?.total_companies ?? null;

  const tiers = priorityRows.reduce(
    (acc, r) => {
      acc[r.tier] = (acc[r.tier] || 0) + 1;
      return acc;
    },
    {}
  );

  $("ana-last-run").textContent = lastRun ? new Date(lastRun).toLocaleString() : "—";
  $("ana-last-run-sub").textContent = `New window: ${windowHours}h · ${relTime(lastRun)}`;

  $("ana-sources").textContent = sourcesTotal != null ? fmtInt(sourcesTotal) : "—";
  if (companiesWithSources != null && totalCompanies != null) {
    $("ana-sources-sub").textContent = `${fmtInt(companiesWithSources)} / ${fmtInt(totalCompanies)} companies have sources`;
  } else {
    $("ana-sources-sub").textContent = "—";
  }

  $("ana-roles").textContent = fmtInt(allRoles.length);
  $("ana-roles-sub").textContent = "Across active + new roles";

  const hot = (tiers["Tier 1"] || 0) + (tiers["Tier 2"] || 0);
  $("ana-priority").textContent = fmtInt(hot);
  $("ana-priority-sub").textContent = `Hot/Warm: ${fmtInt(tiers["Tier 1"] || 0)} / ${fmtInt(tiers["Tier 2"] || 0)}`;
}

function attachTabs() {
  const tabs = Array.from(document.querySelectorAll(".tab"));
  function show(tabName) {
    for (const t of tabs) {
      const on = t.dataset.tab === tabName;
      t.classList.toggle("active", on);
      t.setAttribute("aria-selected", on ? "true" : "false");
    }
    const panels = Array.from(document.querySelectorAll(".panel"));
    for (const p of panels) {
      p.classList.toggle("show", p.id === `panel-${tabName}`);
    }
  }
  for (const t of tabs) {
    t.addEventListener("click", () => show(t.dataset.tab));
  }
}

async function main() {
  attachTabs();

  const [meta, newRoles, activeRoles, rankings, sourceAnalytics, companyPriority, runSummary] = await Promise.all([
    loadJson(`${DATA_DIR}/metadata.json`, null),
    loadJson(`${DATA_DIR}/new_roles.json`, { roles: [] }),
    loadJson(`${DATA_DIR}/active_roles.json`, { roles: [] }),
    loadJson(`${DATA_DIR}/company_rankings.json`, { companies: [] }),
    loadJson(`${DATA_DIR}/source_analytics.json`, null),
    loadJson(`${DATA_DIR}/company_priority.json`, null),
    loadJson(`${DATA_DIR}/run_summary.json`, null),
  ]);

  const newList = (newRoles?.roles || []).map(normalizeRole);
  const activeList = (activeRoles?.roles || []).map(normalizeRole);

  // Rankings (from exporter)
  const rankingCompanies = rankings?.companies || [];

  // Compute a priority table either from company_priority.json (preferred) or from rankings.
  const priorityRows = (companyPriority?.companies || null)
    ? companyPriority.companies
    : computePriorityFromRankings(rankingCompanies);

  const allRoles = [...newList, ...activeList];

  // Header numbers
  const lastUpdated = meta?.last_run || runSummary?.last_run || "";
  $("last-updated").textContent = lastUpdated
    ? `Last updated: ${new Date(lastUpdated).toLocaleString()}`
    : "Last updated: —";
  $("counts").textContent = `${fmtInt(newList.length)} new · ${fmtInt(activeList.length)} active · ${fmtInt(priorityRows.length)} ranked companies`;

  // Render tables
  const roleCols = [
    { key: "company", label: "Company" },
    { key: "title", label: "Role" },
    { key: "location", label: "Location" },
    {
      key: "first_seen",
      label: "First seen",
      render: (v) => {
        const d = parseISO(v);
        return d ? d.toLocaleString() : "—";
      },
    },
    {
      key: "link",
      label: "Link",
      render: (v) => (v ? `<a href="${v}" target="_blank" rel="noopener">Open</a>` : "—"),
    },
  ];

  const rankCols = [
    { key: "company", label: "Company" },
    { key: "new_roles", label: "New" },
    { key: "active_roles", label: "Active" },
    { key: "avg_score", label: "Avg match" },
    { key: "score", label: "Score" },
  ];

  const priorityCols = [
    { key: "tier_label", label: "Priority" },
    { key: "company", label: "Company" },
    { key: "new_roles", label: "New (7d/24h*)" },
    { key: "active_roles", label: "Active" },
    { key: "avg_score", label: "Avg match" },
    { key: "score", label: "Score" },
  ];

  // Interactions
  function renderAllTables() {
    const q = $("q").value;
    const family = $("family").value;
    const sort = $("sort").value;

    const newFiltered = applySort(applyFilters(newList, q, family), sort);
    const activeFiltered = applySort(applyFilters(activeList, q, family), sort);

    $("new-table").innerHTML = makeTable(roleCols, newFiltered, { emptyText: "No new roles yet." });
    $("active-table").innerHTML = makeTable(roleCols, activeFiltered, { emptyText: "No active roles yet." });
    $("rank-table").innerHTML = makeTable(rankCols, rankingCompanies, { emptyText: "No rankings yet. Run the workflow a few times." });
  }

  renderAllTables();
  $("q").addEventListener("input", renderAllTables);
  $("family").addEventListener("change", renderAllTables);
  $("sort").addEventListener("change", renderAllTables);

  // Analytics tables + charts
  $("priority-table").innerHTML = makeTable(priorityCols, priorityRows, {
    emptyText: "No priority data yet. Run discovery + tracking a few times.",
  });

  const sourceCounts = sourceAnalytics?.sources_by_type || computeSourceCounts(allRoles);
  const topCompanies = sourceAnalytics?.top_companies_new_7d || computeTopCompanies(allRoles, 10);

  updateAnalyticsCards({ meta, sourceAnalytics, runSummary, allRoles, priorityRows });
  renderCharts({ sourceCounts, topCompanies });

  // Workflow panel
  const yamlText = await loadText(`${DATA_DIR}/track.yml`, "");
  $("workflow-yaml").textContent = yamlText || "track.yml not found in docs/data/track.yml";
  if (yamlText) renderScheduleFromYaml(yamlText);
}

main();
