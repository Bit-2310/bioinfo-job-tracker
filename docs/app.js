let group2Enabled = false;

async function loadData() {
  const [newResp, activeResp, picksResp, groupResp, runResp, srcAnaResp, priorityResp] = await Promise.all([
    fetch("data/new_roles.json"),
    fetch("data/active_roles.json"),
    fetch("data/top_picks.json"),
    fetch("data/group_summary.json"),
    fetch("data/run_summary.json"),
    fetch("data/source_analytics.json"),
    fetch("data/company_priority.json")
  ]);

  const newData = await newResp.json();
  const activeData = await activeResp.json();
  const picksData = await picksResp.json();
  const groupSummary = await groupResp.json();
  const runSummary = await runResp.json();
  const sourceAnalytics = await srcAnaResp.json();
  const companyPriority = await priorityResp.json();

  return {
    meta: newData.meta,
    newRoles: newData.roles || [],
    activeRoles: activeData.roles || [],
    topPicks: picksData.roles || [],
    groupSummary,
    runSummary,
    sourceAnalytics,
    companyPriority: companyPriority.companies || []
  };
}

function roleGroupOk(role) {
  return role.group === 1 || (group2Enabled && role.group === 2);
}

function fmtDate(s) {
  if (!s) return "";
  // show only YYYY-MM-DD for readability
  return String(s).slice(0, 10);
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function renderPipelineHealth(runSummary, sourceAnalytics) {
  const last = runSummary.last_run || {};
  setText("last-run-status", last.status || "unknown");
  setText("last-run-finished", last.finished_at ? fmtDate(last.finished_at) : "-");

  const src = runSummary.sources || {};
  setText("sources-success", src.sources_success ?? 0);
  setText("sources-fail", src.sources_fail ?? 0);
  setText("roles-seen", src.roles_seen ?? 0);
  setText("roles-new", src.new_roles ?? 0);

  setText("total-companies", sourceAnalytics.total_companies ?? 0);
  setText("companies-with-sources", sourceAnalytics.companies_with_sources ?? 0);
  setText("sources-total", sourceAnalytics.sources_total ?? 0);
}

function renderGroupSummary(summary) {
  setText("group1-count", summary.group1 ?? 0);
  setText("group2-count", summary.group2 ?? 0);
  setText("group3-count", summary.group3 ?? 0);
}

function renderTopPicks(roles) {
  const tbody = document.getElementById("top-picks-table");
  if (!tbody) return;
  tbody.innerHTML = "";
  roles.filter(roleGroupOk).forEach(role => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${role.title}</td>
      <td>${role.company}</td>
      <td>${role.location || ""}</td>
      <td>${role.role_family || ""}</td>
      <td>${(role.match_score ?? 0).toFixed(2)}</td>
      <td>${fmtDate(role.first_seen_at)}</td>
      <td><a href="${role.apply_url}" target="_blank" rel="noreferrer">Apply</a></td>
    `;
    tbody.appendChild(row);
  });
}

function renderNewRoles(roles) {
  const tbody = document.getElementById("new-roles-table");
  if (!tbody) return;
  tbody.innerHTML = "";
  roles.filter(roleGroupOk).forEach(role => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${role.title}</td>
      <td>${role.company}</td>
      <td>${role.location || ""}</td>
      <td>${(role.match_score ?? 0).toFixed(2)}</td>
      <td>${fmtDate(role.first_seen_at)}</td>
      <td><a href="${role.apply_url}" target="_blank" rel="noreferrer">Apply</a></td>
    `;
    tbody.appendChild(row);
  });
}

function renderSourceChart(roles) {
  const counts = {};
  roles.filter(roleGroupOk).forEach(r => {
    const k = r.source_type || "unknown";
    counts[k] = (counts[k] || 0) + 1;
  });

  const labels = Object.keys(counts);
  const data = labels.map(k => counts[k]);

  new Chart(document.getElementById("sourceChart"), {
    type: "doughnut",
    data: {
      labels,
      datasets: [{
        data,
      }]
    },
    options: { responsive: true }
  });
}

function renderCompanyLeaderboard(companies) {
  const tbody = document.getElementById("company-leaderboard");
  if (!tbody) return;
  tbody.innerHTML = "";
  (companies || []).slice(0, 15).forEach(c => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${c.company}</td>
      <td>${c.label || ""}</td>
      <td>${c.new_roles_7d ?? 0}</td>
      <td>${c.active_roles ?? 0}</td>
      <td>${(c.avg_match_score ?? 0).toFixed(2)}</td>
    `;
    tbody.appendChild(row);
  });
}

loadData().then(({ meta, newRoles, activeRoles, topPicks, groupSummary, runSummary, sourceAnalytics, companyPriority }) => {
  setText("total-new-roles", meta?.counts?.new_roles ?? newRoles.length);
  setText("total-active-roles", meta?.counts?.active_roles ?? activeRoles.length);

  renderPipelineHealth(runSummary, sourceAnalytics);
  renderGroupSummary(groupSummary);
  renderTopPicks(topPicks);
  renderNewRoles(newRoles);
  renderSourceChart(activeRoles);
  renderCompanyLeaderboard(companyPriority);

  document.getElementById("toggle-group2").addEventListener("change", e => {
    group2Enabled = e.target.checked;
    renderTopPicks(topPicks);
    renderNewRoles(newRoles);
    renderSourceChart(activeRoles);
  });
});
