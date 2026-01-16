// Load job data and build dashboard
let group2Enabled = false;

async function loadData() {
  const [rolesResp, groupResp] = await Promise.all([
    fetch("data/new_roles.json"),
    fetch("data/group_summary.json")
  ]);
  const { roles } = await rolesResp.json();
  const groupSummary = await groupResp.json();
  return { roles, groupSummary };
}

function filterRolesByGroup(roles) {
  return roles.filter(r => r.group === 1 || (group2Enabled && r.group === 2));
}

function renderRoles(roles) {
  const tbody = document.getElementById("roles-table");
  tbody.innerHTML = "";
  filterRolesByGroup(roles).forEach(role => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${role.title}</td>
      <td>${role.company}</td>
      <td>${role.location}</td>
      <td>${role.posted}</td>
      <td><a href="${role.url}" target="_blank">Link</a></td>
    `;
    tbody.appendChild(row);
  });
}

function renderTopCompanies(roles) {
  const counts = {};
  filterRolesByGroup(roles).forEach(r => {
    counts[r.company] = (counts[r.company] || 0) + 1;
  });
  const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 5);
  const list = document.getElementById("top-companies");
  list.innerHTML = sorted.map(([c, n]) => `<li>${c} (${n})</li>`).join("");
}

function renderSourceChart(roles) {
  const counts = { linkedin: 0, company: 0 };
  filterRolesByGroup(roles).forEach(r => {
    if (r.source === "linkedin") counts.linkedin++;
    else counts.company++;
  });
  new Chart(document.getElementById("sourceChart"), {
    type: "doughnut",
    data: {
      labels: ["Company Site", "LinkedIn"],
      datasets: [{
        data: [counts.company, counts.linkedin],
        backgroundColor: ["#4caf50", "#2196f3"]
      }]
    },
    options: { responsive: true }
  });
}

function renderGroupSummary(summary) {
  document.getElementById("group1-count").textContent = summary.group1;
  document.getElementById("group2-count").textContent = summary.group2;
  document.getElementById("group3-count").textContent = summary.group3;
}

function loadTrackYML() {
  fetch("data/track.yml")
    .then(res => res.text())
    .then(text => {
      document.getElementById("track-yml").textContent = text;
    });
}

loadData().then(({ roles, groupSummary }) => {
  renderGroupSummary(groupSummary);
  renderRoles(roles);
  renderTopCompanies(roles);
  renderSourceChart(roles);

  document.getElementById("toggle-group2").addEventListener("change", e => {
    group2Enabled = e.target.checked;
    renderRoles(roles);
    renderTopCompanies(roles);
    renderSourceChart(roles);
  });

  loadTrackYML();
});
