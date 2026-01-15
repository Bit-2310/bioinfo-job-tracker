const state = {
  tab: "new",
  q: "",
  sort: "newest",
  family: "all",
  newRoles: [],
  activeRoles: [],
  rankings: [],
  meta: null,
};

function fmt(s){
  if(!s) return "";
  return String(s);
}

function matchesQuery(r, q){
  if(!q) return true;
  const hay = `${r.company||""} ${r.title||""} ${r.location||""}`.toLowerCase();
  return hay.includes(q.toLowerCase());
}

function matchesFamily(r, fam){
  if(fam === "all") return true;
  return (r.role_family || "other") === fam;
}

function sortRoles(list, mode){
  const copy = [...list];
  if(mode === "score"){
    copy.sort((a,b) => (b.match_score||0) - (a.match_score||0));
  } else if(mode === "company"){
    copy.sort((a,b) => (a.company||"").localeCompare(b.company||""));
  } else {
    // newest
    copy.sort((a,b) => (b.first_seen||"").localeCompare(a.first_seen||""));
  }
  return copy;
}

function renderRoles(containerId, roles){
  const container = document.getElementById(containerId);
  if(!roles.length){
    container.innerHTML = `<div style="padding:14px;color:#9aa4b2;">No roles yet. Run the workflow a few times. Greenhouse/Lever roles will show first.</div>`;
    return;
  }

  const rows = roles.map(r => `
    <tr>
      <td>${fmt(r.company)}</td>
      <td>
        <div style="font-weight:700;">${fmt(r.title)}</div>
        <div class="badge">${fmt(r.role_family || "other")}</div>
      </td>
      <td>${fmt(r.location)}</td>
      <td>${fmt(r.first_seen)}</td>
      <td class="score">${(r.match_score ?? 0).toFixed(1)}</td>
      <td><a href="${fmt(r.apply_url)}" target="_blank" rel="noreferrer">Apply</a></td>
    </tr>
  `).join("");

  container.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Company</th>
          <th>Role</th>
          <th>Location</th>
          <th>First seen</th>
          <th>Score</th>
          <th>Link</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderRankings(){
  const container = document.getElementById("rank-table");
  const list = state.rankings || [];
  if(!list.length){
    container.innerHTML = `<div style="padding:14px;color:#9aa4b2;">No rankings yet (needs roles data).</div>`;
    return;
  }
  const rows = list.map(r => `
    <tr>
      <td>${r.rank}</td>
      <td style="font-weight:700;">${fmt(r.company)}</td>
      <td class="score">${fmt(r.score)}</td>
      <td>${fmt(r.active_roles)}</td>
      <td>${fmt(r.new_roles_24h)}</td>
      <td>${fmt(r.avg_match_score)}</td>
    </tr>
  `).join("");

  container.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Company</th>
          <th>Priority score</th>
          <th>Active roles</th>
          <th>New (24h)</th>
          <th>Avg match</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function applyFilters(){
  const q = state.q.trim();
  const fam = state.family;

  const newFiltered = sortRoles(
    state.newRoles.filter(r => matchesQuery(r, q) && matchesFamily(r, fam)),
    state.sort
  );

  const activeFiltered = sortRoles(
    state.activeRoles.filter(r => matchesQuery(r, q) && matchesFamily(r, fam)),
    state.sort
  );

  renderRoles("new-table", newFiltered);
  renderRoles("active-table", activeFiltered);
  renderRankings();
}

function setTab(tab){
  state.tab = tab;
  document.querySelectorAll(".tab").forEach(b => b.classList.toggle("active", b.dataset.tab === tab));
  document.getElementById("panel-new").classList.toggle("show", tab === "new");
  document.getElementById("panel-active").classList.toggle("show", tab === "active");
  document.getElementById("panel-rank").classList.toggle("show", tab === "rank");
}

async function load(){
  const metaRes = await fetch("data/meta.json", { cache: "no-store" });
  const meta = metaRes.ok ? await metaRes.json() : null;
  state.meta = meta;

  const last = document.getElementById("last-updated");
  const counts = document.getElementById("counts");

  if(meta){
    last.textContent = "Last updated: " + (meta.last_run || "—");
    const c = meta.counts || {};
    counts.textContent = `New: ${c.new_roles||0} | Active: ${c.active_roles||0} | Ranked companies: ${c.ranked_companies||0}`;
  } else {
    last.textContent = "Last updated: —";
    counts.textContent = "";
  }

  const newRes = await fetch("data/new_roles.json", { cache: "no-store" });
  const newJson = newRes.ok ? await newRes.json() : { roles: [] };

  const activeRes = await fetch("data/active_roles.json", { cache: "no-store" });
  const activeJson = activeRes.ok ? await activeRes.json() : { roles: [] };

  const rankRes = await fetch("data/company_rankings.json", { cache: "no-store" });
  const rankJson = rankRes.ok ? await rankRes.json() : { companies: [] };

  state.newRoles = (newJson.roles || []);
  state.activeRoles = (activeJson.roles || []);
  state.rankings = (rankJson.companies || []);

  applyFilters();
}

document.querySelectorAll(".tab").forEach(btn => {
  btn.addEventListener("click", () => setTab(btn.dataset.tab));
});

document.getElementById("q").addEventListener("input", (e) => {
  state.q = e.target.value;
  applyFilters();
});

document.getElementById("sort").addEventListener("change", (e) => {
  state.sort = e.target.value;
  applyFilters();
});

document.getElementById("family").addEventListener("change", (e) => {
  state.family = e.target.value;
  applyFilters();
});

setTab("new");
load();
