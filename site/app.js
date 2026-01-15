fetch('data/new_roles.json')
  .then(r => r.json())
  .then(data => {
    document.getElementById('last-updated').innerText = "Last updated: " + data.meta.last_run;
    const table = document.getElementById('roles');
    table.innerHTML = "<tr><th>Company</th><th>Role</th><th>Location</th><th>First Seen</th><th>Link</th></tr>";
    data.roles.forEach(r => {
      table.innerHTML += `<tr>
        <td>${r.company}</td>
        <td>${r.title}</td>
        <td>${r.location}</td>
        <td>${r.first_seen}</td>
        <td><a href="${r.apply_url}" target="_blank">Apply</a></td>
      </tr>`;
    });
  });
