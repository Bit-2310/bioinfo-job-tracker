import yaml
from src.utils.db import connect, ensure_tables

SETTINGS = yaml.safe_load(open('src/config/settings.yml'))
DB_PATH = SETTINGS['db_path']

with connect(DB_PATH) as con:
    ensure_tables(con)
    cur = con.cursor()

    cur.execute('SELECT COUNT(*) FROM companies')
    companies = int(cur.fetchone()[0])

    cur.execute('SELECT COUNT(*) FROM company_job_sources')
    sources = int(cur.fetchone()[0])

    cur.execute('SELECT COUNT(*) FROM roles')
    roles = int(cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM roles WHERE status='active'")
    active_roles = int(cur.fetchone()[0])

print('DB preview')
print('---------')
print(f'companies: {companies}')
print(f'sources:   {sources}')
print(f'roles:     {roles} (active: {active_roles})')
