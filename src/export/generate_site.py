import shutil
from pathlib import Path

DATA_OUT = Path("docs/data")
DATA_SRC = Path("data")
DB_SRC = Path("db/jobs.db")
TRACK_YML = Path(".github/workflows/track.yml")

# Ensure target directory exists
DATA_OUT.mkdir(parents=True, exist_ok=True)

# Copy JSON outputs (already in docs/data)
print("✓ JSON outputs assumed built via build_json.py")

# Copy GitHub Actions workflow (for frontend YAML viewer)
shutil.copyfile(TRACK_YML, DATA_OUT / "track.yml")
print("✓ Copied track.yml to dashboard")

# Copy DB (if needed for local use or download/debug)
# Optional: shutil.copy(DB_SRC, DATA_OUT / "jobs.db")

print("✅ Static site generation complete")
