"""Deploy merged code changes to production: build only the dbt models changed since the last deployment."""

import shutil
import subprocess
from pathlib import Path

from include.citibike.dbt_config import DBT_EXECUTABLE_PATH, DBT_PROJECT_PATH

PROFILES_DIR = "/usr/local/airflow/include/dbt_profiles"

# Manifest of the last successful deployment. It lives in the production clone,
# so it survives Airflow restarts, and is ignored by git because it is state, not code.
STATE_DIR = Path(DBT_PROJECT_PATH) / "prod_state"

# Separate target directory, so a deployment does not overwrite files of other dbt runs.
TARGET_DIR = Path("/tmp/citibike_deploy_target")


def _run_dbt(args: list[str]) -> None:
    """Run dbt, stream its output to the task log, and fail the task if dbt fails."""
    command = [
        DBT_EXECUTABLE_PATH,
        *args,
        "--project-dir", DBT_PROJECT_PATH,
        "--profiles-dir", PROFILES_DIR,
        "--target-path", str(TARGET_DIR),
    ]
    print("Running:", " ".join(command))

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in process.stdout:
        print(line, end="")

    if process.wait() != 0:
        raise RuntimeError(f"dbt failed with exit code {process.returncode}")


def deploy_changes(full_refresh: bool = False) -> str:
    """
    Build the models whose code changed since the last deployment, together with everything
    downstream of them, and save the new production state.

    Without a saved state (the first run), the whole project is built.
    """
    state_manifest = STATE_DIR / "manifest.json"

    args = ["build"]
    if state_manifest.exists():
        args += ["--select", "state:modified+", "--state", str(STATE_DIR)]
        scope = "changed models"
    else:
        scope = "whole project (no saved state yet)"

    if full_refresh:
        args.append("--full-refresh")

    _run_dbt(args)

    # Only reached if dbt succeeded: the new code is now what production runs.
    STATE_DIR.mkdir(exist_ok=True)
    shutil.copy(TARGET_DIR / "manifest.json", state_manifest)

    return f"Deployed {scope}" + (" with full refresh" if full_refresh else "")