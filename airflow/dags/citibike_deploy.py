"""Manually triggered: deploy merged code changes to production."""

from datetime import datetime

from airflow.sdk import Param, dag, get_current_context, task

from include.citibike.deploy import deploy_changes


@dag(
    schedule=None,  # run manually after merging to main and running git pull in the production clone
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            description=(
                "Rebuild changed incremental models from scratch. Needed when their logic or "
                "columns changed. On the very first run this rebuilds the whole project."
            ),
        ),
    },
    tags=["citibike", "deploy"],
)
def citibike_deploy():

    @task
    def deploy() -> str:
        full_refresh = get_current_context()["params"]["full_refresh"]
        return deploy_changes(full_refresh=full_refresh)

    deploy()


citibike_deploy()