"""Daily check for a new monthly Citi Bike trip file; load it and rebuild the dbt project."""

from datetime import datetime, timedelta

from airflow.sdk import dag, task
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.constants import InvocationMode

from include.citibike.dbt_config import (
    DBT_EXECUTABLE_PATH,
    execution_config,
    profile_config,
    project_config,
)
from include.citibike.trips import find_month_file_url, get_next_month_to_load, load_trip_month


@dag(
    schedule="0 7 * * *",  # every day at 07:00 UTC, an hour after the stations DAG
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["citibike", "trips"],
)
def citibike_trips():

    @task
    def next_month() -> str:
        return get_next_month_to_load()

    @task.short_circuit
    def is_published(month: str) -> bool:
        """Skip the rest of the run if the file for this month is not published yet."""
        return find_month_file_url(month) is not None

    @task
    def load_month(month: str) -> int:
        return load_trip_month(month)

    dbt_project = DbtTaskGroup(
        group_id="dbt_project",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
    )

    month = next_month()
    is_published(month) >> load_month(month) >> dbt_project


citibike_trips()