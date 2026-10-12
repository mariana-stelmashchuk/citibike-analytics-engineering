"""Daily: snapshot GBFS station information and rebuild station models."""

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
from include.citibike.stations import load_station_snapshot


@dag(
    schedule="0 6 * * *",  # every day at 06:00 UTC
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["citibike", "stations"],
)
def citibike_stations_daily():

    @task
    def load_stations() -> int:
        return load_station_snapshot()

    dbt_stations = DbtTaskGroup(
        group_id="dbt_stations",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["stg_citibike__station_information+"],
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
    )

    load_stations() >> dbt_stations


citibike_stations_daily()