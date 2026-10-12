"""Shared Cosmos settings for running the citibike dbt project from Airflow."""

from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import InvocationMode

DBT_PROJECT_PATH = "/usr/local/airflow/dbt/citibike_dbt"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

project_config = ProjectConfig(DBT_PROJECT_PATH)

profile_config = ProfileConfig(
    profile_name="citibike_dbt",
    target_name="prod",
    profiles_yml_filepath="/usr/local/airflow/include/dbt_profiles/profiles.yml",
)

# Run dbt as a separate program from its own virtualenv, not as a library inside Airflow,
# so dbt and Airflow dependencies cannot conflict.
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
    invocation_mode=InvocationMode.SUBPROCESS,
)