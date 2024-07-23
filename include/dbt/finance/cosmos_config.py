from pathlib import Path

from cosmos.config import ProfileConfig, ProjectConfig

DBT_CONFIG = ProfileConfig(
    profile_name="finance",
    target_name="dev",
    profiles_yml_filepath=Path("/opt/airflow/include/dbt/finance/profiles.yml"),
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path="/opt/airflow/include/dbt/finance/",
)
