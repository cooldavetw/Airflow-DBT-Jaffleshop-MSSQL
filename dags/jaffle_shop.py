from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
from cosmos import DbtDag, ProfileConfig, ProjectConfig
from cosmos.profiles import StandardSQLServerAuth



AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_EXECUTABLE_PATH = os.environ.get("DBT_EXECUTABLE_PATH", "dbt")
DBT_PROJECT_DIR = f"{AIRFLOW_HOME}/dbt/jaffle_shop"
DBT_CONN_ID = os.environ.get("DBT_CONN_ID", "mssql")
DBT_DATABASE = os.environ.get("DBT_DATABASE")
DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "dbo")
DBT_DRIVER = os.environ.get("DBT_DRIVER", "ODBC Driver 18 for SQL Server")
DBT_TRUST_CERT = os.environ.get("DBT_TRUST_CERT", "true").lower() == "true"


def get_dbt_database():
    if DBT_DATABASE:
        return DBT_DATABASE

    try:
        return BaseHook.get_connection(DBT_CONN_ID).schema
    except Exception:
        return None

profile_args = {
    "schema": DBT_SCHEMA,
    "driver": DBT_DRIVER,
    "trust_cert": DBT_TRUST_CERT,
}

dbt_database = get_dbt_database()
if dbt_database:
    profile_args["database"] = dbt_database

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profile_mapping=StandardSQLServerAuth(
        conn_id=DBT_CONN_ID,
        profile_args=profile_args,
    ),
)

jaffle_shop = DbtDag(
    dag_id="jaffle_shop",
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    profile_config=profile_config,
    start_date=datetime(2023, 1, 1),
    schedule=[Dataset("SEED://JAFFLE_SHOP")],
    catchup=False,
    tags=["dbt", "jaffle_shop", "mssql"],
    default_args={"retries": 2},
    operator_args={
        "dbt_executable_path": DBT_EXECUTABLE_PATH,
    },
)

jaffle_shop
