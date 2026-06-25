from airflow.datasets import Dataset
from datetime import datetime
import os
from cosmos import DbtDag, ProfileConfig, ProjectConfig
from cosmos.profiles import StandardSQLServerAuth



AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_EXECUTABLE_PATH = os.environ.get("DBT_EXECUTABLE_PATH", "dbt")
DBT_PROJECT_DIR = f"{AIRFLOW_HOME}/dbt/jaffle_shop"
DBT_CONN_ID = os.environ.get("DBT_CONN_ID", "mssql")
DBT_DATABASE = os.environ.get("DBT_DATABASE", "jaffle_shop")
DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "dbo")
DBT_DRIVER = os.environ.get("DBT_DRIVER", "ODBC Driver 18 for SQL Server")

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profile_mapping=StandardSQLServerAuth(
        conn_id=DBT_CONN_ID,
        profile_args={
            "database": DBT_DATABASE,
            "schema": DBT_SCHEMA,
            "driver": DBT_DRIVER,
        },
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
