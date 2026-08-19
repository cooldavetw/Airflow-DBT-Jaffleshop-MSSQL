from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from pendulum import datetime
import os

from cosmos import ProfileConfig
from cosmos.operators.local import DbtRunOperationLocalOperator, DbtSeedLocalOperator
from cosmos.profiles import StandardSQLServerAuth

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_EXECUTABLE_PATH = os.environ.get("DBT_EXECUTABLE_PATH", "dbt")
DBT_PROJECT_DIR = f"{AIRFLOW_HOME}/dbt/jaffle_shop"
DBT_CONN_ID = os.environ.get("DBT_CONN_ID", "mssql")
DBT_DATABASE = os.environ.get("DBT_DATABASE")
DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "dbo")
DBT_DRIVER = os.environ.get("DBT_DRIVER", "ODBC Driver 18 for SQL Server")
DBT_TRUST_CERT = os.environ.get("DBT_TRUST_CERT", "true").lower() == "true"

profile_args = {
    "schema": DBT_SCHEMA,
    "driver": DBT_DRIVER,
    "trust_cert": DBT_TRUST_CERT,
}

if DBT_DATABASE:
    profile_args["database"] = DBT_DATABASE

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profile_mapping=StandardSQLServerAuth(
        conn_id=DBT_CONN_ID,
        profile_args=profile_args,
    ),
)

with DAG(
    dag_id="import-seeds",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "jaffle_shop", "mssql"],
    default_args={"retries": 2},
) as dag:

    project_seeds = [
        {
            "project": "jaffle_shop",
            "seeds": ["raw_customers", "raw_payments", "raw_orders"],
        }
    ]

    with TaskGroup(group_id="drop_seeds_if_exist") as drop_seeds:
        for project in project_seeds:
            for seed in project["seeds"]:
                DbtRunOperationLocalOperator(
                    task_id=f"drop_{seed}_if_exists",
                    macro_name="drop_table_in_schema_if_exists",
                    args={"table_name": seed},
                    project_dir=f"{AIRFLOW_HOME}/dbt/{project['project']}",
                    profile_config=profile_config,
                    dbt_executable_path=DBT_EXECUTABLE_PATH,
                )

    create_seeds = DbtSeedLocalOperator(
        task_id="jaffle_shop_seed",
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        outlets=[Dataset("SEED://JAFFLE_SHOP")],
    )

    drop_seeds >> create_seeds
