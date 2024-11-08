from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.operators.empty import EmptyOperator
from cosmos.profiles import SnowflakesUserPasswordProfileMapping
from pendulum import datetime
import os

YOUR_NAME = ""
CONNECTION_ID = "snowflake_conn_id"
DB_NAME = "dbt_db"
SCHEMA_NAME = "dbt_schema"
MODEL_TO_QUERY = "model2"

# The path to the dbt project
DBT_PROJECT_PATH=f"{os.environ['AIRFLOW_HOME']}/dags/data_pipeline/dbt_project.yml"
# Path to the dbt executable in the virtual environment
DBT_EXECUTABLE_PATH=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakesUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"database": 'dbt_db','schema':'dbt_schema'},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    params={"my_name": YOUR_NAME},
)
def my_simple_dbt_dag():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "vars": '{"my_name": {{ params.my_name }} }',
        },
        default_args={"retries": 2},
    )

    start >> transform_data >> end

# Running the DAG
my_simple_dbt_dag()
