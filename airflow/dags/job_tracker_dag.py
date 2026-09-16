from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.sensors.python import PythonSensor
import time
import json
from airflow.models import Variable
from datetime import datetime, timedelta
from pathlib import Path


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 9),
    'email_on_failure': False,
    'email_on_retry': False
}

DBT_PROJECT_PATH = Path("/opt/airflow/dbt")

dag = DAG(
    'job_tracker_dag', 
    default_args=default_args, 
    schedule=None, 
    catchup=False)

def wait_for_snowflake_data(**kwargs):
    """
    Waits for data to be available in Snowflake before proceeding.
    """
    ti = kwargs['ti']
    lambda_response = ti.xcom_pull(task_ids='invoke_lambda_function')
    print(f"Waiting for data in Snowflake for files: {lambda_response}")
    #parsing json data from the Lambda response
    lambda_response = json.loads(lambda_response)
    body = json.loads(lambda_response["body"])
    filenames = body["objects"]

    if not filenames:
        print("No new files to wait for.")
        return True

    # Snowflake connection
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    placeholders = ', '.join(["%s"] * len(filenames))
    query = f"""Select filename from JOB_DB.RAW.RAW_JOBS_API where filename in ({placeholders})"""
    records = hook.get_records(query, parameters=filenames)
    loaded_filenames = [record[0] for record in records]
    print(f"Snowflake has loaded {len(loaded_filenames)}/{len(filenames)} expected files.")

    return set(loaded_filenames) == set(filenames)
       
       

dbt_transformations = DbtTaskGroup(
    group_id="dbt_transformations",
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=ProfileConfig(
        target_name="dev",
        profile_name="default",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_conn",
        ),
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path="/home/airflow/.local/bin/dbt",  
    ),
    dag=dag,
)

# Task to invoke the Lambda function
lambda_invoke_task = LambdaInvokeFunctionOperator(
    task_id='invoke_lambda_function',
    function_name=Variable.get("lambda_function_name"), 
    aws_conn_id='aws_conn',
    region_name=Variable.get("aws_region"),
    dag=dag
)

wait_for_snowflake_task = PythonSensor(
    task_id='wait_for_snowflake_data',
    python_callable=wait_for_snowflake_data,
    mode='reschedule',
    poke_interval=30,
    timeout=600,
    dag=dag
)

lambda_invoke_task >> wait_for_snowflake_task >> dbt_transformations




