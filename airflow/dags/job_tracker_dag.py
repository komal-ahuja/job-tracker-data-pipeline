from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 9),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'job_tracker_dag', 
    default_args=default_args, 
    schedule=None, 
    catchup=False)

# Task to invoke the Lambda function
lambda_invoke_task = LambdaInvokeFunctionOperator(
    task_id='invoke_lambda_function',
    function_name=Variable.get("lambda_function_name"), 
    aws_conn_id='aws_conn',
    region_name=Variable.get("aws_region"),
    dag=dag
)


lambda_invoke_task




