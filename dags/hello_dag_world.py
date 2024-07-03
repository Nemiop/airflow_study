from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'redbeast',
}

with DAG(
    dag_id = 'hello_world',
    description = "My first DAG 'Hello World!'",
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ["beginner", "bash", "hw"]
) as dag:

    task = BashOperator(
        task_id = "hello_world_task",
        bash_command = "echo With hello world again!"
    )

task
