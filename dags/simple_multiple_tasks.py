from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'redbeast',
}

with DAG(
    dag_id = 'simple_multiple_tasks',
    description = "Call multiple tasks and dependencies",
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ["beginner", "bash", "multiple"]
) as dag:

    taskA = BashOperator(
        task_id = "taskA",
        bash_command = "echo TASK A has executed"
    )

    taskB = BashOperator(
        task_id = "taskB",
        bash_command = "echo TASK B has executed"
    )

taskA.set_downstream(taskB)
