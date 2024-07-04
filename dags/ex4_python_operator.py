from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


def print_function():
    print("Hello PythonOperator with python_callable!")


default_args = {
    'owner' : 'redbeast',
}

with DAG(
    dag_id = 'ex4_python_operator',
    description = "Call python function as task",
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = timedelta(days=1),
    tags = ["beginner", "python"]
) as dag:
    taskA = PythonOperator(
        task_id="TaskA",
        python_callable=print_function
    )

taskA
