from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


def increment_by_2(value):
    print(f"Get value: {value}!")
    
    new_value = value + 2
    print(f"New value: {new_value}")

    return new_value


def multiply_by_100(ti):
    value = ti.xcom_pull(task_ids="t_increment")
    print(f"Get {value}!")

    new_value = value * 100
    print(f"New value: {new_value}")

    return new_value


def subtract_9(ti):
    value = ti.xcom_pull(task_ids="t_multiply")
    print(f"Get {value}!")

    new_value = value - 9
    print(f"New value: {new_value}")

    return new_value


def print_value(ti):
    value = ti.xcom_pull(task_ids="t_subtract")

    print(f"Final value: {value}")


default_args = {
    'owner' : 'redbeast',
}

with DAG(
    dag_id = 'ex6_xcom_cross_communication',
    description = "Send result ",
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = timedelta(days=1),
    tags = ["op_kwargs", "python"]
) as dag:
    
    task_increment = PythonOperator(
        task_id="t_increment",
        python_callable=increment_by_2,
        op_kwargs={"value": 1}
    )

    task_multiply = PythonOperator(
        task_id="t_multiply",
        python_callable=multiply_by_100
    )

    task_subtract = PythonOperator(
        task_id="t_subtract",
        python_callable=subtract_9
    )

    task_print = PythonOperator(
        task_id="t_print",
        python_callable=print_value
    )

task_increment >> task_multiply >> task_subtract >> task_print
