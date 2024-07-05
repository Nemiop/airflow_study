from datetime import timedelta
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


def read_csv_file():
    df = pd.read_csv("/home/ruslan/airflow/datasets/insurance.csv")
    print(df)
    return df.to_json()


def remove_null_values(**kwargs):
    ti = kwargs['ti']

    json_data = ti.xcom_pull(task_ids="read_csv_file")
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    return df.to_json()


default_args = {
    'owner' : 'redbeast',
}

with DAG(
    dag_id = 'ex7_pandas_pipeline',
    description = "Running a pipeline",
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ["python", "transform", "pipeline"]
) as dag:
    
    task_read_file = PythonOperator(
        task_id="read_scv_file",
        python_callable=read_csv_file
    )

    task_remove_null = PythonOperator(
        task_id="remove_null_values",
        python_callable=remove_null_values
    )

task_read_file >> task_remove_null