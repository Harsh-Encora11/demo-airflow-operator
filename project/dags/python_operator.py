from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=5)
}


def greet():
    print('Hello World')


with DAG(
    default_args=default_args,
    dag_id='python_operator',
    start_date=datetime(2023, 2, 2),
    schedule_interval='* * * * *'
)as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task1