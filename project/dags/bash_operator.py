from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': '5',
    'retry_delay': timedelta(minutes=5)
}
with DAG(
        dag_id='bash_operator',
        description='My first Dag',
        start_date=datetime(2023, 2, 2),
        schedule_interval='@daily',
        default_args=default_args
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello bash operator"
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey, I am task2"
    )

    task1>>task2
