from datetime import datetime, timedelta
from airflow import dag, task

default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=5)

}


@dag(
    dag_id='taskflow_api',
    default_args=default_args,
    start_date=datetime(2023, 2, 2),
    schedule_interval='@daily',
    catchup=False,
    )
def hello_world_etl():
    @task
    def get_name():
        return 'Jerry'

    @task
    def get_age():
        return 20

    @task
    def greet(name, age):
        print(f'My name is {name}'
              f'and age is {age}')
    name = get_name()
    age = get_age()
    greet(name=name,age=age)

greet_dag=hello_world_etl()
