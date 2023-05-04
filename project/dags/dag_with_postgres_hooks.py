import logging
from datetime import datetime, timedelta
from airflow import DAG
import csv
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=5)
}


def postgres_to_s3():
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date <='20220501'")
    with open('dags/get_orders.txt', 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info('Saved order into text file')


with DAG(
        dag_id='dag_with_postgres_hooks',
        default_args=default_args,
        start_date=datetime(2023, 3, 3),
        catchup=False,
        schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='postgres_to_s3',
        python_callable=postgres_to_s3
    )
    task1
