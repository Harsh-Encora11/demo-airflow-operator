from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=5)

}


with DAG(
        dag_id='postgres_operator',
        default_args=default_args,
        start_date=datetime(2023, 2, 2),
        schedule_interval='@daily',
        catchup=False

) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key(dt, dag_id)
            )
        
        """
    )
    task2 = PostgresOperator(
        task_id='insert_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ ds }}','{{ dag.dag_id }}')
        """
    )

    task1 >> task2
