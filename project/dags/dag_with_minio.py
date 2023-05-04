from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow import DAG

default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=5)

}

with DAG(
    dag_id='dag_with_minio',
    start_date=datetime(2023, 3, 3),
    schedule_interval='@daily'
)as dag:
    task1 = S3KeySensor(
        task_id='sensor_minio',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn'
    )
