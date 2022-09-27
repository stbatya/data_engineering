from airflow.models import DAG
import sys
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from spark_funcs import process_log_data, process_song_data, create_spark_session
import configparser
from datetime import datetime, timedelta
import logging

config = configparser.ConfigParser()
config.read('/opt/airflow/dags/config.cfg')

s3 = config['AWS']['AWS_CONN_ID']
bucket = config['S3']['DEST_S3_BUCKET']
spark_master = config['SPARK']['MASTER_ADDRESS']


default_args = {
    'owner': 'kirill',
    'start_date': datetime(2021, 11, 11),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

session = create_spark_session(spark_master)

with DAG('my_spark_dag',
          default_args=default_args,
          description='Load and transform data into parquet files with Spark and Airflow',
          end_date=datetime(2022, 11, 11),
          schedule_interval=None
        ) as dag:

        start_operator = DummyOperator(task_id='Begin_execution')
        
        process_song = PythonOperator(
                    python_callable=process_song_data,
                    op_args = [session, 's3a://'+bucket, 's3a://'+bucket],
                    task_id = 'process_songs'
        )

        process_log = PythonOperator(
                    python_callable=process_log_data,
                    op_args = [session, 's3a://'+bucket, 's3a://'+bucket],
                    task_id = 'process_logs'
        )


        end_operator = DummyOperator(task_id='End_execution')

        start_operator >> process_song >> process_log >> end_operator
