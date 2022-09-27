from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


config = configparser.ConfigParser()
config.read('/opt/airflow/dags/config.cfg')

default_args = {
    'owner': 'kirill',
    'start_date': datetime(2021, 11, 11),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

def table_creator():
    with open('/opt/airflow/dags/create_tables.sql', 'r') as create_query:
        conn = psycopg2.connect(**conn_args)
        cur = conn.cursor()
        cur.execute(create_query.read())
        conn.commit()
        cur.close()
        conn.close()

conn_args=dict(
    host=config['DB']['HOST'],
    user=config['DB']['USER'],
    password=config['DB']['PASSWORD'],
    dbname=config['DB']['DB_NAME'],
    port=config['DB']['PORT'])

with DAG('create_table_dag',
             start_date=datetime.now(),
             default_args=default_args) as dag:
    PythonOperator(
        task_id='create_tables',
        provide_context=True,
        python_callable=table_creator,
        dag=dag)
