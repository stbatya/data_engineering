from airflow.hooks.S3_hook import S3Hook
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from insert_queries import SqlQueries
import configparser
import json
import logging
import psycopg2
import sys

'''This DAG connects to S3 bucket, downloads log and song data (simulated preliminarily)
to the staging tables and then transforms those staging tables into
the star chema(fact table + dimensional tables). Finally, it checks if data was inserted.
'''


dim_tabs = ['users', 'songs', 'artists', 'time']
fact_tab = ['songfact']
dim_tab_dict = {name: getattr(SqlQueries, name+'_table_insert') for name in dim_tabs}
fact_tab_dict = {name: getattr(SqlQueries, name+'_table_insert') for name in fact_tab}

config = configparser.ConfigParser()
config.read('/opt/airflow/dags/config.cfg')

s3 = config['AWS']['AWS_CONN_ID']
bucket = config['S3']['DEST_S3_BUCKET']

default_args = {
    'owner': 'kirill',
    'start_date': datetime(2021, 11, 11),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

conn_args=dict(
            host=config['DB']['HOST'],
            user=config['DB']['USER'],
            password=config['DB']['PASSWORD'],
            dbname=config['DB']['DB_NAME'],
            port=config['DB']['PORT'])

def prepare(aws_credentials_id, conn_args, bucket, folder, table):
    '''Loads data from S3 bucket into the staging tables.
    '''
    s3 = S3Hook(aws_conn_id=aws_credentials_id)
    conn = psycopg2.connect(**conn_args)
    cur = conn.cursor()
    cur.execute("TRUNCATE {};".format(table))
    key_lst = s3.list_keys(bucket, prefix=folder)
    if key_lst:
        for key in key_lst:
            if '.json' in key:
                data = s3.read_key(key, bucket)
                file = data.splitlines()
                lines = (line for line in file)
                data_table = [json.loads(line) for line in lines]
                copy_sql = "INSERT INTO {table}({cols}) values %s;"
                columns = data_table[0].keys()
                query = copy_sql.format(cols=','.join(columns), table=table)
                values = ((None if d[column]=='' else d[column] for column in columns) for d in data_table)
                cur.executemany(query, [(tuple(row),) for row in values])
                conn.commit()
    else:
        logging.info('failed to load keys')
    cur.close()
    conn.close()

def load(conn_args, table, query, mode='update',):
    ''' Inserts data selected from given query into the table connected with conn_args.
    if mode is 'delete' then truncates target table before insertion.
    '''
    logging.info("Connecting to database")
    conn = psycopg2.connect(**conn_args)
    cur = conn.cursor()
    if mode == "delete":
        logging.info("deleting data in table")
        cur.execute("TRUNCATE {};".format(table))
    logging.info("inserting into table")
    cur.execute("INSERT INTO {} {};".format(table, query))
    conn.commit()
    cur.close()
    conn.close()
    logging.info('everything went okay')

def check(conn_args, tables):
    '''Check that tables is not empty.
    '''
    conn = psycopg2.connect(**conn_args)
    cur = conn.cursor()
    for table in tables:
        cur.execute("SELECT COUNT(*) FROM {};".format(table))
        records = cur.fetchall()
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. {} returned no results".format(table))
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. {} contained 0 rows".format(table))
        logging.info("Data quality on table {} check passed with {} records".format(table, records[0][0]))

with DAG('my_etl_dag',
          default_args=default_args,
          description='Load and transform data into PostreSQL with Airflow',
          end_date=datetime(2022, 11, 11),
          schedule_interval=None
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')

    data = 'log_data/'
    prepare_events = PythonOperator(
            python_callable=prepare,
            op_args=[s3, conn_args, bucket, data, 'prepared_events'],
            task_id='prepare_events',
    )
    
    data = 'song_data/'
    prepare_songs = PythonOperator(
            python_callable=prepare,
            op_args=[s3, conn_args, bucket, data, 'prepared_songs'],
            task_id='prepare_songs',
    )

    start_operator >> [prepare_events, prepare_songs]

    table, query = next(iter(fact_tab_dict.items()))
    load_fact_table = PythonOperator(
        python_callable=load,
        op_args=[conn_args, table, query, 'upload'],
        task_id='load_fact_'+table
    )

    [prepare_events, prepare_songs] >> load_fact_table

    for table, query in dim_tab_dict.items():
        load_dim_tables = PythonOperator(
            python_callable=load,
            op_args=[conn_args, table, query, 'delete'],
            task_id='load_dim_'+table
        )

        load_fact_table >> load_dim_tables

    quality_checks = PythonOperator(
        python_callable=check,
        op_args=[conn_args, dim_tabs+fact_tab],
        task_id='qulity_checks'
    )

    load_dim_tables >> quality_checks

    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

    quality_checks >> end_operator
