import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_data_from_postgresql():
  '''
  The function is used to retrieve data from a PostgreSQL database.
  '''
  conn_string = "dbname='airflow' host='postgres' port='5432' user='airflow' password='airflow'"
  conn = db.connect(conn_string)
  df = pd.read_sql("select * from table_m3", conn)
  df.to_csv('/opt/airflow/dags/P2M3_trisna_data_raw.csv',index=False)



def clean_data():
  '''
  This function is used to perform Data Cleaning processes and save the cleaned data to a CSV file.
  '''
  df = pd.read_csv('/opt/airflow/dags/P2M3_trisna_data_raw.csv')
  df = df.drop_duplicates()
  df = df.fillna(0)
  df.columns = df.columns.str.lower().str.replace(' ', '_', regex=True)
  df.to_csv('/opt/airflow/dags/P2M3_trisna_data_clean.csv', index=False)



def import_csv_to_elasticsearch():
  '''
  This function is used to load a CSV file containing cleaned data and insert it into Elasticsearch.
  '''
  es = Elasticsearch('http://elasticsearch:9200')
  df = pd.read_csv('/opt/airflow/dags/P2M3_trisna_data_clean.csv')

  for i, r in df.iterrows():
    doc = r.to_json()
    res = es.index(index="milestone", doc_type="doc", body=doc)
    print(res)


# DAG setup
default_args = {
    'owner': 'trisna',
    'start_date': dt.datetime(2023, 10, 27, 14, 0, 0) - dt.timedelta(hours=7),
    'retries':1,
    'retry_delay': dt.timedelta(minutes=3),
}

with DAG('M3DAG',
         default_args=default_args,
         schedule_interval='30 6 * * *',
        ) as dag:
    
    # Task to fetch data from Postgresql
    fetch_task = PythonOperator(
        task_id='get_data_from_postgresql',
        python_callable=get_data_from_postgresql
    )
    
    # Task to clean data
    clean_task = PythonOperator(
        task_id='clean_dataframe',
        python_callable=clean_data
    )
    
    # Task to post to Elasticsearch
    post_to_kibana_task = PythonOperator(
        task_id='post_to_kibana',
        python_callable=import_csv_to_elasticsearch
    )
    
    
# Set task dependencies
fetch_task >> clean_task >> post_to_kibana_task 