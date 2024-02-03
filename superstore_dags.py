import re

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

def fetch_data():

    db_name = 'airflow'
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
    db_port = '5432'


    connection = db.connect(
        database = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port
    )


    select_query = 'SELECT * FROM superstore;'
    df = pd.read_sql(select_query, connection)

    connection.close()

    df.to_csv('/opt/airflow/dags/superstore_raw.csv', index=False)

# Data Cleaning
def data_cleaning():
    df = pd.read_csv('/opt/airflow/dags/superstore_raw.csv')
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.strip().str.replace(' ', '_')
    df.columns = df.columns.str.strip().str.replace('-', '_')

    df.to_csv('/opt/airflow/dags/superstore_clean.csv', index=False)




# def queryPostgresql():
#     conn_string="dbname='airflow' host='localhost' user='airflow' password='airflow'"
#     conn=db.connect(conn_string)
#     df=pd.read_sql("select * from imdb_2023",conn)
#     df.to_csv('postgresqldata.csv')
#     print("-------Data Saved------")


def insertElasticsearch():
    es = Elasticsearch('http://elasticsearch:9200') 
    df=pd.read_csv('/opt/airflow/dags/superstore_clean.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="milestone3",doc_type="doc",body=doc)
        print(res)	


default_args = {
    'owner': 'daffa',
    'start_date': dt.datetime(2024, 1, 26),
    'retries': 5,
    'retry_delay': dt.timedelta(minutes=1),
}


with DAG('MyDBdag',
         default_args=default_args,
         schedule_interval='30 6 * * *',      # '30 6 * * *',
         ) as dag:

    getData = PythonOperator(task_id='QueryPostgreSQL',
                                 python_callable=fetch_data)
    
    cleanData = PythonOperator(task_id='cleandata',
                                 python_callable=data_cleaning)
    
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                 python_callable=insertElasticsearch)



getData >> cleanData >> insertData