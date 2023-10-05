from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import urllib
import MySQLdb
import warnings
warnings.filterwarnings("ignore")


proc_buffer_dwh = 'dbo.pp_STT_CallList_Buffer_Accounts'
proc_query_maria = 'CallList_Query_Generator_Accounts'

table_buffer_dwh = 'STT_CallList_Buffer_Accounts'
table_buffer_maria = 'STT_CallList_Buffer_Accounts'



def dwh_conn():
        driver = 'Driver={ODBC Driver 18 for SQL Server}'+ \
            ';TrustServerCertificate=Yes'+ \
            ';Server=' + 'MSK1-BIDB01.synergy.local' + \
            ';Database=' + 'Analytic' + \
            f';UID={Variable.get("tuz_login")}' +  \
            f';PWD={Variable.get("tuz_password")}'
        
        params = urllib.parse.quote_plus(driver)
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
        return engine

def maria_conn():
        engine_maria = create_engine("mariadb://{user}:{password}@msk1-ml02:3306/analytic"
                        .format(user=Variable.get("tuz_login"),
                                password=Variable.get("tuz_password")))
        return engine_maria

def exec_proc(proc_name, conn, type):
    connection = conn.raw_connection()
    try:
        cursor = connection.cursor()
        if type == 'maria':
            cursor.execute(f'CALL analytic.{proc_name}()')
        elif type == 'dwh':
            cursor.execute(f'exec {proc_name}')
        else:
            print('error')
        cursor.close()
        connection.commit()
    finally:
        connection.close() 

def truncate_table(table_name , conn):
    connection = conn.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'truncate table {table_name}')
        cursor.close()
        connection.commit()
    finally:
        connection.close()             


def query_generator_accounts(proc_buffer_dwh,table_buffer_dwh,table_buffer_maria,proc_query_maria):
    
    ## Вызов процедуры генерации таблицы списка сотрудников в BIDB01
    exec_proc(proc_buffer_dwh,dwh_conn(),'dwh')
    
    ## Выгружаем таблицу в df
    df = pd.read_sql_table(table_buffer_dwh,con=dwh_conn())

    ## truncate buffer in bidb01
    truncate_table(table_buffer_dwh,dwh_conn())

    ## Truncate maria table
    truncate_table(table_buffer_maria,maria_conn())

    ## Грузим df в mariaDB
    df.to_sql(table_buffer_maria,con=maria_conn(),if_exists='append',index=False)

    ## Запуск процедуры генератора очереди в mariaDB
    exec_proc(proc_query_maria, maria_conn(),'maria')   





# --------------------------- Настройка DAG ------------------------------ #
default_args = {'owner': 'Nesh_Markoski',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="STT_CallList_Query_Accounts",
        tags = ['STT','ETL'],
        description = "Формирование очередей для транскрибации аудио Деп.Счастья" ,
        schedule_interval='0 7 * * *', 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:


    query_generator_accounts = PythonOperator(
                    task_id='query_generator_accounts',
                    python_callable=query_generator_accounts,
                    op_kwargs={'proc_buffer_dwh': proc_buffer_dwh,
                              'table_buffer_dwh':table_buffer_dwh,
                              'table_buffer_maria':table_buffer_maria,
                              'proc_query_maria':proc_query_maria})

 
    query_generator_accounts           