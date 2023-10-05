# Airflow modules import
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

# Base modules import
import requests as r
import pandas as pd 
import openpyxl
import urllib
from datetime import datetime, timedelta
from sqlalchemy import create_engine



# Переменные
file_pth = r'/opt/airflow/lk_2035/ЗачисленияЛК.xlsx'
tablename = 'NM_ETL_TEST_Airflow'


def sqlserver_connection():

    """ Создание подключения """
    
    # Драйвер для подклбчения к БД
    driver = 'Driver={ODBC Driver 18 for SQL Server}' + \
                ';TrustServerCertificate=Yes'+ \
                ';Server=' + 'MSK1-ADB01.synergy.local' + \
                ';Database=' + 'Analytic' + \
                f';UID={Variable.get("tuz_login")}' +  \
                f';PWD={Variable.get("tuz_password")}'
    params = urllib.parse.quote_plus(driver)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    return engine


# Выгружаем файл и добавляем SYSMOMENT
def get_file_to_db(file_pth,tablename):
    
    xls_df = pd.read_excel(file_pth)
    xls_df['SYSMOMENT']= datetime.now()
    xls_df.to_sql(tablename,con=sqlserver_connection(),if_exists='replace', index=False)    




# ------------ Определение DAG ------------ #


default_args = {'owner': 'Markoski_Nesh',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="NM_Test_ETL",
        tags = ['NM'],
        description = "Тестовый скрипт NM" ,
        schedule_interval=None, 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:

    nm_test = PythonOperator(
                    task_id='test_etl',
                    python_callable=get_file_to_db,
                    op_kwargs={'tablename' : tablename,
                                'file_pth':file_pth}
                    )

    nm_test