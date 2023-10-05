from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import time
import pandas as pd
from sqlalchemy import create_engine
import urllib


## Путь к файлу для отправки клинтов в АКАДА
file_path_AKADA = '/var/lib/documents/88.das/03.Общая/ЛК_2035/ETL/ОтправкаАКАДА.csv'
## Таблица для загрузки данных в БД на отправку в АКАДА
table_name_AKADA = 'NM_ETL_AKADA_WhiteList'

## Путь к файлу для процесса зачисления в ЛК2035
file_path_Approve = '/var/lib/documents/88.das/03.Общая/ЛК_2035/ETL/ЗачисленияЛК.csv'
## Таблица для загрузки данных в БД на зачисление в ЛК2035
table_name_Approve = 'NM_ETL_Approve_WhiteList'


## Подключение к серверу
def db_conn():
    # Драйвер для подключения к БД
    driver = 'Driver={ODBC Driver 18 for SQL Server}'+ \
            ';TrustServerCertificate=Yes'+ \
            ';Server=' + 'MSK1-BIDB01.synergy.local' + \
            ';Database=' + 'Analytic' + \
            f';UID={Variable.get("tuz_login")}' +  \
            f';PWD={Variable.get("tuz_password")}'

    params = urllib.parse.quote_plus(driver)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    return engine


def insert_akada(file_path_AKADA,table_name_AKADA):
    print('Starting insert_akada')
    df_akada = pd.read_csv(file_path_AKADA)
    df_akada_old = pd.read_sql_table(table_name_AKADA,con=db_conn(),columns=["DEAL_ID"])
    df_akada = pd.concat([df_akada,df_akada_old]).drop_duplicates(keep=False)
    df_akada['SYSMOMENT'] = datetime.now()
    df_akada.to_sql(table_name_AKADA,con=db_conn(),index=False,if_exists='append')


def insert_approve(file_path_Approve,table_name_Approve):
    df_approve = pd.read_csv(file_path_Approve,encoding='windows-1251',delimiter=';')
    df_approve['SYSMOMENT'] = datetime.now()
    ##df_approve_old = pd.read_sql_table(table_name_Approve,con=db_conn(),columns=["id_deal"])
    ##df_approve = pd.concat([df_approve,df_approve_old],ignore_index=True, sort =False).drop_duplicates(['id_deal'],keep=False)
    df_approve.to_sql(table_name_Approve,con=db_conn(),index=False,if_exists='replace')



##########################################################################
# --------------------------- Настройка DAG ------------------------------ #
##########################################################################


default_args = {'owner': 'Nesh_Markoski',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="University_2035_ETL_WhiteList",
        tags = ['CF/CP'],
        description = "Загрузка Excel файлов из общего диска для одобрения и зачисления клиентов" ,
        schedule_interval='50 5,9,14 * * *', 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:


    insert_akada = PythonOperator(
                    task_id='insert_akada',
                    python_callable=insert_akada,
                    op_kwargs={'file_path_AKADA':file_path_AKADA,
                              'table_name_AKADA':table_name_AKADA})

    insert_approve = PythonOperator(
                    task_id='insert_approve',
                    python_callable=insert_approve,
                    op_kwargs={'file_path_Approve':file_path_Approve,
                              'table_name_Approve':table_name_Approve})
 
    insert_akada >> insert_approve