from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from sqlalchemy import create_engine
from datetime import datetime, timedelta
import urllib
import pyodbc


## Название процедуры

proc_name = 'NM_CP_STARTER'


def init_starter(proc_name):
    ## Создаем подключение для обновления
    conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server}'+ \
            ';TrustServerCertificate=Yes'+ \
            ';Server=' + 'MSK1-BIDB01.synergy.local' + \
            ';Database=' + 'Analytic' + \
            f';UID={Variable.get("tuz_login")}' +  \
            f';PWD={Variable.get("tuz_password")}')
    # Открываем подключение
    cursor = conn.cursor() 
    try:
        # Выполняем обновление
        cursor.execute(f'exec {proc_name}')
        # Коммитим транзакцию
        conn.commit()
    finally:
        # Закрываем курсор и подключение
        cursor.close()
        conn.close()        


##########################################################################
# --------------------------- Настройка DAG ---------------------------- #
##########################################################################


default_args = {'owner': 'Nesh_Markoski',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="University_2035_ProcStarter",
        tags = ['CF/CP'],
        description = "Запуск процедур обновления витрин на BIDB01" ,
        schedule_interval='30 5 * * *', 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:


    init_starter = PythonOperator(
                    task_id='init_starter',
                    python_callable=init_starter,
                    op_kwargs={'proc_name': proc_name})

 
    init_starter