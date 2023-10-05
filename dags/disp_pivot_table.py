# Base libs
import pandas as pd
from datetime import datetime, timedelta

# My modules
from plugins.Disp_Pivot_Table import Disp_Pivot_Table
from plugins.config import Queries

# Airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import  PythonOperator



#--------------------- Основные методы ---------------------#

def make_disp_pivot():

	pivot = Disp_Pivot_Table(Queries.query_for_full_df)

	pivot.tech_pivot_exctration(query_disp_dict = Queries.query_disp_dict,
								disp_dict_list = ['ФИО', 'Департамент', 'Структурное подразделение 1', 
											'Структурное подразделение 2', 'Структурное подразделение 3'])
	pivot.tr_pivot_exctration()
	pivot.disp_pivot_connection()
	pivot.total_pivot_exctration()
	pivot.record_quality_pivot(Queries.query_for_conf, ['Lead_id', 'Качество записи'], conf_percent=30)

	result_path = '/opt/airflow/disp_pivot/' # поменяй тут название пути когда системную папку примонтируют 

	result_file_name = f'{result_path}Оценка качества {datetime.now().strftime("%Y_%m_%d_%H_%M")}.xlsx' 
	pivot.df.to_excel(result_file_name, sheet_name='Оценки качества обработки лидов')
	writer = pd.ExcelWriter(result_file_name, engine = 'openpyxl', mode='a')
	pivot.total_pivot.to_excel(writer, sheet_name = 'Сводная оценка по всем метрикам')
	pivot.disp_pivot.to_excel(writer, sheet_name = 'Сводная оценка по диспетчерам')
	writer.close()

# ------------ Определение DAG ------------ #

default_args = {'owner': 'Dubai Omar',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(seconds=120)
                }


with DAG(dag_id="Disp_pivot_table",
        tags = ['STT'],
        description = "Оценка качества разговора диспетчера с лидом." ,
        schedule_interval= '0 6 * * 1', # на самом деле дага запускается в 9 утра GMT+3
        start_date= days_ago(7),
        catchup=True,
        default_args=default_args) as dag:


	makepivot = PythonOperator(
                    task_id="key_word_checker",
                    python_callable=make_disp_pivot)
	makepivot