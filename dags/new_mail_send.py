from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
import plugins.Disp_Pivot_Table 
# Создаем DAG(контейнер) в который поместим наши задачи
# Для DAG-а характерны следующие атрибуты
# - Интервал запусков
# - Начальная точка запуска

default_args = {'owner': 'Dubai_Omar',
                'depends_on_past': False,
                'retries': 0,
                #'retry_delay': None
                }


with DAG(dag_id='disp_report',
         description = 'Выгрузка агрегированного отчета по оценкам диспетчеров',

         schedule_interval=None, # Интервал запусков
         start_date=days_ago(1), # Начальная точка запуска
         catchup = False,
         default_args=default_args,
    ) as dag:

 


    # Создадим задачу которая будет отправлять файл на почту
    email_op = EmailOperator(
        task_id='send_email',
        to="DDubai@synergy.ru",
        subject="Test Email Please Ignore",
        html_content='some test text',
        files=['/opt/airflow/temporary_files/requirements.txt']
    )

    # Создадим порядок выполнения задач
    # В данном случае 2 задачи буудт последователньы и ещё 2 парараллельны
    email_op