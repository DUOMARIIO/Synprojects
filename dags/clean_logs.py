from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

""" Очистка логов AIRFLOW старше 30 дней """

default_args = {'owner': 'Samigullin_Ildus',
                'depends_on_past': False}      # Запуск вне зависимости от статуса прошедших запусков

with DAG(dag_id='Clean_logs',
        tags = ['service', 'msk1-ml02'],
        description = 'Очистка логов airflow' ,
        schedule_interval='0 0 * * 1',     
        start_date=days_ago(7),            
        catchup=False,
        default_args=default_args) as dag:
    
    clean_logs = BashOperator(task_id='clean_logs',
                              bash_command='find /opt/airflow/logs -name "*.log" -type f -mtime +30 -delete')
    clean_logs