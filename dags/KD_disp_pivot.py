from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from docker.types import Mount

"""
Оценка разговров диспетчеров.
Пайплайн основан на использовании Docker image проекта https://lab.syndev.ru/ml/speechtotext/-/tree/master/transcribation_check
Перед запуском DAG необходимо сделать build образа проекта
"""


# ------------------ Определение DAG ------------------ #

default_args = {'owner': 'Samigullin_Ildus',
                'depends_on_past': False}      # Запуск вне зависимости от статуса прошедших запусков

with DAG(dag_id='KD_Disp_pivot',
        tags = ['STT', 'STT_disp', 'docker', 'KD'],
        description = 'Формирование отчета по диспетчерам КД' ,
        schedule_interval='0 6 * * 1',     
        start_date=days_ago(14),            
        catchup=False,
        default_args=default_args) as dag:

    task = DockerOperator(
            task_id='run_app_in_container',             # Название task
            image='disp_tr_check:3',                    # Название и тег запускаемого образа
            command='python /app/main.py',              # Запускаемая команда внутри контейнера
            docker_url='unix://var/run/docker.sock',    
            network_mode='bridge',                      # Тип docker-network с каким будет подниматься контейнер
            auto_remove=True,                           # Удаление поднятого контейнера после исполнения кода
                                                        # Прокидывание volume в контейнер
            mounts=[Mount(source='/mnt/disp_tr_check/base',
                     target='/base', type='bind'),
                    Mount(source='/mnt/disp_tr_check/logs',
                     target='/logs', type='bind'),
                    Mount(source='/mnt/disp_tr_check/KD_pivot',
                     target='/app', type='bind'),
                    Mount(source='/var/lib/documents/88.das/08.Речевая аналитика',
                     target='/Result', type='bind')]
            )
    task
    