from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from docker.types import Mount

'''
Автоматизация предобработки отчета по Лубянскому гримеру
'''

# ------------------ Определение DAG ------------------ #

default_args = {'owner': 'Dubai Omar',
                'depends_on_past': False}      # Запуск вне зависимости от статуса прошедших запусков

with DAG(dag_id='grimer',
        tags = ['grimer','docker'],
        description = 'Автоматизация предобработки отчета по Лубянскому гримеру' ,
        schedule_interval='0 7 * * *',     
        start_date=days_ago(1),            
        catchup=False,
        default_args=default_args) as dag:

    task = DockerOperator(
            task_id='grimer_automatization',             # Название task
            image='grimer_img:4',            # Название и тег запускаемого образа
            command='python /grimer_auto/app/main.py', # Запускаемая команда внутри контейнера
            docker_url='unix://var/run/docker.sock',    
            network_mode='bridge',                      # Тип docker-network с каким будет подниматься контейнер
            auto_remove=True,                           # Удаление поднятого контейнера после исполнения кода
                                                        # Прокидывание volume в контейнер
            mounts=[Mount(source='/var/lib/documents/88.das/03.Общая/lubyanskiy_grimer/', target='/grimer_auto/files_for_proc/', type='bind')]
                        )
    task
    