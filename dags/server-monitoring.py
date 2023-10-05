from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from docker.types import Mount
import docker

"""
Оценка разговров диспетчеров.
Пайплайн основан на использовании Docker image проекта https://lab.syndev.ru/ml/speechtotext/-/tree/master/transcribation_check
Перед запуском DAG необходимо сделать build образа проекта
"""


# ------------------ Определение DAG ------------------ #

default_args = {'owner': 'Samigullin_Ildus',
                'depends_on_past': False}      # Запуск вне зависимости от статуса прошедших запусков

with DAG(dag_id='msk1-ml02_server_monitoring',
        tags = ['msk1-ml02', 'monitoring', 'docker'],
        description = 'Мониторинг загруженности сервера msk1-ml02' ,
        schedule_interval='0 * * * *',     
        start_date=days_ago(1),            
        catchup=False,
        default_args=default_args) as dag:

    task = DockerOperator(
            task_id='run_monitoring_in_container',             # Название task
            image='server_usage_monitoring:1',                # Название и тег запускаемого образа
            command='python /app/server_monitoring.py',              # Запускаемая команда внутри контейнера
            docker_url='unix://var/run/docker.sock',    
            network_mode='bridge',                      # Тип docker-network с каким будет подниматься контейнер
            auto_remove=True,                           # Удаление поднятого контейнера после исполнения кода
                                                        # Прокидывание volume в контейнер
            mounts=[Mount(source='/home/isamigullin/base/create_log.py',
                          target='/app/create_log.py', type='bind')],
            device_requests=[docker.types.DeviceRequest(count=-1, capabilities=[['gpu']])]
            )
    task    