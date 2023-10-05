from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from docker.types import Mount

'''
Мониторинг работы stt пайплайна на возникающие ошибки и ненайденные звонки
'''

# ------------------ Определение DAG ------------------ #

default_args = {'owner': 'Dubai Omar',
                'depends_on_past': False}      # Запуск вне зависимости от статуса прошедших запусков

with DAG(dag_id='STT_monitoring',
        tags = ['STT', 'monitoring','docker'],
        description = 'Мониторинг работы stt пайплайна' ,
        schedule_interval='0 */1 * * *', 
        max_active_runs = 1,    
        start_date=days_ago(1),            
        catchup=False,
        default_args=default_args) as dag:

    task = DockerOperator(
            task_id='stt_pipeline_monitoring_in_container',             # Название task
            image='stt_monitoring_img:1',            # Название и тег запускаемого образа
            command='python main.py', # Запускаемая команда внутри контейнера
            docker_url='unix://var/run/docker.sock',    
            network_mode='bridge',                      # Тип docker-network с каким будет подниматься контейнер
            auto_remove=True,                           # Удаление поднятого контейнера после исполнения кода
                                                        # Прокидывание volume в контейнер
            mounts=[Mount(source='/mnt/Transcribation_pipeline/pipeline_logs/',
                           target = '/app/pipeline_logs/', 
                           type = 'bind'),
                    Mount(source='/mnt/stt_pipeline_monitoring/stt_monitoring/pipeline_monitoring_files/',
                           target='/app/pipeline_monitoring_files/', 
                           type='bind'),
                    Mount(source= '/mnt/Transcribation_pipeline/managers/pipeline_logs/',
                          target = '/app/pipeline_logs_managers/',
                          type = 'bind')                
                    ]
                        )
    task
    