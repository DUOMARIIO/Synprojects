from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from docker.types import Mount

'''
STT пайплайн для транскрибации разговоров менеджеров
'''

# ------------------ Определение DAG ------------------ #

default_args = {'owner': 'Dubai Omar',
                'depends_on_past': True}      # Запуск в/вне зависимости от статуса прошедших запусков

with DAG(dag_id='STT_disp',
        tags = ['STT', 'docker'],
        description = 'Транскрибация разговоров диспетчеров' ,
        schedule_interval='0 */2 * * *',
        max_active_runs = 1,     
        start_date=days_ago(1),            
        catchup=False,
        default_args=default_args) as dag:

    task = DockerOperator(
            task_id='stt_disp_transcribation',             # Название task
            image='stt_img:2',            # Название и тег запускаемого образа
            command='python /stt/app/main_disp.py', # Запускаемая команда внутри контейнера
            docker_url='unix://var/run/docker.sock',    
            network_mode='bridge',                      # Тип docker-network с каким будет подниматься контейнер
            auto_remove=True,                           # Удаление поднятого контейнера после исполнения кода
                                                        # Прокидывание volume в контейнер
            mounts=[Mount(source='/mnt/stt_prod/stt_managers/test_queries/', target='/stt/test_queries/', type='bind'),
                    Mount(source='/mnt/Transcribation_pipeline/pipeline_logs/', target='/stt/pipeline_logs/', type = 'bind'),
                    Mount(source='/mnt/Transcribation_pipeline/Vad/', target='/stt/Vad/', type = 'bind'),
                    Mount(source='/mnt/Transcribation_pipeline/Transcribs/', target='/stt/Transcribs/', type = 'bind'),
                    Mount(source='/mnt/Transcribation_pipeline/transcribs_df/', target='/stt/transcribs_df/', type = 'bind'),
                    Mount(source='/var/lib/oktell/', target='/var/lib/oktell/', type = 'bind')
                    ]
                        )
    task