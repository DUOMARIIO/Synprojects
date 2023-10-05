from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from docker.types import Mount


default_args = {'owner': 'Samigullin_Ildus',
                'depends_on_past': False}      # Запуск вне зависимости от статуса прошедших запусков

with DAG(dag_id='STT_kd_managers_tr_check',
        tags = ['STT', 'STT_managers', 'docker'],
        description = 'Проверка транскрибаций менеджеров КД1' ,
        schedule_interval='0 22 * * *',     
        start_date=days_ago(1),            
        catchup=False,
        default_args=default_args,
        max_active_runs= 1) as dag:

    task = DockerOperator(task_id='run_app_in_container',
                          image='kd_managers:13.06',
                          command='python /app/main.py',
                          docker_url='unix://var/run/docker.sock',    
                          network_mode='bridge',
                          auto_remove=True,
                          mounts=[Mount(source='/mnt/samigullin/evaluation/logs', target='/app/logs', type='bind')]
                          )
    task
    