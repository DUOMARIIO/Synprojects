from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import requests
from datetime import datetime, timedelta
import re





# ------------------ Определение функций (тасков) ------------------ #


def module_update(file_pth, excel_save_pth, url, token, logs_pth):
    '''
    Функция для завершения модуля. Используется аналогичный метод. В параметре запроса пишем "finished"
    '''
        
    # достаем эксель файлы
    df = pd.read_excel(file_pth)
    
    # в хедере передаем токен
    head = {'Authorization': f'Token {token}'}
 
    # Запись результатов запросов
    result = []
    # идем циклом по всем спискам и данные из этих списков записывем в params
    # platform_id всегда равен 1358

    for _, row in df.iterrows():
        params = {
                "platform_id": 1358,
                "unti_id": row['Unti ID'],
                "course_id": row['ID курса'],
                "status": "finished",
                } 
        try:
            req = requests.post(url, headers=head, data=params)

            # в будкщую таблицу записываем:
            # - id студента
            # - id курса
            # id потока
            # - статус переданный в json (при ошибке возвращает номер respons)
            # номер response
            # время, в которое была произведена запись
            
            result.append([row['Unti ID'], row['ID курса'], req.json().get('success', req.status_code), req.status_code])
            
        except Exception as e:
            with open(logs_pth, 'a') as f:
                f.writelines(f"\n{datetime.now()} - Error at the step of post_api_request_update - {e} - untiId:{row['Unti ID']}")
        
    result = pd.DataFrame(result,columns=['untiId','course_id','json_value','request_status'])

    data = df.merge(result, how = 'left', left_on='Unti ID', right_on='untiId')
    data['sysmoment'] = datetime.now()

    file_save_name = str(datetime.now())
    file_save_name = file_save_name[:file_save_name.find('.')]
    file_save_name = re.sub('[^0-9a-zA-Z]+', '_', file_save_name)
    result_filename = f'{excel_save_pth}Загрузка завершивших модуль_{file_save_name}.xlsx'
    data.to_excel(result_filename)





# ------------ Определение DAG ------------ #


default_args = {'owner': 'Samigullin_Ildus',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="University_2035_zavershenie_modulya",
        tags = ['CF/CP'],
        description = "Завершение модуля студентов" ,
        schedule_interval=None, 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:

    module_update = PythonOperator(
                    task_id='module_update',
                    python_callable=module_update,
                    op_kwargs={'file_pth' : '/var/lib/documents/88.das/03.Общая/ЛК_2035/Завершил модуль.xlsx',
                                'excel_save_pth' : '/opt/airflow/lk_2035/',
                                'url' : 'https://cat.2035.university/api/v6/course/enroll/update/',
                                'token' : '20ab09550cb3dabd4cd8a9cbb85ec6513e23e573',
                                'logs_pth' : '/opt/airflow/dag_logs/un2035/university2035_module_update.txt'})

    module_update