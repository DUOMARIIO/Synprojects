from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import urllib




# ------------------ Определение функций (тасков) ------------------ #

def sqlserver_connection():

    """ Создание подключения """

    # Драйвер для подклбчения к БД
    driver = 'Driver={ODBC Driver 18 for SQL Server}' + \
                ';TrustServerCertificate=Yes'+ \
                ';Server=' + 'MSK1-BIDB01.synergy.local' + \
                ';Database=' + 'Analytic' + \
                f';UID={Variable.get("tuz_login")}' +  \
                f';PWD={Variable.get("tuz_password")}'
    params = urllib.parse.quote_plus(driver)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    return engine


def decline_api(decline_file_pth, url, token, logs_pth):

    """ Отклонение заявок. Результат пишется в базу. """
    

    df = pd.read_excel(decline_file_pth)

    reques = df[['Unti ID','ID курса','ID потока']].drop_duplicates().values.tolist()

    # в хедере передаем токен
    head = {'Authorization': f'Token {token}'}

    # сюда будем записывать мелкие списки, которые будут строкой в будущем датафрейме
    result = []
    # идем циклом по всем спискам и данные из этих списков записывем в params
    # platform_id всегда равен 1358

    for i in reques:
        params = {
                "platform_id": 1358,
                "unti_id": i[0],
                "course_id": i[1],
                "status": "declined",
                "reason_choice" : "application_deadline"
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
            
            result.append([i[0], i[1], i[2], req.json().get('success', req.status_code), req.status_code, datetime.now()])
        except Exception as e:
            with open(logs_pth, 'a') as f:
                f.writelines(f"\n{datetime.now()} - Error at the step of post_api_request_update - {e} - untiId:{i[0]}")
        
    # из получившегося списка списков получаем датафрейм
    result = pd.DataFrame(result,columns=['untiId','course_id','flow','json_value','request_status','sysmoment'])

    # чтобы таблица запендилась в SQL добавим поля с пустыми значениями
    data = result

    data['ФИО'] = ''

    data['result'] = ''

    data['status_student'] = 'Отклонена'

    # грузим к нам  в базу
    data.to_sql('Kras_dig_prof_post_upd',con=sqlserver_connection(),if_exists='append')



# ------------ Определение DAG ------------ #


default_args = {'owner': 'Samigullin_Ildus',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="University_2035_decline",
        tags = ['CF/CP'],
        description = "Завершение модуля студентов University_2035" ,
        schedule_interval=None, 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:

    decline = PythonOperator(
                    task_id='decline_api',
                    python_callable=decline_api,
                    op_kwargs={'decline_file_pth' : '/var/lib/documents/88.das/03.Общая/ЛК_2035/Отклонить.xlsx',                                
                                'url' : 'https://cat.2035.university/api/v6/course/enroll/update/',
                                'token' : '20ab09550cb3dabd4cd8a9cbb85ec6513e23e573',
                                'logs_pth' : '/opt/airflow/dag_logs/un2035/university2035_decline.txt'})

    decline
