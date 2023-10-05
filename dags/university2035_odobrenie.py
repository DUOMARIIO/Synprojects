from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import urllib
import requests
import re


# ------------------------- Определение базовых переменных ------------------------ #
                

# Запрос в LMS на выгрузку данных по студентам
query_to_LMS = """SELECT * FROM mc2_prod.special_university2035
                WHERE 1=1
                AND EGPU_checkbox != 1
                AND untiId !=0"""

# Запрос в ASS_FC_DP_COURSES для выгрузки информации по названиям и ID курсов
query_to_course_ID_ass = """SELECT * FROM [Analytic].[dbo].[ASS_FC_DP_COURSES]"""

# Запрос на выгрузку студентов которые висят в статусе "Одобрено" в LMS и "На рассмотрении" в 2035.university
query_to_get_status_api = """
                            SELECT LMS.[index] ,LMS.[suId] ,LMS.[untiId] ,LMS.[1st_name] ,LMS.[2nd_name] ,LMS.[3rd_name] ,LMS.[email]
                            ,LMS.[phone] ,LMS.[EGPU_checkbox] ,LMS.[Synergy_checkbox] ,LMS.[created] ,LMS.[answers] ,LMS.[status]
                            ,LMS.[result] ,LMS.[status_str] ,EX.[ФИО] ,EX.[Leader ID] ,EX.[Unti ID] ,EX.[Курс] ,EX.[ID курса]
                            ,EX.[Номер потока] ,EX.[Статус], AD.[ID потока]
                        FROM [Analytic].[dbo].[Kras_dig_profession_LMS] LMS
                        JOIN [Analytic].[dbo].[Kras_dig_profession_excel] EX ON LMS.[untiId] = EX.[Unti ID]
                        JOIN [Analytic].[dbo].[ASS_FC_DP_COURSES] AD 
                        ON EX.[Курс] = AD.[Курс]
                        WHERE 1=1
                        AND EX.[Unti ID] IS NOT NULL
                        AND status_str = 'одобрен'
                        AND Статус = 'На рассмотрении'
                        """

# Запрос на выгрузку всех данных по студентам из Kras_dig_prof_status
query_post_api_request_update = 'SELECT * FROM [Analytic].[dbo].[Kras_dig_prof_status]'



# ------------------ Определение функций (тасков) ------------------ #


def sqlserver_connection():

    """ Создание подключения """

    # Драйвер для подклбчения к БД
    driver = 'Driver={ODBC Driver 18 for SQL Server}' + \
                ';TrustServerCertificate=Yes'+ \
                ';Server=' + 'MSK1-ADB01.synergy.local' + \
                ';Database=' + 'Analytic' + \
                f';UID={Variable.get("tuz_login")}' +  \
                f';PWD={Variable.get("tuz_password")}'
    params = urllib.parse.quote_plus(driver)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    return engine


def upload_from_lms_to_analytic(query_to_LMS, upload_table_name):
    
    """ Выгрузка данных по студентам из LMS с дальнейшой загрузкой в БД Аналитик"""
    
    
    # Подключение к БД LMS и SELECT запрос в таблицу special_university2035
    LMS_connection = pymysql.connect(
                    host='msk1-mcdb01',
                    user=Variable.get("lms_login"),
                    password=Variable.get("lms_password"),
                    database='mc2_prod',
                    cursorclass=pymysql.cursors.DictCursor
                    )
    data = pd.read_sql(query_to_LMS, con=LMS_connection)

    # Функция для определения статусов
    def status(x):
        if x == 0:
            return 'новый'
        elif x == 1:
            return 'проверен'
        elif x == 2:
            return 'одобрен'
        elif x == 3:
            return 'отклонен'
    # Определение статуса ученика
    data['status_str'] = data['status'].apply(status)
    
    # Выгрузка в БД
    data['upload_sys_moment'] = datetime.now()
    data.to_sql(upload_table_name, con=sqlserver_connection(), if_exists='replace')


def join_course_id(file_path, query_to_course_ID_ass, upload_table_name):

    """ Добавление к данным из LMS информации о названии курса """
    
    df = pd.read_excel(file_path)
    # course_id = pd.read_sql(query_to_course_ID_ass, con=sqlserver_connection())
    # data = df.merge(course_id, left_on='Курс', right_on='Курс', how='left')
    df.to_sql(upload_table_name, con=sqlserver_connection(), if_exists='replace')


def get_status_api_update(query_to_get_status_api, url, token, logs_pth):

    """ Выгрузка данных по статусам студентов из ресурса 2035.university """

    df = pd.read_sql(query_to_get_status_api,con=sqlserver_connection())

    # Добавлям дату экзамена
    df['date_exam'] = pd.to_datetime(df['created'].dt.date,format='%Y-%m-%d')

    # в хедере передаем токен
    head = {'Authorization': f'Token {token}'}

    # Список для сбора результатов api запросов
    result = []

    # идем циклом по всем спискам и данные из этих списков записывем в params
    # platform_id всегда равен 1358
    for _, row in df.iterrows():
        params = {
                'platform_id':1358,
                'unti_id':row['untiId'],
                'external_course_id':row['ID курса']
                }
        try:
            req = requests.get(url, headers=head, params=params,timeout=80)
            # в будкщую таблицу записываем:
            # - id студента
            # - статус переданный в json (при ошибке возвращает номер respons)
            # номер response
            # время, в которое была произведена запись
            

            # добавляем лист в общий список
            result.append([row['untiId'], req.json().get('application_status',req.status_code), req.status_code, datetime.now()])

        # При ошибке принтим айди студента с ошибкой
        except Exception as e:
            with open(logs_pth, 'a') as f:
                f.writelines(f"\n{datetime.now()} - Error at the step of get_status_api_update - {e} - untiId:{row['untiId']}")
            
    # Добавление результатов в DataFrame и выгрузка в БД
    data = pd.DataFrame(result,columns=['untiId','univ_status','request_status','sysmoment'])

    # джойним нашу первоначальную выгрузку из начала функции и полученные запросы
    all_data = df.merge(data)
    all_data.to_sql('Kras_dig_prof_status', con=sqlserver_connection(), if_exists='replace')



def post_api_request_update(query_post_api_request_update, url, token, upd_table, res_save_path, logs_pth):

    """
    Функция post запросов для обновления информации на ресурсе university.2035
    Ссылка на документацию https://cat.u2035test.ru/api/docs/#api-v5-course-e
    ССылка на конфу https://wiki.2035.university/pages/viewpage.action?pageId=104235121
    
    """

    # Выгружаем данные из общей таблицы со статусами
    df = pd.read_sql(query_post_api_request_update,con=sqlserver_connection())

    # приводим дату экзамена к str
    df['date_exam'] = df['date_exam'].astype('str')

    # в хедере передаем токен
    head = {'Authorization': f'Token {token}'}

    # сюда будем записывать мелкие списки, которые будут строкой в будущем датафрейме
    result = []

    # platform_id всегда равен 1358
    for _, row in df.iterrows():
        params = {
                "platform_id": 1358,
                "unti_id": row['untiId'],
                "course_id": row['ID курса'],
                "status": "approved",
                "flow":row['ID потока'],   # id потока
                "enter_exam_date": row['date_exam']  # дата экзамена
                } 
        try:      
            res = requests.post(url, headers=head, data=params)

            # в будкщую таблицу записываем:
            # - id студента
            # - id курса
            # id потока
            # - статус переданный в json (при ошибке возвращает номер respons)
            # номер response
            # время, в которое была произведена запись
                        
            result.append([row['untiId'], row['ID курса'], row['ID потока'], res.json().get('success', res.status_code), res.status_code, datetime.now()])
        except Exception as e:
            with open(logs_pth, 'a') as f:
                f.writelines(f"\n{datetime.now()} - Error at the step of post_api_request_update - {e} - untiId:{row['untiId']}")

    # Оставляем в исходном df только требуемые колонки
    # df = df[['untiId','ФИО','result']]

    if len(result) > 0:
        result = pd.DataFrame(result,columns=['untiId','course_id','flow','json_value','request_status','sysmoment'])

        # мерджим к нашим данным фио и оценку
        data = result.merge(df[['untiId','ФИО','result']], how='left', left_on='untiId', right_on='untiId')

        data['status_student'] = 'Одобрен'

        # добавляем в базу
        data.to_sql('Kras_dig_prof_post_upd',con=sqlserver_connection(),if_exists='append')
  
        # Сохранение результата
        file_save_name = str(datetime.now())
        file_save_name = file_save_name[:file_save_name.find('.')]
        file_save_name = re.sub('[^0-9a-zA-Z]+', '_', file_save_name)
        result_filename = f'{res_save_path}Загруженные_{file_save_name}.xlsx'
        
        data.to_excel(result_filename, index=False)
    else:
        print('*' * 50)
        print('Нет новых записей для выгрузки')


# ------------ Определение DAG ------------ #


default_args = {'owner': 'Samigullin_Ildus',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="University_2035_odobrenie",
        tags = ['CF/CP'],
        description = "Одобрение студентов (перевод в статус 'approved')" ,
        schedule_interval='0 7,11,14 * * *', 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:

    query_to_db = PythonOperator(
                    task_id='upload_from_lms_to_analytic',
                    python_callable=upload_from_lms_to_analytic,
                    op_kwargs={'query_to_LMS': query_to_LMS,
                              'upload_table_name':'Kras_dig_profession_LMS'})

    upload_LK2025 = PythonOperator(
                    task_id='upload_LK2025',
                    python_callable=join_course_id,
                    op_kwargs={'file_path' : '/var/lib/documents/88.das/03.Общая/ЛК_2035/ЛК2035.xlsx',
                               'query_to_course_ID_ass' : query_to_course_ID_ass,
                               'upload_table_name':'Kras_dig_profession_excel'})

    get_status_api_update = PythonOperator(
                task_id='get_status_api_update',
                python_callable=get_status_api_update,
                op_kwargs={'query_to_get_status_api' : query_to_get_status_api,
                            'token' : '20ab09550cb3dabd4cd8a9cbb85ec6513e23e573',
                            'url' : 'https://cat.2035.university/api/v5/course/enroll/',
                            'logs_pth' : '/opt/airflow/dag_logs/un2035/un2035.txt'})

    post_api_request_update = PythonOperator(
                task_id='post_api_request_update',
                python_callable=post_api_request_update,
                op_kwargs={'query_post_api_request_update' : query_post_api_request_update,
                            'token' : '20ab09550cb3dabd4cd8a9cbb85ec6513e23e573',
                            'url' : 'https://cat.2035.university/api/v6/course/enroll/update/',
                            'upd_table' : 'Kras_dig_prof_post_upd',
                            'res_save_path' : '/var/lib/documents/88.das/03.Общая/ЛК_2035/Result/',
                            'logs_pth' : '/opt/airflow/dag_logs/un2035/un2035.txt'})

    query_to_db >> upload_LK2025 >> get_status_api_update >> post_api_request_update