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


"""
Функция post запросов для обновления информации на ресурсе university.2035
Ссылка на документацию https://cat.u2035test.ru/api/docs/#api-v5-course-e
ССылка на конфу https://wiki.2035.university/pages/viewpage.action?pageId=104235121

"""



##########################################################################
# --------------------------- Подключения ------------------------------ #
##########################################################################


## Подключение к BIDB01

def db_conn():

    # Драйвер для подключения к БД 
    driver = 'Driver={ODBC Driver 18 for SQL Server}'+ \
            ';TrustServerCertificate=Yes'+ \
            ';Server=' + 'MSK1-BIDB01.synergy.local' + \
            ';Database=' + 'Analytic' + \
            f';UID={Variable.get("tuz_login")}' +  \
            f';PWD={Variable.get("tuz_password")}'

    params = urllib.parse.quote_plus(driver)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    return engine
 



## Подключение к БД LMS

def lms_conn():
    engine = create_engine("mysql://{user}:{password}@{server}:3306/{db}"
                       .format(user=Variable.get("lms_login"),
                               password=Variable.get("lms_password"),                                               
                               server="msk1-mcdb01",
                               db="mc2_prod"))
    return engine



        ##########################################################################
        # ------------------------- ДОП. ПАРАМЕТРЫ  ---------------------------- #
        ##########################################################################

# Таблица для загрузки данных LMS в BIDB01
upload_table_name_LMS ='NM_CP_LMS'

## Таблица для загрузки данных LMS в BIDB01
upload_table_name_LK2035 = 'NM_CP_LK2035'

## Таблица для загрузки результата get запроса в BIDB01
get_request_table = 'NM_CP_GetRequest_Status'

## Таблица для загрузки результата post запроса в BIDB01
post_request_table = 'NM_CP_PostRequest_Status'

## Токен Авторизации к API ЛК2035
token = Variable.get("University2035_API_Token")

## Header Авторизации к API ЛК2035
head = {'Authorization': f'Token {token}'}

## URL для GET запроса текущего статуса клиента
url_get ='https://cat.2035.university/api/v5/course/enroll/'

## URL для POST запроса текущего статуса клиента
url_post = 'https://cat.2035.university/api/v6/course/enroll/update/'

## Путь логов
logs_pth = '/opt/airflow/dag_logs/un2035/un2035.txt'

## Путь к Excel файлу выгрузки ЛК2035
file_path_excel = '/var/lib/documents/88.das/03.Общая/ЛК_2035/ЛК2035.xlsx'

## Путь для файла сохрания на общий диск
res_save_path = '/var/lib/documents/88.das/03.Общая/ЛК_2035/Result/'




        ##########################################################################
        # --------------------------- SQL ЗАПРОСЫ ------------------------------ #
        ##########################################################################

## Запрос в LMS для выгрузки списка клиентов ЛК2035
query_to_LMS = """
SELECT 
* 
FROM mc2_prod.special_university2035
WHERE 1=1
AND EGPU_checkbox != 1
AND untiId !=0
"""



## Запрос для получения списка клиентов для одобрения 
# Доп. логика поля [id_flow]: 
# Если клиент по новому курсу то смотрится новая # таблица NM_CP_COURSE_CALENDAR, 
# если же course_id старый то смотрится старая таблица ASS_FC_DP_COURSES
query_to_get_status_api = """
SELECT 
LMS.[untiId] [unti_id]
,cast(LMS.[created] as date) [date_exam]
,LMS.[status_str] [status_lms] 
,EX.[Статус] [status_lk2035]
,EX.[ФИО] [FIO]
,LMS.[result]
,EX.[Курс]  [course_name]
,EX.[ID курса] [course_id]
,COALESCE(FLOW.[id_flow],AD.[ID потока]) [id_flow]

FROM [dbo].[NM_CP_LMS] LMS WITH(NOLOCK)
JOIN [dbo].[NM_CP_LK2035] EX WITH(NOLOCK)
	ON LMS.[untiId] = EX.[Unti ID]
	AND EX.[Unti ID] IS NOT NULL

--Новая логика
LEFT JOIN [dbo].[NM_CP_COURSE_CALENDAR] FLOW WITH(NOLOCK)
	on FLOW.[ID_COURSE] = EX.[ID курса]
	and FLOW.[IS_CURRENT] = 1

--Старая логика
LEFT JOIN  [Analytic].[dbo].[ASS_FC_DP_COURSES] AD WITH(NOLOCK)
	ON EX.[Курс] = AD.[Курс]

WHERE 1=1
AND LMS.[status_str]= 'одобрен'
AND EX.[Статус] = 'На рассмотрении'
and COALESCE(FLOW.[id_flow],AD.[ID потока]) is not null

"""

## Выборка клинтов по кому будет отправлен POST запрос на Одобрение ("Approved")
query_post_api_request_update = '''
SELECT * 
FROM [Analytic].[dbo].[NM_CP_GetRequest_Status] WITH(NOLOCK)
where univ_status = 'review'
'''



        ####################################################################
        # -------------------------- ФУНКЦИИ ----------------------------- #
        ####################################################################

##################################
## Загрузка таблицы ЛК2035 в BIDB01
##################################
def upload_LMS_to_BIDB01(query_to_LMS, upload_table_name_LMS):
    print('read_sql LMS start')
    data = pd.read_sql(query_to_LMS, con=lms_conn())  
    print('read_sql LMS finish')
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
    print('insert to ADB start')
    data.to_sql(upload_table_name_LMS, con=db_conn(), if_exists='replace', index=False)
    print('insert to ADB start')





####################################
## Загружаем выгрузку ЛК2035 в BIDB01
####################################

def upload_LK2035_to_BIDB01(file_path_excel, upload_table_name_LK2035):
    print('exec excel')
    df = pd.read_excel(file_path_excel)
    print('insert excel to db')
    df.to_sql(upload_table_name_LK2035, con=db_conn(), if_exists='replace', index=False)
    print('finish')



##############################
## GET запрос текущего статуса
##############################


## Отправляем get запрос по списку из Excel файла
def get_status2035(query_to_get_status_api, url_get, head, logs_pth, get_request_table):
      
    ## Выгружаем список клинтов для GET запроса
    print('starting exec df_clients')
    df_clients = pd.read_sql(query_to_get_status_api,con=db_conn())
    
    ## Список для сбора результатов api запросов
    result = []

    for _, row in df_clients.iterrows():
        params = {
                'platform_id':1358, ## ID нашего аккаунта (не меняется)
                'unti_id':row['unti_id'], ## Определяется из SQL запроса
                'external_course_id':row['course_id'] ## Определяется из SQL запроса
                }
        try:
            print(f'try to get {params}')
            req = requests.get(url_get, headers=head, params=params,timeout=80)
            # в будкщую таблицу записываем:
            # --id студента
            # --статус переданный в json (при ошибке возвращает номер respons)
            # номер response
            # время, в которое была произведена запись
            print(f'get for {params}: success')    

            # добавляем лист в общий список
            result.append([row['unti_id'], req.json().get('application_status',req.status_code), req.status_code, datetime.now()])
            print(f'result appended') 


        # При ошибке принтим айди студента с ошибкой
        except Exception as e:
            print(f'get for {params}: fail') 
            with open(logs_pth, 'a') as f:
                f.writelines(f"\n{datetime.now()} - Error at the step of get_status_api_update - {e} - unti_id:{row['unti_id']}")
                
    # Добавление результатов в DataFrame и выгрузка в БД
    data = pd.DataFrame(result,columns=['unti_id','univ_status','request_status','sysmoment'])

    # джойним нашу первоначальную выгрузку из начала функции и полученные запросы
    all_data = df_clients.merge(data)
    print(f'inserting to {get_request_table}: start') 
    all_data.to_sql(get_request_table, con=db_conn(), if_exists='replace', index=False)
    print(f'inserting to {get_request_table}: finish') 



#####################################
## POST запрос для перевода в ОДОБРЕН
#####################################

def post_status2035(query_post_api_request_update, url_post, head, logs_pth, post_request_table,res_save_path):
    
    # Выгружаем данные из общей таблицы со статусами
    print('Read_sql query_post_api_request_update')
    df = pd.read_sql(query_post_api_request_update,con=db_conn())

    # приводим дату экзамена к str
    print('date_exam to str')
    df['date_exam'] = df['date_exam'].astype('str')

    result = []

    for _, row in df.iterrows():
        params = {
                "platform_id": 1358,
                "unti_id": row['unti_id'],
                "course_id": row['course_id'],
                "status": "approved",
                "flow":row['id_flow'],   # id потока
                "enter_exam_date": row['date_exam']  # дата экзамена
                } 
        try:      
            print(f'try to post: {params}')
            res = requests.post(url_post, headers=head, data=params)
            
            # в будкщую таблицу записываем:
            # - id студента
            # - id курса
            # - id потока
            # - статус переданный в json (при ошибке возвращает номер respons)
            # номер response
            # время, в которое была произведена запись
            print(f'post success: {params}')               
            result.append([row['unti_id'], row['course_id'], row['id_flow'], res.json().get('success', res.status_code), res.status_code, datetime.now()])


        except Exception as e:
            print(f'post failed: {params}') 
            with open(logs_pth, 'a') as f:
                f.writelines(f"\n{datetime.now()} - Error at the step of post_api_request_update - {e} - untiId:{row['unti_id']}")



    print('start') 
    if len(result) > 0:

        print('result to df') 
        result = pd.DataFrame(result,columns=['unti_id','course_id','id_flow','json_value','request_status','sysmoment'])

        # мерджим к нашим данным фио и оценку
        print('merge result') 
        data = result.merge(df[['unti_id','FIO','result']], how='left', left_on='unti_id', right_on='unti_id')

        print('inserting status: "Одобрен"') 
        data['status_student'] = 'Одобрен'

        # добавляем в базу
        print(f'to_sql into  {post_request_table} -- start') 
        data.to_sql(post_request_table,con=db_conn(),if_exists='append', index=False)
        print(f'to_sql into  {post_request_table} -- success') 

        # Сохранение результата
        file_save_name = str(datetime.now())
        file_save_name = file_save_name[:file_save_name.find('.')]
        file_save_name = re.sub('[^0-9a-zA-Z]+', '_', file_save_name)
        result_filename = f'{res_save_path}Загруженные_{file_save_name}.xlsx'
        
        print(f'to_excel into {result_filename} -- start') 
        data.to_excel(result_filename, index=False)
        print(f'to_excel into {result_filename} -- success') 

    else:
        print('*' * 50)
        print('Нет новых записей для выгрузки') 



##########################################################################
# --------------------------- Настройка DAG ------------------------------ #
##########################################################################


default_args = {'owner': 'Nesh_Markoski',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="University_2035_odobrenie_v2",
        tags = ['CF/CP'],
        description = "Одобрение студентов (перевод в статус 'approved'). Обновленная версия" ,
        schedule_interval='0 7,11,14 * * *', 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:


    upload_LMS_to_BIDB01 = PythonOperator(
                    task_id='upload_LMS_to_BIDB01',
                    python_callable=upload_LMS_to_BIDB01,
                    op_kwargs={'query_to_LMS': query_to_LMS,
                              'upload_table_name_LMS':upload_table_name_LMS})

    upload_LK2035_to_BIDB01 = PythonOperator(
                    task_id='upload_LK2035_to_BIDB01',
                    python_callable=upload_LK2035_to_BIDB01,
                    op_kwargs={'file_path_excel' : file_path_excel,
                               'upload_table_name_LK2035':upload_table_name_LK2035})

    get_status2035 = PythonOperator(
                    task_id='get_status2035',
                    python_callable=get_status2035,
                    op_kwargs={'query_to_get_status_api' : query_to_get_status_api,
                                'url_get' : url_get,
                                'head': head,
                                'logs_pth' : logs_pth,
                                'get_request_table': get_request_table})

    post_status2035 = PythonOperator(
                task_id='post_status2035',
                python_callable=post_status2035,
                op_kwargs={'query_post_api_request_update' : query_post_api_request_update,
                            'url_post' : url_post,
                            'head' : head,
                            'logs_pth' : logs_pth,
                            'post_request_table' : post_request_table,
                            'res_save_path' : res_save_path})

    upload_LMS_to_BIDB01 >> upload_LK2035_to_BIDB01 >> get_status2035 >> post_status2035