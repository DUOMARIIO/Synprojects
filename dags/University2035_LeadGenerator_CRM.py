from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import urllib
import requests
import time




        ##########################################################################
        # ------------------------- ДОП. ПАРАМЕТРЫ  ---------------------------- #
        ##########################################################################

## Лимит создания лидов в одном цикле
leadlimit = 9999 ##Пока без ограничений

## URL для Post запроса генерации лида в CRM 
url_post = 'https://corp.synergy.ru/api/api_d_crm/crm_create_lead'

## Токен авторизации для CRM
token = Variable.get("CRM_API_Token")

## Формируем header для post запроса
header = {'App-Token': f'{token}'}

## Запрос для отбора локальной очереди на генерацию лидов в рамках одного цикла.
## Далее отработанные данные отметим в NM_api_minion с меткой IS_SENT = 1
query_lead_list = f'''
select top({leadlimit})
dd.*
from dbo.[NM_api_minion] dd
left join dbo.NM_api_minion_hst hst with(nolock) on hst.UNTI_ID = dd.UNTI_ID
where 1=1
and dd.IS_SENT = 0    --Статус "Не отправлен"
and hst.UNTI_ID is null --Проверяем что лид с таким UNTI не отправляли
'''




        ##########################################################################
        # --------------------------- Подключения ------------------------------ #
        ##########################################################################


## Подключение к BIDB01
def db_conn():
    print('db_conn starting')
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
 

## Получить текущую очередь для генерации
def get_query():
    print('get_query starting')
    ## Выгружаем локальную очередь
    df_lead_list = pd.read_sql(query_lead_list, con=db_conn())
    print('get_query df_lead_list success')
    return df_lead_list
    
## Получить количество строк локальной очереди
def count_query():
    ## Считаем локальную очередь
    print('count_query start')
    count_lead_list = get_query().loc[get_query()['IS_SENT'] == 0].shape[0]
    print('count_query count_lead_list success')

    return count_lead_list


## Функция для обновления поля IS_SENT в очереди
def update_results(result, unti, lead, contact):
    print(f'update_results start -- result:{result} unti:{unti} lead:{lead} contact:{contact}')
    ## Если получили ответ "Успешно" записываем 1 в таблицу очереди
    if result == 1:
        print('result = 1 start update')
        update_query = f"""
        update	dbo.[NM_api_minion]
        set		IS_SENT = 1,
                CONTACT_ID = {contact},
                LEAD_ID = {lead}
        from	dbo.[NM_api_minion]
        where	UNTI_ID = {unti};
        """
    
    ## Если получили ответ "Ошибки" записываем 2 в таблицу очереди
    else:
        print('result = else start update')
        update_query = f"""
        update	dbo.[NM_api_minion]
        set		IS_SENT = 2
        from	dbo.[NM_api_minion]
        where	UNTI_ID = {unti};
        """
    ## Создаем подключение для обновления
    print('Connecting to BIDB01')
    conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server}'+ \
            ';TrustServerCertificate=Yes'+ \
            ';Server=' + 'MSK1-BIDB01.synergy.local' + \
            ';Database=' + 'Analytic' + \
            f';UID={Variable.get("tuz_login")}' +  \
            f';PWD={Variable.get("tuz_password")}')
    
    # Открываем подключение
    cursor = conn.cursor()
    print('Connection success')

    # Выполняем обновление
    print('start cursor execute update_query')
    cursor.execute(update_query)

    # Коммитим транзакцию
    conn.commit()

    # Закрываем курсор и подключение
    cursor.close()
    conn.close()


def insert_results(unti, json_value):
    ## Запрос для инсерта в таблицу истории обновления
    print('start insert_query')
    insert_query = f'''
    select
    d.UNTI_ID
    ,'{json_value}' api
    ,d.IS_SENT
    ,d.[VERSION] [CP_TYPE]
    ,d.CONTACT_ID
    ,d.LEAD_ID
    ,GETDATE() SYSMOMENT
    from dbo.NM_api_minion d with(nolock)
    left join NM_api_minion_hst dh with(nolock) on d.UNTI_ID = dh.UNTI_ID
    where
    d.IS_SENT <> 0
    and dh.UNTI_ID is null
    and d.UNTI_ID = {unti}
    '''
    print('try inserting')
    try:
        print('get insert df')
        df_insert = pd.read_sql(insert_query,con=db_conn())
        print('start inserting')
        df_insert.to_sql('NM_api_minion_hst',con=db_conn(),if_exists='append',index=False)
        print('inserting success')
    except:
        print('value already exists in hst table')










                                ###############################################################
                                ###################     ГЕНЕРАТОР ЛИДОВ     ###################
                                ###############################################################

def leadgenerator(leadlimit, url_post, header, query_lead_list):
    
    print('формируем json df_json_fields')
    ## Оставляем только нужные поля для отправки d JSON
    df_json_fields = get_query()[['LAND', 'VERSION', 'CAMPAIGN' ,'QUOTE_ID' 
                ,'UNTI_ID' ,'LEAD_NAME' ,'PHONE' ,'EMAIL' ,'CITY' ,'REGION' ,'COMMENT','GENDER_STUD' ,'BIRTHDATE_STUD' 
                ,'DOC_TYPE_STUD' ,'CITIZENSHIP_STUD','SNILS_STUD' ,'PASS_SERIES_STUD','PASS_NUMBER_STUD','PASS_AUTHORITY_STUD','PASS_DATE_STUD','PASS_CODE_STUD'
                ,'LAST_NAME_PARENT','NAME_PARENT','SECOND_NAME_PARENT','BIRTHDATE_PARENT','PHONE_PARENT','EMAIL_PARENT'
                ,'PASS_SERIES_PARENT','PASS_NUMBER_PARENT','PASS_AUTHORITY_PARENT','PASS_DATE_PARENT' ,'PASS_CODE_PARENT' ,'GENDER_PARENT', 'UF_CRM_1456812806']]

    count = count_query()

    print(f'''
        *********************
            count query: {count}
        *********************
        ''')

    while count > 0:
        try:
            for index, row in df_json_fields.iterrows():
                print('*' * 50)
                ## Формируем json файл для каждой строки
                json_value = row.to_json()

                ## Записываем unti_id в качестве переменной 
                unti_id = row['UNTI_ID']

                ## Отправляем post запрос на генерацию лидов
                print('try Post request')
                try:
                    res = requests.post(url=url_post, headers=header, data=json_value)
                    print('Post request success')
                except:
                    print('Post request failed')

                ## Записываем результаты в переменные для дальнейшего обновления
                json_result = res.json().get('result')
                json_id_contact = res.json().get('id_contact')
                json_id_lead = res.json().get('id_lead')

                ## Делаем обновления боевой таблицы очереди
                print('start update_results')
                update_results(json_result, unti_id, json_id_lead, json_id_contact)
                
                ## Записываем данные в таблицу истории
                print('start insert_results')
                insert_results(unti_id, json_value)

                # Таймаут между циклами 1 сек 
                print('start timeout')
                time.sleep(5)

                
                count = count_query()
                print(f'''
                *********************
                    count query: {count}
                *********************
                ''')

        except:
            print('FAIL LOOP')
            break
            

    print('finish')        

   

##########################################################################
# --------------------------- Настройка DAG ------------------------------ #
##########################################################################


default_args = {'owner': 'Nesh_Markoski',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="University_2035_LeadGenerator",
        tags = ['CF/CP'],
        description = "Генерация лидов в CRM системе" ,
        schedule_interval='45 6 * * *', 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:


    leadgenerator = PythonOperator(
                    task_id='leadgenerator',
                    python_callable=leadgenerator,
                    op_kwargs={'leadlimit': leadlimit,
                              'url_post':url_post,
                              'header':header,
                              'query_lead_list':query_lead_list})

 
    leadgenerator