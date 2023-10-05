from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


from datetime import datetime, timedelta
import pyodbc
import json
import pandas as pd
from sqlalchemy import create_engine
import urllib
import requests as req
import time


##########################################################################
# ------------------------- ДОП. ПАРАМЕТРЫ  ---------------------------- #
##########################################################################

## URL для обновления контакта
url_update = 'https://corp.synergy.ru/api/api_d_crm/crm_create_lead'

#№ URL для обновления сделки
url_deals   = 'https://corp.synergy.ru/api/api_d_crm/crm_update_deal'

## Токен авторизации
token = Variable.get("CRM_API_Token")

header = {'App-Token': f'{token}'}

## Лимит сделок в очереди на отправку
deal_limit = 15

## Таблицы для обновления контатов 
update_table = 'NM_api_minion_UPDATE' ## Main таблица
update_table_hst = 'NM_api_minion_UPDATE_HST' ## Hst таблица

## Таблицы для обновления сделок
deal_table = 'NM_cp_deal_update'  ## Main таблица
deal_table_hst = 'NM_cp_deal_update_HST' ## Hst таблица

##########################################################################
# --------------------------- Подключения ------------------------------ #
##########################################################################
## Подключение к MSSQL
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

##########################################################################################################################

## Очередь сделок для обновления
query_deals = '''
select 
dd.*
from [NM_cp_deal_update] dd
left join [NM_cp_deal_update_HST] hst with(nolock) on hst.DEAL_ID = dd.DEAL_ID
where 1=1
and dd.IS_SENT = 0    --Статус "Не отправлен"
and hst.DEAL_ID is null --Проверяем что лид с таким UNTI не отправляли
and dd.IS_UPDATED <> 'SpecProj'
'''

##########################################################################################################################
### ОБНОВЛЕНИЕ СДЕЛОК
##########################################################################################################################

## Получение df со сделками для обновления 
def get_deal_df(query_deals):
    df_deals = pd.read_sql(query_deals,con=db_conn())
    ##  Получаем JSON для обновления сделок 
    df_deals = df_deals[['DEAL_ID','PRODUCT_ID','PRICE','UF_CRM_STATUS_DP_LMS'
                        ,'UF_OPERATOR_STATUS','UF_CRM_CONTROL','STAGE_ID'
                        ,'UF_CRM_1498200987', 'UF_CRM_1492016348','UF_CRM_1495622099','UNTI_ID','IS_SENT'
                        ]]
    return df_deals

## Считаем кол-во строк в df для обновления сделок, которые мы ещё не обновляли
def get_deal_count(df_deals):
    count_deal_query = df_deals.loc[df_deals["IS_SENT"] != 1].shape[0]
    return count_deal_query


##########################################################################################################################
### ОБНОВЛЕНИЕ КОНТАКТОВ
##########################################################################################################################

## Получаем df для обновления контактов с поиском по unti_id
def get_update_df(update_table,update_table_hst):
    query_updater = f'''
    select 
    dd.*
    from {update_table} dd
    left join {update_table_hst} hst with(nolock) on hst.UNTI_ID = dd.UNTI_ID
    where 1=1
    and dd.IS_SENT = 0    --Статус "Не отправлен"
    and hst.UNTI_ID is null --Проверяем что лид с таким UNTI не отправляли
    '''
    df_update = pd.read_sql(query_updater,con=db_conn())
    df_update = df_update[['LAND', 'VERSION', 'CAMPAIGN' ,'QUOTE_ID' 
					,'UNTI_ID' ,'LEAD_NAME' ,'PHONE' ,'EMAIL' ,'CITY' ,'REGION' ,'COMMENT','GENDER_STUD' ,'BIRTHDATE_STUD' 
					,'DOC_TYPE_STUD' ,'CITIZENSHIP_STUD','SNILS_STUD' ,'PASS_SERIES_STUD','PASS_NUMBER_STUD','PASS_AUTHORITY_STUD','PASS_DATE_STUD','PASS_CODE_STUD'
					,'LAST_NAME_PARENT','NAME_PARENT','SECOND_NAME_PARENT','BIRTHDATE_PARENT','PHONE_PARENT','EMAIL_PARENT','UF_CRM_1456812806'
					,'PASS_SERIES_PARENT','PASS_NUMBER_PARENT','PASS_AUTHORITY_PARENT','PASS_DATE_PARENT' ,'PASS_CODE_PARENT' ,'GENDER_PARENT', 'CONTACT_ID']]
    return df_update    


##########################################################################################################################

## Получение боевой очереди Битрикс для отправки в АКАДА
def get_bitrix_queue():
    bitrix_queue = req.get('https://corp.synergy.ru/test/monitoring/ajax/get-crm-stat-data.php')
    bitrix_queue = int(bitrix_queue.json().get('queue_akada_cnt'))
    return bitrix_queue


##########################################################################################################################
### ОБНОВЛЕНИЯ ИСТОРИИ И БОЕВЫХ ОЧЕРЕДЕЙ
##########################################################################################################################

## Функция для обновления поля IS_SENT в очередях
def modify_results(result, unti, table_name):
    print(f'update_results start -- result:{result} unti:{unti}')
    ## Если получили ответ "Успешно" записываем 1 в таблицу очереди
    if result == 1:
        print('result = 1 start update')
        update_query = f"""
        update	{table_name}
        set		IS_SENT = 1
        from	{table_name}
        where	[UNTI_ID] = {unti};
        """
    
    ## Если получили ответ "Ошибки" записываем 2 в таблицу очереди
    else:
        print('result = else start update')
        update_query = f"""
        update	{table_name}
        set		IS_SENT = 2
        from	{table_name}
        where	UNTI_ID = {unti};
        """
    ## Создаем подключение для обновления
    conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server}'+ \
            ';TrustServerCertificate=Yes'+ \
            ';Server=' + 'MSK1-BIDB01.synergy.local' + \
            ';Database=' + 'Analytic' + \
            f';UID={Variable.get("tuz_login")}' +  \
            f';PWD={Variable.get("tuz_password")}')
    # Открываем подключение
    cursor = conn.cursor() 
    # Выполняем обновление
    print('start cursor execute update_query')
    cursor.execute(update_query)
    # Коммитим транзакцию
    conn.commit()
    # Закрываем курсор и подключение
    cursor.close()
    conn.close()


## Инсерт в таблицу истории обновления контактов
def insert_results_UPDATE(unti_id,update_table,update_table_hst):
    ## Запрос для инсерта в таблицу истории обновления
    print('start insert_query')
    insert_query = f'''
    select
    d.UNTI_ID
    ,'json passed' api
    ,d.IS_SENT
    ,'InfoUpdate' [CP_TYPE]
    ,d.CONTACT_ID
    ,d.LEAD_ID
    ,GETDATE() [SYSMOMENT]
    from {update_table} d with(nolock)
    left join {update_table_hst} dh with(nolock) on d.UNTI_ID = dh.UNTI_ID
    where 1=1
    and d.IS_SENT <> 0
    and dh.UNTI_ID is null
    and d.UNTI_ID = {unti_id}
    '''
    print('try inserting')
    try:
        print('get insert df')
        df_insert = pd.read_sql(insert_query,con=db_conn())
        print('start inserting')
        df_insert.to_sql('NM_api_minion_UPDATE_hst',con=db_conn(),if_exists='append',index=False)
        print('inserting success')
    except:
        print('value already exists in hst table')

## Инсерт в таблицу истории обновления сделок
def insert_results_DEAL(deal_id,deal_table,deal_table_hst):
    ## Запрос для инсерта в таблицу истории обновления
    print('start insert_query')
    insert_query = f'''
    select
    d.DEAL_ID
    ,d.PRODUCT_ID
    ,d.PRICE
    ,d.UF_CRM_STATUS_DP_LMS
    ,d.UF_OPERATOR_STATUS
    ,d.UF_CRM_CONTROL
    ,d.STAGE_ID
    ,d.UF_CRM_1498200987
    ,d.UF_CRM_1492016348
    ,d.UF_CRM_1495622099
    ,d.IS_SENT
    ,GETDATE() [SYSMOMENT]
    from {deal_table} d with(nolock)
    left join {deal_table_hst} dh with(nolock) on d.DEAL_ID = dh.DEAL_ID
    where 1=1
    and d.IS_SENT <> 0
    and dh.DEAL_ID is null
    and d.DEAL_ID = {deal_id}
    '''
    print('try inserting')
    try:
        print('get insert df')
        df_insert = pd.read_sql(insert_query,con=db_conn())
        print('start inserting')
        df_insert.to_sql('NM_cp_deal_update_hst',con=db_conn(),if_exists='append',index=False)
        print('inserting success')
    except:
        print('value already exists in hst table')

###########################################################################################################################






########################################################################
##################### ОБНОВЛЕНИЕ КОНТАКТОВ #############################
########################################################################
def update_contacts(query_deals,url_update,header,update_table,update_table_hst):
    print('*'*10)
    print('Генерим DF обновления')
    df_update = get_update_df(update_table,update_table_hst)
    count_update = df_update.shape[0]
    i = 0
    print('Пробуем обновить контакт')
    for index, row in df_update.iterrows():
        
        i+=1
        print(f'**** {i} / {count_update} ****')

        print('Генерим JSON обновления и задаем переменные')
        df_json_update = row.to_json()
        contact_id = row['CONTACT_ID']
        unti_id = row ['UNTI_ID']
        
        
        try:
            print(f'Пробуем обновить CONTACT_ID: {contact_id} / UNTI_ID: {unti_id}')
            res = req.post(url=url_update, headers=header, data=df_json_update)
            print('Обновление успешно')
            print(res)
            
        except:
            print('Ошибка обновления')

        ## Записываем ответ, чтобы записать его в таблицу результатов
        print('Пробуем распарсить json')
        try:
            json_result = res.json().get('result')
        except:
            json_result = 1    
        print(f'json result {json_result}')
        ## Делаем обновления боевой таблицы очереди
        print('start update_results')
        try:   
            modify_results(json_result, unti_id, update_table)
        except:
            print('fail')
            break

        ## Заливаем обновление в таблицу истории
        print('start update_results')
        try:
            insert_results_UPDATE(unti_id,update_table,update_table_hst)   
        except:
            print('fail')
            break

        # Таймаут между циклами 1 сек 
        print('start timout')
        time.sleep(1)             




########################################################################
######################## ОБНОВЛЕНИЕ СДЕЛОК #############################
########################################################################
def update_deals(query_deals,url_deals,header,deal_table,deal_table_hst,deal_limit):
    
    df_deals = get_deal_df(query_deals)
    count_deal_query = get_deal_count(df_deals)

    print('Пробуем отправить сделку')
    for index, row in df_deals.iterrows():

        ## Запрашиваем у Битрикса текущую очередь
        bitrix_queue = get_bitrix_queue()
        print(f'btrx queue: {bitrix_queue}')
        print(f'deal push queue: {count_deal_query}')
        
        ## Если боевая очередь Битркис больше лимита уходим в таймаут и ждем
        if bitrix_queue >= deal_limit:
            print('Wait for delay, bitrix query is full')
            time.sleep(30)
        
        ## Иначе продолжаем
        else:  
            print('Генерим JSON обновления')
            df_json_deals = row.to_json()
            deal_id = row['DEAL_ID']
            unti_id = row['UNTI_ID']
            print(f'Взяли DEAL_ID: {deal_id}')

            try:
                print('Пробуем обновить')
                res = req.post(url=url_deals, headers=header, data=df_json_deals)
                print('Обновление успешно')
                
            except:
                print('Ошибка обновления')

            ## Записываем результаты в переменные для дальнейшего обновления
            json_result = res.json().get('result')
            print(f'json result {json_result}')
            ## Делаем обновления боевой таблицы очереди
            print('start update_results')
            try:   
                modify_results(json_result, unti_id, deal_table)
            except:
                print('fail')
                break

            print('start update_results')

            ## Заливаем обновление в таблицу истории
            try:
                insert_results_DEAL(deal_id,deal_table,deal_table_hst)   
            except:
                print('fail')
                break
            
            # Таймаут между циклами 1 сек 
            print('start timout')
            count_deal_query = count_deal_query-1
            time.sleep(1)             




##########################################################################
# --------------------------- Настройка DAG ------------------------------ #
##########################################################################


default_args = {'owner': 'Nesh_Markoski',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="University_2035_DealPusher",
        tags = ['CF/CP'],
        description = "Процедура отправки сделок из Битрикс в 1С АКАДА" ,
        schedule_interval='0 7 * * *', 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:


    update_contacts = PythonOperator(
                    task_id='update_contacts',
                    python_callable=update_contacts,
                    op_kwargs={'query_deals': query_deals,
                              'url_update':url_update,
                              'header':header,
                              'update_table':update_table,
                              'update_table_hst':update_table_hst})

    update_deals = PythonOperator(
                    task_id='update_deals',
                    python_callable=update_deals,
                    op_kwargs={'query_deals': query_deals,
                              'url_deals':url_deals,
                              'header':header,
                              'deal_table':deal_table,
                              'deal_table_hst':deal_table_hst,
                              'deal_limit':deal_limit})
 
    update_contacts >> update_deals

