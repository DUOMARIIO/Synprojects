from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from sqlalchemy import create_engine
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import time
import urllib
import requests as req


# ------------------------- ДОП. ПАРАМЕТРЫ  ---------------------------- #

## URL для обновления контакта
url_comment = 'https://corp.synergy.ru/api/api_d_crm/crm_update_timeline'

#№ URL для обновления сделки
url_deals   = 'https://corp.synergy.ru/api/api_d_crm/crm_update_deal'

## Токен авторизации
token = Variable.get("CRM_API_Token")
header = {'App-Token': f'{token}'}

## Main query for get_df()
query_main = '''
select
[DEAL_ID]
,CONTACT_ID 
,UNTI_ID
,[PRODUCT_ID]
,[PRICE]
,'24' [STAGE_ID]
,''[UF_CRM_STATUS_DP_LMS]
,''[UF_OPERATOR_STATUS]
,''[UF_CRM_CONTROL]
,'1' [UF_CRM_1498200987]
,'2616492' [UF_CRM_1492016348]
,'100916533' [UF_CRM_1495622099]
,DEAL_ID [ENTITY_ID]
,'2' [ENTITY_TYPE_ID]
,'Y' [IS_FIXED]
,a.REASON [COMMENT]
,a.REASON
,[IS_SENT]

from [dbo].[NM_cp_deal_fails] a
where IS_SENT = 0
'''

# --------------------------- Подключения ------------------------------ #
## DataBase connection
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

# --------------------------- Вспомогательные функции ------------------------------ #
# Фнукция для выгрузки датафрейма
def get_df(query_main):
    df_main = pd.read_sql_query(query_main,con=db_conn())
    return df_main


## Json generator
# json for deal
def get_deal_json(df_main):
       df_deals = df_main[['DEAL_ID'
                    ,'PRODUCT_ID'
                    ,'PRICE'
                    ,'STAGE_ID'
                    ,'UF_CRM_STATUS_DP_LMS'
                    ,'UF_OPERATOR_STATUS'
                    ,'UF_CRM_CONTROL'
                    ,'UF_CRM_1498200987'
                    ,'UF_CRM_1492016348'
                    ,'UF_CRM_1495622099'
                    ]] 
       deal_json = df_deals.to_json()
       return deal_json

# json for comment
def get_comment_json(df_main):
       df_comments = df_main[['ENTITY_ID'
                    ,'ENTITY_TYPE_ID'
                    ,'IS_FIXED'
                    ,'COMMENT'
                    ]]  
       comment_json = df_comments.to_json()
       return comment_json

# update data in db
def update_db(unti):
   
    update_query = f'''
    update [dbo].[NM_cp_deal_fails]
    set IS_SENT = 1
    from [dbo].[NM_cp_deal_fails]
    where UNTI_ID = {unti}
    '''
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
    print('update success')



# --------------------------- Основной цикл ------------------------------ #

def data_updater(query_main,url_comment,header,url_deals):
    df_main = get_df(query_main)
    result = []

    for index, row in df_main.iterrows():
        _json_deal = get_deal_json(row)
        _json_comment = get_comment_json(row)
        _deal_id = row['DEAL_ID']
        _unti = row['UNTI_ID']

        ## insert comment to bitrix
        print(f'try comment to deal: {_deal_id}')
        try:
            res_comment = req.post(url=url_comment, headers= header, data= _json_comment)
            json_result_comment = res_comment.json()
            print(f'success: {json_result_comment}')
        except:
            print('fail')

    
        ## update status deal
        try:
            print(f'try update :{_deal_id}')
            res_deal = req.post(url=url_deals, headers= header, data= _json_deal)
            json_result_deal = res_deal.json()
            print(f'success: {json_result_deal}')
        except:
            print('fail')

        ## update main table in db
        print(f'try update db by unti: {_unti}')
        try:
            update_db(_unti)
        except:
            print('fail')

        ## create df for insert in hst table in db
        print('try to append hst in db')
        try:
            result.append([row['DEAL_ID'], str(json_result_comment), str(json_result_deal), datetime.now()])
        except:
            print('fail')

    ## inserting df into hst table in db
    data = pd.DataFrame(result,columns=['DEAL_ID','ANSWER_COMMENT','ANSWER_DEAL','SYSMOMENT'])       
    hst_data = df_main.merge(data)
    hst_data = hst_data[[
                        'DEAL_ID'
                        , 'UNTI_ID'
                        , 'REASON'
                        , 'ANSWER_COMMENT'
                        , 'ANSWER_DEAL'
                        , 'SYSMOMENT'
                        ]]
    hst_data.to_sql('NM_cp_deal_fails_HST', con=db_conn() ,index=False ,if_exists='append')
    



 # --------------------------- Настройка DAG ------------------------------ #
default_args = {'owner': 'Nesh_Markoski',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="University_2035_DealCancel",
        tags = ['CF/CP'],
        description = "Процедура отмены некорректных сделок в Битрикс" ,
        schedule_interval='0 7 * * *', 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:


    data_updater = PythonOperator(
                    task_id='data_updater',
                    python_callable=data_updater,
                    op_kwargs={'query_main': query_main,
                              'url_comment':url_comment,
                              'header':header,
                              'url_deals':url_deals})

 
    data_updater  