#------------------ Загрузка библиотек ------------------#
import json
import pyodbc
import urllib
import logging
import pandas as pd
from sqlalchemy import create_engine


class BaseProcessing:
    
    def __init__(self, sql_table_name: str, login_password_file_path: str, **kwargs):
    
        self.sql_table_name = sql_table_name
        self.login_password_file_path = login_password_file_path
        logging.basicConfig(**kwargs)
        
    #------------------ Встроенные функции ------------------#
    def __sqlserver_connection(self):
        '''
        Метод для создания подключения к базе
        '''
        with open(self.login_password_file_path) as file: # Читаем файл с логином и паролем для подключения к базе
            data = file.read()
            jsn = json.loads(data)

        __user_name = jsn['username']
        __user_password = jsn['password']
        
        driver = 'Driver={ODBC Driver 18 for SQL Server}' + \
                            ';TrustServerCertificate=Yes'+ \
                            ';Server=' + 'MSK1-BIDB01.synergy.local' + \
                            ';Database=' + 'ANALYTIC' + \
                            f';UID={__user_name}' +  \
                            f';PWD={__user_password}'
        
        return driver

    #------------------ Основные методы ------------------#

    def get_base_df(self, query_path) -> pd.DataFrame:
        '''
        Метод для выгрузки последнего значения из базы
        '''
        columns = ['id', 'name', 'phone', 'email', 'promocodes', 'created_at'] # Колонки, которые участвуют в запросе
        # Последняя запись, она же base_df
        last_record = None

        try:
            with open(query_path,'r', encoding='utf-8') as query:  # Читаем из файла sql запрос
                sql_query = query.read() # SQL запрос
                
        except Exception as e:
            logging.error(f'error at step get data, something wrong with sql_query {e}')

        try:

            conn = pyodbc.connect(self.__sqlserver_connection()) # Создаем соединение с нашей базой данных
            cur = conn.cursor()  # Создаем курсор - это специальный объект который делает запросы и получает их результаты
            cur.execute(sql_query) # Выполняем запрос
            last_record = pd.DataFrame.from_records(cur.fetchall(), columns=columns) # Формируем pandas DataFrame из данных с запроса
        
        except Exception as e:
            logging.error(f'error at step query execution {e}')
        
        return(last_record)
    
    #----------------------------------

    def base_append(self, df: pd.DataFrame) -> None:
        '''
        Метод для загрузки нужного датафрейма в базу
        '''

        if df is None:
            print('Dataframe is None')

        else:

            try:
                params = urllib.parse.quote_plus(self.__sqlserver_connection())
                engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
            except Exception as e:
                logging.error(f'error at step base append {e}')

            try:
                # Загружаем в бд
                df.to_sql(self.sql_table_name, con = engine, if_exists='append', chunksize=200, index=False)
            except Exception as e:
                logging.error(f'error at step base append {e}')

        return None