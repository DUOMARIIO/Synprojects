import pandas as pd
import urllib
from sqlalchemy import create_engine
import logging
import json

class Base_append:
    '''
    Класс для заливки в базу pandas DataFrame с транскрибациями
    '''
    def __init__(self, login_password_file_path, **kwargs) -> None:
        '''
         sql_table_name - Название таблицы куда заливаем ошибки
         login_password_file_path - путь до файла с логином и паролем учетной записи
        '''


        self.login_password_file_path = login_password_file_path

        logging.basicConfig(**kwargs)
    
    def base_append(self, sql_table_name, flag):
        '''
        Метод для подключения к базе и заливки в нее датафреймов
        sql_table_name - Название таблицы куда заливаем ошибки  / не IVR звонки с датой 
        flag - None или pandas DataFrame с ошибкой / не IVR звонками и датой 
        
        '''
        if flag is None:
            pass

        else: 
            try:
                with open(self.login_password_file_path) as txt_file: # Читаем данные от учетной записи для заливки в базу
                    data = txt_file.read()
                    jsn = json.loads(data)

                    __user_name = jsn['username']
                    __user_password = jsn['password']            

                # Создаем подключение к базе (возможно придется переписать под новое подключение)
                driver = 'Driver={ODBC Driver 18 for SQL Server}' + \
                                    ';TrustServerCertificate=Yes'+ \
                                    ';Server=' + 'MSK1-ADB01.synergy.local' + \
                                    ';Database=' + 'Analytic' + \
                                    f';UID={__user_name}' +  \
                                    f';PWD={__user_password}'
                                    
                params = urllib.parse.quote_plus(driver)
                engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

            except Exception as e:
                logging.error(f'ошибка на этапе base append create engine {e}')

            try:
                
                flag.to_sql(sql_table_name, engine,if_exists='append', chunksize=200, index=True) # Подгружаем данные текущей сессии в таблицу

            except Exception as e:
                logging.error(f'ошибка на этапе base_append to sql {e}')

        return None
