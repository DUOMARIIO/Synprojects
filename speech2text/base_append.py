import urllib
from sqlalchemy import create_engine
import os
import logging
import json

class Base_append:
    '''
    Класс для заливки в базу pandas DataFrame с транскрибациями
    '''
    def __init__(self, **kwargs) -> None:


        logging.basicConfig(**kwargs)
    
    def base_append(self, sql_table_name, transcrib_df, login_password_file_path, transcrib_df_path):
        '''
        Метод для подключения к базе и заливки в нее датафреймов

        sql_table_name - Название таблицы куда заливаем транскрибации
        transcrib_df - датафрейм с транскрибированными репликами и метаинформацией
        login_password_file_path - путь до файла с логином и паролем учетной записи
        transcrib_df_path - файл с транскрибациями текущего батча
        
        '''
        try:
            transcrib_df.to_excel(transcrib_df_path) # Сохраняем датафрейм с транскрибациями в файл
        except:
            pass

        with open(login_password_file_path) as txt_file: # Читаем данные от учетной записи для заливки в базу
            data = txt_file.read()
            jsn = json.loads(data)

            __user_name = jsn['username']
            __user_password = jsn['password']            

        # Создаем подключение к базе (возможно придется переписать под новое подключение)
        driver = 'Driver={ODBC Driver 18 for SQL Server}' + \
                            ';TrustServerCertificate=Yes'+ \
                            ';Server=' + '******' + \
                            ';Database=' + '******' + \
                            f';UID={__user_name}' +  \
                            f';PWD={__user_password}'
                            
        params = urllib.parse.quote_plus(driver)
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

        try:
            transcrib_df.to_sql(sql_table_name, engine,if_exists='append', chunksize=200, index=False) # Подгружаем данные текущей сессии в таблицу 
            print('В базу залили {0} новых записей'.format(transcrib_df.shape[0]))
            os.remove(transcrib_df_path) # если все залилось в базу, то удаляем файл с транскрибацией
        except Exception as e:
            logging.critical(f'error on base append step {e}')
            print('Произошла ошибка при заливке данных в базу')
            print(e)

        return None
