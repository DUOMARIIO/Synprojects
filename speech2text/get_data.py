import pandas as pd
import logging
import subprocess
import pyodbc
import os
from datetime import datetime 
import json

class Get_data:
    '''
    Модуль для подключения к базе и формирования списка из кортежей.
    В кортеже содержится (audio_oktell_id, path до звонка на сервере)
    В БАЗЕ ТАКЖЕ ВСТРЕЧАЮТСЯ ЗВОНКИ С ПОМЕТКОЙ IVR они заливаются в отдельный файл с ненайденными на сервере звонками !!!!!!
    '''
    def __init__(self, login_password_file_path, non_found_calls,  **kwargs) -> None:

        '''
        **kwargs - параметры для логирования ошибок
        login_password_file_path - Путь до файла с логином и паролем от учетки для запросов в базу
        non_found_calls - путь до файла, где лежат ненайденные звонки
        '''

        self.login_password_file_path = login_password_file_path  # Путь до файла с логином и паролем от учетки
        self.non_found_calls = non_found_calls # Путь до файла, куда пишутся не найденные звонки
        logging.basicConfig(**kwargs) # Инициализация логгера

    def __sqlserver_connection(self):
        with open(self.login_password_file_path) as file: # Читаем файл с логином и паролем для подключения к базе
            data = file.read()
            jsn = json.loads(data)

        __user_name = jsn['username']
        __user_password = jsn['password']

        return pyodbc.connect('Driver={ODBC Driver 18 for SQL Server}' + \
                            ';TrustServerCertificate=Yes'+ \
                            ';Server=' + '******' + \
                            ';Database=' + '******' + \
                            f';UID={__user_name}' +  \
                            f';PWD={__user_password}')


        
    def get_data(self, query_path):
        start_time =  datetime.now()
        columns = ['id', 'recordfile'] # Колонки, которые участвуют в запросе
 
        with open(query_path,'r', encoding='utf-8') as query:  # Читаем из файла sql запрос
            sql_query = query.read() # SQL запрос

        try:
            print('request to database')
            conn = self.__sqlserver_connection()
            cur = conn.cursor()
            cur.execute(sql_query) # Выполняем запрос

            paths_df = pd.DataFrame.from_records(cur.fetchall(), columns=columns) # Формируем pandas DataFrame из данных с запроса
            paths = paths_df.set_index('id').to_dict()['recordfile'] # Формируем словарь с audio_oktell_id + path_to_file
            print('Requested calls: ', paths_df.shape[0])

            pair_list = []
            for id_call, from_path in paths.items():
                pair_list.append(tuple((id_call, from_path))) # Лист с кортежами нужен для дальнейшего мультипроцессинга
            

            # ПРОВЕРЯЕМ НАЛИЧИЕ АУДИОЗАПИСЕЙ ПО ЭТИМ ПУТЯМ, В БАЗЕ МОГУТ БЫТЬ ЗАПИСАНЫ ПУТИ, НО ПО ФАКТУ НА СЕРВЕРЕ ИХ НЕТ. ОСТАВЛЯЕМ ТОЛЬКО ТЕ, ЧТО ЕСТЬ НА СЕРВАКЕ
            print('creating list with tuples (audio_oktell_id, path)')
            
            calls_found = [] # Список из кортежей с найденными на сервере звонками
            calls_not_found = [] # Список с не найденными на сервере звонками

            for file in pair_list:
                if os.path.exists(file[1]):
                    calls_found.append(file)
                else:
                    calls_not_found.append(file)

            print('Сформировали список кортежей, '+' звонков найдено: ', len(calls_found))
            print ('звонков на сервере не найдено: ', len(calls_not_found))

            # Сохраняем ненайденные на сервере звонки в файл
            with open(self.non_found_calls, 'a') as txt_file_1:
                txt_file_1.write('='*100 + '\n')
                txt_file_1.write('всего : ' + str(len(calls_not_found)) + '\n')
                txt_file_1.write(str(start_time) + '\n') # Записываем время для очередной порции ненайденных
                for call in calls_not_found:
                    txt_file_1.write(str(call) + '\n')
            
            return(calls_found)
        
        # Логируем ошибки в файл test.log
        except Exception as e:
             logging.critical(f'Error at the step of requesting to the database and preprocessing {e}')
             failure_flag = True

    ########################################################
    ########################################################
    ########################################################

    # Метод для батчевых выгрузок из xlsx файла
    def get_data_batch(self, df_path):
             
            '''
            Метод для работы с тестовыми выгрузками
            Принимает на вход pandas df -> на выход list (tuples)
            Метод для формирования списков с кортежами 
            '''
            paths = None
            try:  
                start_time = datetime.now()
                paths_df = pd.read_excel(df_path)
            
                try:
                    paths = paths_df.set_index('Id').to_dict()['RecordFile'] 
                except:
                    pass
                    #paths = paths_df.set_index('id').to_dict()['RecordFile']
                
                pair_list = []
                print('creating list with tuples (audio_oktell_id, path)')
                for id_call, from_path in paths.items():
                    pair_list.append(tuple((id_call, from_path))) # Лист с кортежами нужен для дальнейшего мультипроцессинга


                calls_found = [] # Список из кортежей с найденными на сервере звонками
                calls_not_found = [] # Список с не найденными на сервере звонками
                for file in pair_list:
                    if os.path.exists(file[1]):
                        calls_found.append(file)
                    else:
                        calls_not_found.append(file)
                print('Найдено звонков: ', len(calls_found))
                print('Не найдено звонков: ', len(calls_not_found))

                # Записываем ненайденные звонки в отдельный файл
                with open(self.non_found_calls, 'a') as txt_file: 
                    txt_file.write('='*100 + '\n')
                    txt_file.write(str(start_time) + '\n')
                    txt_file.write('всего: '+ str(len(calls_not_found)) + '\n')
                    for call in calls_not_found:
                        txt_file.write(str(call) + '\n')

                return(calls_found)

            except Exception as e:
                logging.critical(f'Error at the step of requesting data to the database and preprocessing {e}')