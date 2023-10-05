#------------------ Загрузка библиотек ------------------#
import re
from datetime import datetime
import pandas as pd
import logging

#------------------ Обработка датафреймов ------------------#   
class DfProcessing:
    '''
    Класс для обработки датафреймов

    get_date_time - внутренний метод для работы с полем даты-времени
    preprocess - метод для преобразования полей в dataframe

    '''
    def __init__(self, **kwargs) -> None:

        logging.basicConfig(**kwargs)
        
    #------------------ Встроенные функции ------------------#
    def _get_date_time(self, date: str):
        '''
        Внутренний метод для работы с полем даты-времени
        Избавляемся от лишних элементов в поле дата время
        '''
        try:
            raw_date_time = date.split(".")[0]

            date = raw_date_time.split("T")[0]
            time = raw_date_time.split("T")[1]
            date_time = date + ' ' + time

            return(datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S'))
        except Exception as e:
            logging.error(f'error at _get_date_time step {e}')
            
    #------------------ Основные методы ------------------#
    
    def dfprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Метод для обработки исходного датафрейма
        обрабатывает колонки по условиям
        '''
        try:
            # Оставляем только дату и время
            df['created_at'] = df['created_at'].apply(lambda x: self._get_date_time(x) if x else None)
            #-----------------
            #Оставляем поле промокода
            df['promocodes'] = df['promocodes'].apply(lambda x: x[0].get("code") if x else None)
            #-----------------
            #Оставляем только цифры
            df['phone'] = df['phone'].apply(lambda x: ''.join(re.findall('\d+', x)) if x else None)
            # Возвращаем обработанный датафрейм
            return(df)
        
        except Exception as e:
            logging.error(f'error at preprocessing step {e}')
            return 
              
    #----------------------------------

    def dfcompare(self, df: pd.DataFrame, base_df: pd.DataFrame):
        '''
        Метод сравнивает два df и str_df
        находит индекс последней загрузки
        возвращает ту часть, что еще не загружена в базу

        Обновленный метод сравнивает два датафрейма и возвращает антиджойн
        '''
        # Алгоритм сравнения через исключение найденного в базе
        try:
            id_list_frombase = base_df['id'].tolist()
            return(df.query(f"id not in {id_list_frombase}"))

        except Exception as e:
            logging.error(f'error at compare step  {e}')
            return None
    

        # Алгоритм сравнения через последнюю  запись в базе

        # try:
        #     # Ячейки по значениям которых будем находить нужный индекс
        #     base_list = \
        #     (
        #         base_df[['id','name','phone','email', 'promocodes']]
        #         .iloc[0]
        #         .tolist()
        #     )

        # except Exception as e:
        #     logging.error(f'error at compare step 1 {e}')
            
        
        # try:
        #     # Индекс последней записи в бд
        #     index_ = \
        #     (
        #         df
        #         .index[(df['id'] == base_list[0])
        #               &(df['name'] == base_list[1])
        #               &(df['phone'] == base_list[2])
        #               &(df['email'] == base_list[3])
        #               &(df['promocodes'] == base_list[4])]
        #         .tolist()[0]
        #     )
            
        #     # Возвращаем все, что выше последней записи в бд
        #     # Это и будут новые данные

        #     return(df[:index_])
        
        # except Exception as e:
        #     logging.error(f'error at compare step 2 {e}')
            