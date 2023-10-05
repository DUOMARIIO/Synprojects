import os
import time
import json
import pyodbc
import urllib
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import logging
import warnings
warnings.filterwarnings('ignore')


class Preprocessing:

    def __init__(self, file_directory, **kwargs):

        self.file_directory = file_directory
        logging.basicConfig(**kwargs)
        
        
    def preprocessing(self) -> pd.DataFrame:
        

        df = None
        #----------------------- Загружаем датафрейм -----------------------#
        try: # Тут будет лежать новая выгрузка
            for file in os.listdir(self.file_directory):
                if os.path.isfile(f'{self.file_directory}/{file}'): 
                    df = pd.read_csv(f'{self.file_directory}/{file}', sep = ';', encoding='utf-8') 

                    
                    print('Начало постобработки')
                    #----------------------- Обработка нового датафрейма -----------------------#
                    # Флаг на платные билеты 
                    df['Флаг на платные билеты'] = df['Цена со скидкой'].apply(lambda x: 1 if x > 0 else 0) 
                    #--------------------------------
                    #  Флаг возврата 
                    df['Флаг возврата'] = df['Цена со скидкой'].apply(lambda x: 1 if x < 0 else 0) 
                    #--------------------------------
                    #Флаг реферальная программа по экскурсиям 
                    df['Флаг реферальная программа (экск)'] = \
                    (
                        df['Тариф/промокод']
                        .str
                        .startswith('sf')
                        .map({True: 1, False: 0})
                    )
                    #--------------------------------
                    # Флаг реферальная программа рассылка 
                    df['Флаг реферальная программа (Рассыл)'] = \
                    (
                        df['Тариф/промокод']
                        .str
                        .startswith('sg')
                        .map({True: 1, False: 0})
                    )
                    #--------------------------------
                    # Флаг кешбек 
                    df['Флаг кешбек'] = \
                    (
                        pd
                        .Series((df['Тариф/промокод'] == 'sber')
                                | (df['Тариф/промокод'] == 'tinkoff20'))
                        .map({True: 1, False: 0})
                    )
                    #--------------------------------
                    # Флаг полная стоимость 
                    df['Флаг полная стоимость'] = \
                    (
                        pd
                        .Series((df['Цена (номинал)'] == df['Цена со скидкой']) 
                                & (df['Цена (номинал)'] > 0) 
                                & (df['Цена со скидкой'] > 0))
                        .map({True: 1, False: 0})
                    )
                    #--------------------------------
                    #Агент оплаты 
                    (
                        df['Агент оплаты']
                        .replace({'-': 'Интикетс',
                                'ООО Интикетс': 'Интикетс',
                                'МДТЗК': 'Тикетленд',
                                'ООО "БИЛЕТНАЯ СИСТЕМА"': 'Билетная система',
                                'ООО "КАССИР.РУ - НБО"': 'Кассир.ру',
                                'ООО Компания Афиша': 'Афиша рамблер',
                                'ООО Яндекс.Медиасервисы': 'Афиша яндекс',
                                'ООО СМАРТБИЛЕТ': 'Почта Банк'
                                }, inplace = True)
                    )
                    #--------------------------------
                    # Источники UTM source 
                    (
                        df['UTM source']
                        .replace({'(none)': 'Органика',
                                'bloggers-lg': 'Блогеры',
                                'yandex_d': 'Яндекс',
                                'yandex_s': 'Яндекс',
                                'eLama-yandex': 'Яндекс',
                                'Inst_free': 'Инстаграм',
                                'maillist': 'E-mail',
                                'maillist#rec508098179': 'E-mail',
                                'maillist#certificate': 'E-mail',
                                'target_mail': 'MyTarget',
                                'Unisender': 'Рассылка по театральным базам'
                                }, inplace = True)
                    )
                    #--------------------------------
                    # Преобразуем телефон в str
                    df['Телефон'].fillna(0, inplace = True)
                    df['Телефон'] = df['Телефон'].apply(lambda x: str(int(x)))
                    #--------------------------------
                    # Преобразуем ряд в int вместо '-' присваиваем значение 0
                    df['Ряд'] = df['Ряд'].apply(lambda x: int(x) if x != '-' else 0)
                    #--------------------------------
                    # Преобразуем место в int
                    df['Место'] = df['Место'].apply(lambda x: int(x) if x != '-' else 0)
                    #--------------------------------
                    # Флаг электронного билета
                    df['Эл. билет'] = \
                    (
                        pd.
                        Series(df['Эл. билет'] == 'v')
                        .map({True: 1, False: 0})
                    )

                    df.rename(columns =  {'Эл. билет': 'Эл билет',
                                        'Эл.почта': 'Эл почта'}, inplace= True)
                    #--------------------------------
                    # Цена номинал делаем все значения положительными 
                    df['Цена (номинал)'] = df['Цена (номинал)'].apply(lambda x: abs(x))
                    #--------------------------------

                    # Меняем время под нужные поля
                    df['Время оплаты'] = (df['Дата оплаты'] +' '+ df['Время оплаты']).apply(lambda x: datetime.strptime(x, '%d.%m.%Y %H:%M'))
                    df['Дата оплаты'] = df['Дата оплаты'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y'))

                    df['Сеанс'] = df['Сеанс'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y %H:%M').date())

                    df['Время бронирования'] = (df['Дата бронирования'] +' '+ df['Время бронирования']).apply(lambda x: datetime.strptime(x, '%d.%m.%Y %H:%M'))
                    df['Дата бронирования'] = df['Дата бронирования'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date())
                    
                #--------------------------------
                    print('Конец предобработки')
                else:
                    print('no file found')

        except Exception as e:
            logging.error(f'error at preprocessing step : {e}')

        return df


    def base_append(self, dataframe: pd.DataFrame ,sql_table_name: str, login_password_file_path: str) -> None:

        if dataframe is None:
            print('Dataframe is None')

        else:
            with open(login_password_file_path) as txt_file: # Читаем данные от учетной записи для заливки в базу 
                data = txt_file.read()
                jsn = json.loads(data)

                __user_name = jsn['username']
                __user_password = jsn['password']
                            
            # Создаем подключение к базе MSSQL
            driver = 'Driver={ODBC Driver 18 for SQL Server}' + \
                                ';TrustServerCertificate=Yes'+ \
                                ';Server=' + 'MSK1-ADB01.synergy.local' + \
                                ';Database=' + 'Analytic' + \
                                f';UID={__user_name}' +  \
                                f';PWD={__user_password}'
                                
            params = urllib.parse.quote_plus(driver)
            engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

        
            try:
                # Текущая дата для названия файла
                curr_day = datetime.now().strftime("%d_%m_%Y") 
                
                # Загружаем в бд
                dataframe.to_sql(sql_table_name, con = engine, if_exists='append', chunksize=200, index=False)
                print('Выгрузили в базу')
                # Загружаем в системную папку
                dataframe.to_excel(f'{self.file_directory}/Гример_{curr_day}.xlsx', index= False)
                print('Выгрузили excel')
                time.sleep(7200)
                for file  in os.listdir(self.file_directory):
                    os.remove(f'{self.file_directory}/{file}')

            
            except Exception as e:
                print(f'error at base append step: {e}')


        return None