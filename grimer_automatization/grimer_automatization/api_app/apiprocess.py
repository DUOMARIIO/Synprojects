#------------------ Загрузка библиотек ------------------#
import json
import logging
import requests
import pandas as pd


#------------------ Взаимодействие с API ------------------#
class FromApi:
    '''
    Класс для взаимодействия с API по лубянскому гримеру
    '''
    def __init__(self, token_file_path, **kwargs):

        self.token_file_path = token_file_path
        logging.basicConfig(**kwargs)

    #--------------------------------------#

    def post(self) -> str:
        '''
        Метод посылает post запрос и обновляет файл с токеном
        '''
        
        # Данные для запроса
        post_url = 'https://friends.xn--90aegkbefgpe1bdez5p.xn--p1ai/api/auth/login'
        post_data = {"email": "admin.lg@synergy.ru",
                     "password": "bdlfghklkjghdf"}
        
        try:
            # Post запрос в API
            responce = requests.post(post_url, json = post_data)

            # Новый токен 
            token = (json
                     .loads(responce.text)
                     .get("access_token"))

            # Записываем новый токен в файл, обновляем его
            new_token = {"token": f"{token}"}
            with open(self.token_file_path, 'w+') as token_file:
                json.dump(new_token, token_file)

        except Exception as e:
            logging.error(f'error on POST step : {e}')
        
        return None
    
    #--------------------------------------#

    def get(self) -> pd.DataFrame:
        '''
        Метод для get запроса к сайту, выдает датафрейм 
        '''
        # Читаем токен для запроса
        with open(self.token_file_path, 'r') as token_file:
            token = (json
                     .loads(token_file.read())
                     .get("token")
                     )
        # Параметры для get запроса
        get_url = 'https://friends.xn--90aegkbefgpe1bdez5p.xn--p1ai/api/leads?include=promocodes&filter[landing]=1'
        headers = {'Authorization': f'Bearer {token}'} 
        df = None
        
        try:
            # GET запрос 
            responce = requests.get(get_url, headers = headers)
            data = (json
                    .loads(responce.text)
                    .get("data"))
            # Формируем датафрейм из таблички, что прислал get запрос
            df = pd.DataFrame(data)
        
        except Exception as e:
            logging.error(f'error on GET step : {e}')
        
        return(df)
    