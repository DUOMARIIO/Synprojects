from apiprocess import FromApi
from config import Config

'''
Получение токена
'''
get_token = FromApi(token_file_path = Config.token_file_path,
                    filename = Config.logging_file, filemode = 'a', 
                    format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')

if __name__ == '__main__':
    get_token.post()