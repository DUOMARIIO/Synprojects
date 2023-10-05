from apiprocess import FromApi
from dfprocess import DfProcessing
from baseprocess import BaseProcessing
from config import Config

token_file_path = Config.token_file_path
logging_file = Config.logging_file
sql_table_name = Config.sql_table_name
login_password_file_path = Config.login_password_file_path
query_path = Config.query_path_2

#---------------Создаем объекты---------------#
# Get dataframe from API
from_api = FromApi(token_file_path = token_file_path,
                 filename = logging_file, filemode = 'a', 
                 format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')

# Processing df we got from API
df_processing = DfProcessing(filename = logging_file, filemode = 'a', 
                            format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')


base_processing = BaseProcessing(sql_table_name = sql_table_name,
                                 login_password_file_path = login_password_file_path,
                                 filename = logging_file, filemode = 'a', 
                                 format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s') 

if __name__ == '__main__':
    print('start')
    # GET запросом получаем датафрейм. Преобразуем его
    print('-'*50)
    processed_df = df_processing.dfprocess(df= from_api.get())
    print('process ok')
    print('-'*50)
    # Последняя загруженная запись в базу
    base_df = base_processing.get_base_df(query_path = query_path)
    print('from base last record ok')
    print('-'*50)
    # Возвращаем то, что выше последней записи
    df_to_append = df_processing.dfcompare(df = processed_df, base_df = base_df)
    print('compare ok')
    # Загружаем в базу
    base_processing.base_append(df = df_to_append)
    print('base append ok')

