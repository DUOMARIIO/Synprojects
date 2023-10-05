from preproc import Preprocessing
from config import Config



result = Preprocessing(file_directory= Config.file_directory)

df = result.preprocessing()

result.base_append(dataframe=df, 
                   sql_table_name= Config.sql_table_name, 
                   login_password_file_path= Config.login_password_file_path)
