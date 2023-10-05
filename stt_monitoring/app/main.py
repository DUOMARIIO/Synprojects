
#-------------- Загрузим модули и либы --------------#
# My modules import
from errors_and_calls_find import Error_check, IVR_calls_check
from base_append import Base_append
from config import Config, Config_mngrs

# Standart libs import
from datetime import datetime, timedelta

#-------------- Определим переменные Диспов--------------#

pipeline_errors_path = Config.pipeline_errors_path # Путь до файла с ошибками возникающими при работе stt_pipeline
non_found_calls_path = Config.non_found_calls_path # Путь до файла с ненайденными звонками
saved_error_path = Config.saved_error_path # Путь до файла с последней замороженной ошибкой
saved_call_path = Config.saved_call_path

logging_file = Config.logging_file # Путь до файла с логами процесса мониторинга
login_password_file_path = Config.login_password_file_path # Путь до файла с логином и паролем от тех учетки

sql_table_name_errors = Config.sql_table_name_errors # Название витрины с новыми ошибками
sql_table_name_calls = Config.sql_table_name_calls # Название витрины с не IVR звонками

#-------------- Определим переменные Менеджеров--------------#

man_pipeline_errors_path = Config_mngrs.man_pipeline_errors_path # Путь до файла с ошибками возникающими при работе stt_pipeline
man_non_found_calls_path = Config_mngrs.man_non_found_calls_path # Путь до файла с ненайденными звонками
man_saved_error_path = Config_mngrs.man_saved_error_path # Путь до файла с последней замороженной ошибкой
man_saved_call_path = Config_mngrs.man_saved_call_path

man_sql_table_name_errors = Config_mngrs.sql_table_name_errors # Название витрины с новыми ошибками
man_sql_table_name_calls = Config_mngrs.sql_table_name_calls # Название витрины с не IVR звонками


#-------------- Определим Объекты --------------#

#---- Диспетчеры ----#
disp_pipeline_error_check = Error_check(saved_file_path = saved_error_path,
                                        pipeline_errors_path = pipeline_errors_path,        
                                        filename = logging_file, filemode = 'a', 
                                        format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')

disp_non_found_calls_check = IVR_calls_check(non_found_calls_path = non_found_calls_path,
                                             saved_file_path = saved_call_path,
                                             filename = logging_file, filemode = 'a', 
                                             format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')


disp_push_to_base = Base_append(login_password_file_path = login_password_file_path,
                             filename = logging_file, filemode = 'a', 
                             format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')


#---- Менеджеры ----#

man_pipeline_error_check = Error_check(saved_file_path = man_saved_error_path,
                                       pipeline_errors_path = man_pipeline_errors_path,                                   
                                       filename = logging_file, filemode = 'a', 
                                       format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')

man_non_found_calls_check = IVR_calls_check(non_found_calls_path = man_non_found_calls_path,
                                            saved_file_path= man_saved_call_path,
                                            filename = logging_file, filemode = 'a', 
                                            format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')


man_push_to_base = Base_append(login_password_file_path = login_password_file_path,
                             filename = logging_file, filemode = 'a', 
                             format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')

def check_stt_pipeline():
    #---- Диспетчеры ----#
    # stt errors
    disp_push_to_base.base_append(sql_table_name= sql_table_name_errors, flag= disp_pipeline_error_check.error_check())
    # stt non_found_calls
    disp_push_to_base.base_append(sql_table_name= sql_table_name_calls, flag= disp_non_found_calls_check.ivr_calls_check())
    
    #---- Менеджеры ----#
    # stt managers errors
    man_push_to_base.base_append(sql_table_name= man_sql_table_name_errors, flag= man_pipeline_error_check.error_check())
    # stt managers non_found_calls
    man_push_to_base.base_append(sql_table_name= man_sql_table_name_calls, flag= man_non_found_calls_check.ivr_calls_check())

if __name__ == '__main__':
    check_stt_pipeline()

 

