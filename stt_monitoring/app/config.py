

class Config:
    '''
    Класс с конфигурациями для мониторинга пайплайна по транскрибациям диспов
    '''
    pipeline_errors_path = r'/app/pipeline_logs/logging.log' # Тут файл с ошибками из stt_pipeline
   # logger_errors_path = r'/mnt/Transcribation_pipeline/pipeline_logs/logging_errors.log'
    #--------------------------------------------
    non_found_calls_path = r'/app/pipeline_logs/not_found_calls.txt' # Тут файл с не найденными звонками из  stt_pipeline
    #non_found_calls_path = r'/mnt/Transcribation_pipeline/pipeline_logs/not_found_calls.txt'
    saved_call_path = r'/app/pipeline_monitoring_files/saved_call_disp.txt'
    #--------------------------------------------
    saved_error_path = r'/app/pipeline_monitoring_files/saved_error.txt' # Тут последняя сохраненная ошибка, с которой сверяемся
    #saved_error_path = r'/mnt/pipeline_monitoring/pipeline_monitoring_files/saved_error.txt'
    #--------------------------------------------
    logging_file = r'/app/pipeline_monitoring_files/logging.log' # Сюда пишутся логи мониторинга
    #logging_file = r'/mnt/pipeline_monitoring/pipeline_monitoring_files/logging.log'
    #--------------------------------------------
    login_password_file_path = r'/app/pipeline_monitoring_files/tech_password.txt' # логин пароль тех учетки для работы с бд
    #login_password_file_path = r'/mnt/pipeline_monitoring/pipeline_monitoring_files/tech_password.txt'
    #--------------------------------------------
    # ВИТРИНЫ
    sql_table_name_errors = r'STT_pipeline_errors' # Сюда заливаем найденные ошибки, если появились
    sql_table_name_calls = r'STT_non_found_calls'  # Сюда заливаем не IVR звонки, если появились


class Config_mngrs:
    '''
    Класс с конфигурациями для мониторинга пайплайна по транскрибациям менеджеров
    '''
    man_pipeline_errors_path = r'/app/pipeline_logs_managers/logging_managers.log' # Тут файл с ошибками из stt_pipeline
   # logger_errors_path = r'/mnt/Transcribation_pipeline/pipeline_logs/logging_errors.log'
    #--------------------------------------------
    man_non_found_calls_path = r'/app/pipeline_logs_managers/not_found_calls_managers.txt' # Тут файл с не найденными звонками из  stt_pipeline
    #non_found_calls_path = r'/mnt/Transcribation_pipeline/pipeline_logs/not_found_calls.txt'
    man_saved_call_path = r'/app/pipeline_monitoring_files/saved_call_managers.txt'
    #--------------------------------------------
    man_saved_error_path = r'/app/pipeline_monitoring_files/saved_error_managers.txt' # Тут последняя сохраненная ошибка, с которой сверяемся
    #saved_error_path = r'/mnt/pipeline_monitoring/pipeline_monitoring_files/saved_error.txt'
    #--------------------------------------------
    man_logging_file = r'/app/pipeline_monitoring_files/logging.log' # Сюда пишутся логи мониторинга
    #logging_file = r'/mnt/pipeline_monitoring/pipeline_monitoring_files/logging.log'
    #--------------------------------------------
    man_login_password_file_path = r'/app/pipeline_monitoring_files/tech_password.txt' # логин пароль тех учетки для работы с бд
    #login_password_file_path = r'/mnt/pipeline_monitoring/pipeline_monitoring_files/tech_password.txt'
    #--------------------------------------------
    # ВИТРИНЫ
    sql_table_name_errors = r'STT_managers_pipeline_errors' # Сюда заливаем найденные ошибки, если появились
    sql_table_name_calls = r'STT_managers_non_found_calls'  # Сюда заливаем не IVR звонки, если появились