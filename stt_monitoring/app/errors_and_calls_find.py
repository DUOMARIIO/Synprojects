import pandas as pd
import json
from typing import List
import logging 
from datetime import datetime

class Compare:

    def __init__(self, saved_file_path):
        self.saved_file_path = saved_file_path

    def compare(self, current):
        flag = None
        with open(self.saved_file_path, 'r') as saved_file:
            saved_file_data = json.loads(saved_file.read())
            saved = saved_file_data.get('saved')

        if current == saved:
            flag = False
        
        else:
            saved_file_data['saved'] = current
            with open(self.saved_file_path, 'w') as f: # Перезаписываем последнюю сохраненную ошибку для следующей итерации
                json.dump(saved_file_data, f)
            flag = True
        return (flag)


class Error_check(Compare):
    '''
    Класс для проверки возникающих ошибок в ходе работы stt пайплайна по транскрибациям
    '''


    def __init__ (self, saved_file_path, pipeline_errors_path, **kwargs):

        super().__init__(saved_file_path)
        self.pipeline_errors_path = pipeline_errors_path   # Путь до файла с ошибками логгера

        logging.basicConfig(**kwargs)

    def error_check(self):
        '''
        Метод сравнивает последнюю записанную ошибку в файле logging с замороженной ошибкой предыдущей итерации
        Возвращает None если ошибки совпадают и само тело ошибки, в случае несовпадения
        '''
        flag = None # Флаг наличия новой ошибки

        try:
            # Читаем файл с ошибками, возникающими в ходе работы пайплайна по транскрибациям
            with open(self.pipeline_errors_path, 'r') as current_error_file:
                current_error_data = current_error_file.readlines()
                current_error = current_error_data[-1] # Текущая  ошибка в файле логгера
                current_error = current_error_data[-2]+current_error_data[-1] if 'https://sql' in current_error_data[-1] else current_error # потому что такая ошибка записывается в две строчки,

            # Читаем файл с последней ошибкой
            try:
                if super().compare(current= current_error):
                
                    flag = pd.DataFrame([current_error], columns = ['error_body'])  # формируем датафрейм с ошибкой
                    flag['date_time'] = datetime.now()  # Добавляем временную метку
                    flag['is_sent'] = 0
    
                else:
                    flag = None
            except Exception as e:
                logging.error(f'возникла ошибка на этапе error_check проверки наличия свежих ошибок {e}')
  
        except Exception as e:
            logging.error(f'возникла ошибка на этапе error_check проверки наличия свежих ошибок {e}')

        return(flag) # Возвращаем None, если новых ошибок не возникло / саму ошибку в противном случае



class IVR_calls_check(Compare):
    '''
    Класс для проверки не найденных на сервере звонков
    '''
    def __init__(self, saved_file_path, non_found_calls_path, **kwargs) :

        super().__init__(saved_file_path)
        self.non_found_calls_path = non_found_calls_path # Путь до файлика, куда складываем ненайденные на сервере звонки
        logging.basicConfig(**kwargs)

    def ivr_calls_check(self):
        '''
        Метод берет последний залитый батч ненайденных звонков (на один запуск пайплайна по транскрибациям приходится один батч ненайденных звонков).
        При работе с батчем ищет звонки БЕЗ пометки _IVR_. Если находит такие звонки, то отправляет время и дату заливки соответствующего батча + сами звонки
        в формате (audio_oktell_id, path_to call)
        '''
        flag = None # Этой переменной мы присводим None/dataframe в зависимости от наличия не IVR звонков
        sep = '='*100 + '\n' # Разделитель между запусками транскрибации (они же батчи запуска)
        try:
            with open(self.non_found_calls_path, 'r') as file:
                data = file.readlines()
            
            calls_df = pd.DataFrame(data = data, columns = ['oktell_id_path_col']) # Формируем датафрейм со всеми ненайденными на сервере звонками

            curr_day_sep = calls_df.index[calls_df['oktell_id_path_col'] == sep].tolist()[-1] # Находим индекс последнего на текущий день разделителя батчей запуска
            curr_day_date = calls_df.iloc[curr_day_sep + 2].tolist() # Находим дату последней на текущий день заливки в файл
            curr_day_calls_df = calls_df.iloc[curr_day_sep + 3:] # Формируем датафрейм с ненайденными звонками из последнего батча

            non_ivr_calls = [call for call in curr_day_calls_df['oktell_id_path_col'] if 'IVR' not in call] # Формирем список с не IVR звонками, если все звонки IVR - то список пустой

            # Переменная flag = None если список с не IVR звонками пустой.
            # Если не пустой то присваиваем ей список с такими звонками и датой их заливки
            
            if len(non_ivr_calls) == 0:
                flag = None
            else: 

                if super().compare(current= non_ivr_calls[-1]):
                    non_ivr_calls.insert(0, curr_day_date[0])
                    flag = pd.DataFrame(non_ivr_calls, columns = ['error_body']) # Формируем датафрейм из найденных не IVR звонков
                    flag['date_time'] = datetime.now()  # Добавляем временную метку
                    flag['is_sent'] = 0
                else:
                    flag = None
            
        except Exception as e:
            logging.error(f'ошибка на этапе ivr_calls_check - проверки наличия не IRV звонков {e}')

        return (flag)