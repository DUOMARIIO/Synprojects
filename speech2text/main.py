# IMPORT MODULES
import get_data as gd 
import split_audio as sa 
import transcribation as trans 
import transcribation_postprocessing as tp 
import base_append as ba

# IMPORT TEST MODULES

# IMPORT LIBS
import pandas as pd 
import multiprocessing 
from multiprocessing import Pool 
from datetime import datetime 
import os 
from sqlalchemy import create_engine

# README
'''
stt_omar - папка, в которой лежат: - все питоновские файлы 
                                   - среда выполнения 
                                   - зависимости 
                                   - sql скрипт 
                                   - логин_пароль тех учетки
Transcribation_pipeline - тут лежат все артефакты, возникающие по ходу выполнения пайплайна:

                                   - Wav правого и левого канала (Vad)
                                   - Транскрибации (Transcribs)
                                   - Большой датафрейм с постобработанными транскрибациями (transcribs_df)
                                   - Файл с логированием ошибок (pipeline_logs/logging.log )
                                   - Файл с логированием cron (pipeline_logs/cron_log.txt)
                                   - Файл с ненайденными звонками (pipeline_logs/non_found_calls.txt)

'''


# PATHS
login_password_file_path = r'******' # Тут лежит файлик с логином и паролем технической учетки
#--------------------------------------------
non_found_calls = r'******' # Тут лежат ненайденные звонки в формате (oktell_id, audio_path)
#--------------------------------------------
logging_file = r'******' # Сюда записываются ошибки, возникающие по ходу выполнения пайплайна
#--------------------------------------------
query_path = r'******' # Тут лежит файл с запросом к базе 
#--------------------------------------------
vad_path = r'******' # Путь по которому складываем уже разбитые аудио на каналы 
#--------------------------------------------
model_dir = r'******' # Тут лежит модель
#--------------------------------------------
_transcrib_folder = r'******'  # Тут лежат транскрибации
#--------------------------------------------
transcrib_df_path = r'******' # Сюда записывается датафрейм с транскрибациями очередного батча 
#--------------------------------------------
# ВИТРИНЫ
table_name = r'******' # Название витрины для разговоров диспетчеров
test_table_name = r'******' # Название витрины для разговоров манагеров


###########################

num_cores = 7 # Колличество ядер
###########################
###########################

# GET DATA FROM DATABASE (LIST WITH TUPLES(audio_oktell_id, audio_path))
data_test = gd.Get_data(login_password_file_path = login_password_file_path , non_found_calls = non_found_calls,
                        filename = logging_file, filemode = 'a', 
                        format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')

# SPLIT AUDIOS TO LEFT/RIGHT CHANNELS AND SAVE THEM INTO vad_path 
split = sa.Split_Audio(filename = logging_file, filemode = 'a', 
                       format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s') 

def splitted_audio(record_path): 
    split.split_audio(records_path = record_path, vad_path = vad_path) 
 
def pool_handler_3(): # Объект для параллельного разбиения на каналы
    p = multiprocessing.Pool(num_cores) # ЗАДАЙ КОЛ-ВО ЯДЕР!!!!!!!!! 
    p.map(splitted_audio, data_for_split_batch) 
 
 
# TRANSCRIBATION 4 
transcrib = trans.Transcribation(vad_path = vad_path, model_dir = model_dir,
                                 filename = logging_file, filemode = 'a', 
                                 format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s') 
def transcribate(wav): 
    transcrib.transcribation(wav = wav) 
 
def pool_handler_4(): # Объект для параллельного транскрибирования
    p = multiprocessing.Pool(num_cores) # ЗАДАЙ КОЛ-ВО ЯДЕР!!!!!!!!! 
    p.map(transcribate, waws) 

 
#TESTIM PIPELINE 
if __name__ == '__main__':
    
    start_time = datetime.now() # Время начала пайплайна
    print('='*70)
    print('Pipeline starttime: ', str(start_time))
    try:
        data_for_split = data_test.get_data(query_path = query_path)
        print('Finished модуль выгрузки из базы', datetime.now())

        i = 0
        step = 500 # Размер батча который обрабатываем и заливаем в базу

        while i < len(data_for_split):
            print('Начинало работы над батчем № ', int(i/500))
            if i+step < len(data_for_split):
                data_for_split_batch = data_for_split[i:i+step]
            else:
                data_for_split_batch = data_for_split[i:]


            print('Start модуль разделения на каналы ')
            pool_handler_3() # Разделение на каналы
            print('Finished модуль разделения на каналы ', datetime.now())


            del data_for_split_batch # Удаляем из глобального пространства имен список с wav

            print('Start cбор аудио для транскрибаций ')
            waws = [f for f in os.listdir(vad_path) if f and 'wav' in f] 
            print('Finish сбор аудио для транскрибаций ', datetime.now())

            print('Start модуль транскрибации ')
            pool_handler_4() 
            print('Finish модуль транскрибации ', datetime.now())
            del waws # Удаляем из глобального пространства имен список с wav

            print('Start постобработка транскрибаций')
            transcrib_df = pd.DataFrame() # Сюда заливается вся партиция за отработку пайплайна, его и грузим в базу
            transc_postproc = tp.Transcrib_postpoc(_transcrib_folder = _transcrib_folder, filename = logging_file, filemode = 'a', 
                                                    format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')  # Экземпляр класса для постобработки аудио
            
            transcribs_list = [x for x in os.listdir(_transcrib_folder) if 'txt' in x] # Собираем список транскрибаций после отработки 4 модуля   
            for transcrib_txt in transcribs_list: # Проходимся циклом по всем транскрибациям, для каждой формируем маленький датафрейм и конкатенируем с основным
                res = transc_postproc.transcrib_postpoc(transcrib_txt)  # Применяем функцию постобработки для файла транскрибации
                transcrib_df = pd.concat([transcrib_df, res], axis = 0) # Конкатенируем полученный датафрейм к исходному
            
            transcrib_df['created'] = datetime.now() # Заменяем столбец времени, чтобы было 
            print('Finish постобработка транскрибаций',datetime.now())

            try:
                #КОСТЫЛЬ, ЕЩЕ ОДИН ПРЕДОХРАНИТЕЛЬ ДЛЯ ХРАНЕНИЯ ТРАНСКРИБИРОВАННЫХ ЗВОНКОВ
                transcrib_df.to_excel(transcrib_df_path)
            except:
                pass


            # BASE APPEND
            apend = ba.Base_append(filename = logging_file, filemode = 'a', 
                                format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')

            apend.base_append(sql_table_name = table_name, transcrib_df = transcrib_df, 
                            login_password_file_path = login_password_file_path,
                            transcrib_df_path = transcrib_df_path)
            
            print('Времени с начала пайплайна: ', datetime.now() - start_time)
            print('Конец работы над батчем № ', int(i/500))
            print('-'*20)
            i+=step
    except:
        pass

    print('Время отработки всего пайплайна: ',datetime.now() - start_time)
