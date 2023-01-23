from datetime import datetime
import os
import json
import pandas as pd
import logging

# 5.-----------------------------  Постпроцессинг транскрибации

class Transcrib_postpoc:
    '''
    Класс для постпроцессинга транскрибированных файлов
    _transcrib_folder - папка с транскрибированными файлами
    '''
    def __init__(self, _transcrib_folder, **kwargs) -> None:
        self._transcrib_folder = _transcrib_folder # Папка с транскрибированными файлами
        logging.basicConfig(**kwargs) # Инициализируем логгинг


    def transcrib_postpoc(self, transcribation_file_name):
        '''
        Метод для формирования датафрейма с самой транскрибацией и метаинформацией по диалогу;
        Принимает на вход txt файл, возвращает pandas DataFrame
        transcribation_file_name - файл, где хранится результат транскрибации 
        '''
        transcrib_path = os.path.join(self._transcrib_folder, transcribation_file_name) # полный путь до файла
        created_ts = os.path.getctime(transcrib_path) # время последнего изменения файла в Unix или время временем создания файла или каталога в Windows
        created = datetime.fromtimestamp(created_ts) # время created_ts в читабельном формате
        transcribation_file_name = transcribation_file_name.replace('.txt', '') # в названии файла убираем расширение 

        # Постобработка содержимого файла с транскрибацией бля последующей работы с ним
        content = None
        with open(transcrib_path, 'r') as f:
                # TODO fix sentence merging algo in transcriber to remove this: 
                content = f.read().strip()
                content = '[' + content.replace('\n', '')
                content = content.replace('}{', '},{') + ']'
                content = json.loads(content) # получаем json нужного формата

        audio_oktell_id = transcribation_file_name.split('_')[0] # получаем номер аудиозаписи из названия файла - audio_oktell_id
        channel = transcribation_file_name.split('_')[3] # получаем канал (левый или правый) - channel

        starts_list = [replica.get('result')[0].get('start') for replica in content if replica and replica.get('result', None)] # фиксируем список из времени начала реплик
        min_start_offset = None # время начала первой реплики
        if len(starts_list):
            min_start_offset = min(starts_list) # выбираем минимальное время начала реплик
        
        # если есть min_start_offset, т.е. присутсвует хотя бы одна запись в транскрибации, то возвращаем список текстов с метаданными звонка (audio_oktell_id, начала и конец разговора)
        transcrib_to_pd = None
        if min_start_offset:
            try:
                result = [(int(audio_oktell_id), # Октелл id звонка
                    replica.get('result')[0].get('start') - min_start_offset, # Начало реплики
                    replica.get('result')[0].get('end') - min_start_offset, # Конец реплики
                    channel,  # Канал
                    replica.get('text'),  # Транскрибированный текст
                    created, # Время заливки в базу (как итог)
                    json.dumps(replica.get('result')) # Сам json с полным содержанием файла с транскрибацией
                    ) for replica in content if replica['text']]

                os.remove(self._transcrib_folder + transcribation_file_name + '.txt') # Удаляем файл с файлами с транскрибациями
                transcribations = [] # Формируем общий список
                transcribations += result #Заполняем его каждой репликой
                transcrib_to_pd = pd.DataFrame(transcribations, columns=['audio_oktell_id', 'start_time', 'end_time', 'channel', 'text', 'created', 'json']) # Формируем итоговый датафрейм, который заливаем в базу
            except Exception as e:
                logging.error(f' error on postprocessing step {e}')

        # КОСТЫЛЬ ДЛЯ ПУСТОГО ЗВОНКА его тоже заливаем в базу но вместо содержательных полей None
        else:
            try:
                result = [(int(audio_oktell_id), # Октелл id звонка
                None,    # Начало реплики
                None,    # Конец реплики
                channel, # Канал
                None,    # Транскрибированный текст
                created, # Время заливки в базу (как итог)
                None,   #json.dumps(content)
                )]
                os.remove(self._transcrib_folder + transcribation_file_name + '.txt') # Удаляем файл с файлами с транскрибациями, даже если он пустой
                transcribations = []  # Формируем общий список
                transcribations += result # Заполняем его каждой репликой
                transcrib_to_pd = pd.DataFrame(transcribations, columns=['audio_oktell_id', 'start_time', 'end_time', 'channel', 'text', 'created', 'json']) # Формируем итоговый датафрейм, который заливаем в базу

            except Exception as e:
                logging.error(f' error on postprocessing step {e}')
                
        return transcrib_to_pd
