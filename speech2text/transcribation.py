from typing import List
import os
from typing import List
import warnings 
warnings.filterwarnings("ignore")

from vosk import Model, KaldiRecognizer
import wave


import logging

class Transcribation:
    '''
    Класс для транскрибирования моноканального аудио (разбитого на левый и правый каналы звонка)
    Возвращает txt файл с транскрибированным текстом и метаинформацией по звонку
    vad_path - путь до папки, где лдежат разбитые на каналы звонки
    model_dir - путь до папки, где живет модель

    '''
    def __init__(self, vad_path ,model_dir , SAMPLE_RATE = 16000, **kwargs) -> None:
        
   
        self.vad_path = vad_path # путь, куда записываем звонки, разделенные на левый и правый каналы. потом эти аудизаписи удаляются после транскрибации 
        self.model = Model(model_dir) # Создаем обьект модели  подгруженной из локальной директории
        self.SAMPLE_RATE = SAMPLE_RATE
        self.rec = KaldiRecognizer(self.model, self.SAMPLE_RATE) # Создаем модельку
        logging.basicConfig(**kwargs) # Инициализируем логгинг


    def _convert_vad_path_to_text(self, path): # Вместо path передается путь до wav + само название
        '''
        Метод для замены папки и формата файла при транскрибации;
        Метод принимает на вход полный путь до файла, возвращает новый путь с новым форматом
        '''
        new_path = path.replace(r'/Vad/', r'/Transcribs/') # Заменяем путь, чтобы складывать в эту папку транскрибированные звонки
        return new_path.replace('wav', 'txt') # Меняем формат данныъ

        
    def transcribation(self, wav):
        '''
        Метод для транскрибации разделенных на каналы звонков;
        Принимает на вход аудио в формате wav, складывает его в новую папку в виде txt транскрибации
        '''
        self.rec.SetWords(True)
        self.rec.Reset()
        wav = self.vad_path + wav # Путь до аудио + само название аудио, его и передаем в модель
        try:
            res = [] # массив для сбора транскрибации
            with wave.open(wav, "rb") as wf:
                while True: # цикл прекратится когда закончатся данные при очередной итерации wf.readframes(8000)
                    data = wf.readframes(8000) # читаем поочередно часть данных
                    
                    if len(data) == 0:
                        break # прерывание цикла while True когда закончатся данные при очередной итерации wf.readframes(8000)
                    if self.rec.AcceptWaveform(data): # если еще есть данные для транскрибации, то в массив res записываем очередную итерацию транскрибации
                        res.append(self.rec.Result()) # Заполняем массив результатами транскрибации
                    else:
                        self.rec.PartialResult()
                res.append(self.rec.FinalResult())
                res = '\n'.join(res) # получаем итоговый массив, где реплики разделены переносом строки '\n'

            with open(self._convert_vad_path_to_text(wav), 'w+') as f: # записываем данные в txt файл
                f.write(res)
            os.remove(wav) # удаляем wav файлы те, что разделили по каналам left и right)

            return None 
        # Обработка исключений
        except Exception as e:
            logging.error(f'error on transcribation step {e}')