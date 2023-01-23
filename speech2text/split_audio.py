from typing import List
import os
import librosa as lr
import soundfile as sf
from typing import List
import warnings 
import logging
warnings.filterwarnings("ignore")


class Split_Audio:
    '''
    Модуль производит предобработку аудио, в том числе разделение записи на левый и правый каналы,
    переименование и изменение формата исходного аудио и запись разбитых на два канала аудио в директорию Vad_path

    '''    

    def __init__(self, SAMPLE_RATE = 16000, CHANNELS = ['left', 'right'], **kwargs) -> None:
        self.SAMPLE_RATE = SAMPLE_RATE
        self.CHANNELS = CHANNELS
        logging.basicConfig(**kwargs) # Инициализация логгинга
    
    def _split_audio_to_segments(self, wav) -> List[dict]:
        '''
        Метод для разбиения аудио на левый и правый каналы
        в каждом канале реплики одного из собеседников
        Принимает на вход файл формата wav; выдает два сегмента
        '''
        try:
            audio_segments = [{ 
                'channel': 'left',
                'wav': wav[0],  
                'start': 0,
                'end': len(wav[0])
                },
                {
                    'channel': 'right',
                    'wav': wav[1],
                    'start': 0,
                    'end': len(wav[1])
                }]

            return audio_segments
        except Exception as e:
            logging.error(f'error on step split to audio segments {e}')
    
    def split_audio(self, records_path,  vad_path): 
            """
            records_path - путь до аудиозаписи
            vad_path - путь куда будем складывать аудиозаписи после VADa (в main.py)
            audio_oktell_id - берется id из sql базы
            """
            try:
                wav, _ = lr.load(str(records_path[1]), sr=self.SAMPLE_RATE, mono=False) # Преобразуем аудиозапись в цифровой поток
    
                audio_segments = self._split_audio_to_segments(wav) # Используем функцию разбиения на левый и правый каналы

                for segment in audio_segments: # Аудио сегмента два,  поэтому цикл
                    start = segment['start'] # Время начала сегмента
                    end = segment['end'] # Время конца сегмента
                    channel = segment['channel'] # Канал аудио (левый/правый)

                    # Задаем новое имя для левого/правого аудио
                    new_filename = f'{str(records_path[0])}_{start}_{end}_{channel}.wav'  # records_path[0] == audio_oktell_id
                    
                    # Записываем разбитые звонки в Vad_path
                    new_path = os.path.join(vad_path, new_filename)   
                    
                    sf.write(new_path, segment['wav'], samplerate=self.SAMPLE_RATE)    

            # Логируем ошибки
            except Exception as e:
                
                logging.error(f'error on step split audio{e}, {new_filename}')
            
            return None

