from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.operators.python import ShortCircuitOperator

import logging
import pyodbc
import pandas as pd
from datetime import timedelta
from ast import literal_eval
import gc
import urllib
from datetime import datetime
from sqlalchemy import create_engine


# ------------------------- Определение базовых переменных и конфигураций ------------------------ #

# Настройка логгера

logging.basicConfig(filename = '/opt/airflow/dag_logs/tr_check/tr_check.log', 
                    filemode = 'a',
                    level = logging.INFO, # Уровень записей info
                    format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')

# Запрос в БД на выгрузку транскрибаций звонков, а так же названия колонок таблицы запроса

sql_query = """SELECT *
                FROM [dbo].[Call_ID_Lead_ID] CL LEFT JOIN [Analytic].[dbo].[STT_Transcribations_test] STT_test
                ON CL.Call_Id = STT_test.audio_oktell_id 
                WHERE 1=1
                AND (CAST(channel AS VARCHAR(50)) = 'left' or channel is null)
                AND CL.Call_Resourse = 'Outcoming call'
                AND (Month(CL.TimeStart) >= (MONTH(GETDATE())-1))
                AND CL.Lead_Id not in (SELECT Lead_Id FROM [dbo].[STT_check_metric_test]
                                        WHERE 1=1
                                        AND (Month(TimeStart) >= (MONTH(GETDATE())- 1)))"""
                    
columns=['Call_Id', 'IdOktell', 'IdChain', 'Lead_Number', 'Disp_login',
            'Lead_Id', 'DATE_CREATE', 'TimeStart', 'Manager_login', 'Call_Resourse',
            'SYS_MOMENT', 'id', 'audio_oktell_id', 'start_time', 'end_time',
            'channel', 'text', 'created', 'json']

# Словарь с порогами по отработкам по каждому пункту проверок
threshhold_dict = {'greeting_check' : 1,
                    'performance_check' : 4,
                    'comfort_check' : 3, 
                    'city_check' : 1, 
                    'lvl_check' : 2, 
                    'faculty_check' : 2, 
                    'trans_check' : 1}

# --- Базовые переменные --- #

# Путь и название временного файла через который таски будут обмениваться данными 
file_path = f'{Variable.get("temp_files")}/STT/tr_check.csv'

# Почта для отправки уведомления об ошибке
email = 'ISamigullin@synergy.ru'

# Драйвер для подключения к БД
driver = 'Driver={ODBC Driver 18 for SQL Server}' + \
            ';TrustServerCertificate=Yes'+ \
            ';Server=' + 'MSK1-ADB01.synergy.local' + \
            ';Database=' + 'Analytic' + \
            f';UID={Variable.get("tuz_login")}' +  \
            f';PWD={Variable.get("tuz_password")}'


def query_to_db(sql_query : str, columns : list, file_path : str, driver : str):
  
    try:

        # Подключение к БД и select запрос на выгрузку не оцененных 
        conn = pyodbc.connect(driver)
        cur = conn.cursor()
        cur.execute(sql_query)
        tr_df = pd.DataFrame.from_records(cur.fetchall(), columns=columns)

        # Склеивание транскрибаций
        tr_df.dropna(inplace=True)
        tr_df['Transcribation'] = tr_df.groupby('Lead_Id')['text'].transform(' '.join)
        tr_df['id'] = tr_df['id'].astype(int)

        # Сохраниение dataframe во временный файл 
        tr_df.to_csv(file_path, index=False)

        # Запись размера dataframe в xcom
        return(tr_df.shape[0])

    except Exception as e:
        logging.critical(f'Error at the step of requesting data to the database and preprocessing (sql_query, __connection_to_db) - {e}')
        print('*' * 30)
        raise ValueError

def data_volume(**kwargs):
    xcom_value = int(kwargs['ti'].xcom_pull(task_ids='query_to_db'))
    if xcom_value > 0:
        return True
    else:
        return False

def confidence_extract(lead_data):
    # Извлечение confidence
    try:
        confidences = []
        for _, row in lead_data.iterrows():
            conf = literal_eval(row['json'])
            for word_data in conf:
                confidences.append(word_data['conf'])
        return confidences
    except Exception as e:
        logging.error(f'Error at the step confidence_extract - {e}')



def extract_data(file_path : str):
    """
    Извлечение части данных из data_frame.
    На данном этапе формируются следующие колонки:
    - confidences - список confidence для каждого транскрибированного слова;
    - end_id - id конца разговора (из таблицы STT_Transcribations);
    - duration_talk - продолжительность разговора;
    - mean_conf - среднее значение confidence для транскрибации.
    
    После обработки остаются только необходимые колонки. Атрибут columns_order
    """

    try:
        tr_df = pd.read_csv(file_path)
        result = pd.DataFrame()
        lead_ids_list = tr_df['Lead_Id'].unique().tolist()

        # Итерирование по ID_Lead
        for lead_id in lead_ids_list:
            lead_data = tr_df[tr_df['Lead_Id']==lead_id]
            # Извлечение confidences для записи
            confidences = confidence_extract(lead_data)
            # Получение end_id записи
            end_id = lead_data['id'].iloc[-1]
            # Получение duration_talk записи
            duration_talk = lead_data['end_time'].iloc[-1]
            # Объединение транскрибаций
            transcribation = lead_data['text'].str.cat(sep=' ')
            # Расчет среднего confidence
            mean_conf = sum(confidences)/len(confidences)
            # Формирование итогового набора данных для лида
            lead_data = lead_data.iloc[:1]
            lead_data['confidences'] = str(confidences)
            lead_data[['end_id', 'duration_talk', 'transcribation', 'confidences', 'mean_confidence', 'words_count']] =  \
                    [end_id, duration_talk, transcribation, str(confidences), mean_conf, len(confidences)]
            result = pd.concat([result, lead_data])
        
        # Формирование итоговой таблицы
        columns_order = ['Call_Id', 'IdOktell', 'IdChain', 'Lead_Number', 'Disp_login', 'Lead_Id', 
                        'DATE_CREATE', 'TimeStart' , 'Manager_login', 'id', 'end_id',
                        'audio_oktell_id', 'duration_talk', 'transcribation', 'confidences',
                        'mean_confidence','words_count']
        tr_df = result[columns_order].reset_index(drop=True)
        tr_df.to_csv(file_path, index=False)
        # Все помежуточные результаты хранящиеся в df result удаляются из ОП
        del(result)
        gc.collect()
    except Exception as e:

        extract_data_variables = ['lead_id']
        extract_data_variables = {variable : str(globals()[variable]) for variable in  \
                                    extract_data_variables if variable in locals()}
        logging.error(f'Error at the step key_word_checker - {e} - iter - {extract_data_variables}') 


def key_word_checker(file_path : str, key_words_df_path : str, threshhold_dict:dict):
    """
    Данный метод производит проверку включения ключевых слов в транскрибацию и оценку качества диалога диспетчера
    с лидом.

    Принимаемые аргументы:
        - key_words_df - Dataframe с набором ключевых слов распределенными по корпусам проверок;
        - threshhold_dict - словарь с порогами отсечки по кажому корпусу проверок.
    """
    from natasha import Doc, Segmenter, MorphVocab, NewsEmbedding, NewsMorphTagger
    import ru_core_news_sm
    import pymorphy2
    from flashtext.keyword import KeywordProcessor
    import pandas as pd
    import logging 
    
    

    segmenter = Segmenter()
    morph_vocab = MorphVocab()
    emb = NewsEmbedding()
    morph_tagger = NewsMorphTagger(emb)
    nlp = ru_core_news_sm.load()
    stopwords = nlp.Defaults.stop_words

    logging.basicConfig(filename = '/opt/airflow/dag_logs/tr_check/tr_check.log', 
                    filemode = 'a',
                    level = logging.INFO, # Уровень записей info
                    format = '%(name)s - %(levelname)s - %(message)s - %(asctime)s')


    def lemmatizer(input_text : list):
        """
        Лемматизация текста
        Принимаемые аргументы:
            - input_text - список из лемматизируемого текса

        """

        input_text = list(input_text)
        # Проход по каждой транскрибации и удаление всех не алфавитных символов (процедура не обязательная) и приведение к нижнему регистру
        
        try:
            # Лемматизация входного набора текстов с помощью библиотеки natasha
            for i, text in enumerate(input_text):

                # Удаление стоп слов
                text = [word for word in text.split() if word not in stopwords]
                text = ' '.join(text)

                # Инициализация класса Doc библиотеки natasha и передача в него очищенного текста с последующими операциями segmenter, morph_tagger (Такая последовательность требуется библиотекой)
                doc = Doc(text)
                doc.segment(segmenter)
                doc.tag_morph(morph_tagger)

                # Лемматизация токенов (слов)
                for token in doc.tokens:
                    token.lemmatize(morph_vocab)            

                # Сбор лемматизированных токенов
                lemmatized_text = [token.lemma for token in doc.tokens]
                lemmatized_text = ' '.join(lemmatized_text)


                # Замена исходного текста из input_text на лемматизировнный текст
                input_text[i] = lemmatized_text 
            return input_text 
        except Exception as e:
            logging.error(f'Error at the step lemmatization - {e} - iter - {i, text}')


    key_words_df = pd.read_excel(key_words_df_path)
    tr_df = pd.read_csv(file_path)
    
    # Лемматизация 
    lemmatized_transcribations = lemmatizer(tr_df['transcribation'])

    tr_df['lemmatized_transcribations'] = lemmatized_transcribations

    # Выделение названий колок корпусов
    corpus_list = key_words_df.columns.to_list()
    # Список для сохраниения имен колонок с результатами сверки метрик
    metrics_list = []
    try:
        # Проход по корпусам проверок
        for corpus in corpus_list:
            metric = []
            # Выделение ключевых слов из корпусов
            corpus_check =  key_words_df[corpus].dropna().values
            # Выделение наборов ключевых слов (Синонимичные слова)
            multicheck_words = [milticheck for milticheck in corpus_check if milticheck.find(',') != -1]  
            # Выделение отдельных ключевых слов
            separately_check_words = [sep for sep in corpus_check if sep.find(',') == -1]
            # Процессор ключевых слов для отдельных слов(separately_check_words)
            sep_keyword_processor = KeywordProcessor()
            sep_keyword_processor.add_keywords_from_list(separately_check_words)

            # Список для хранения результатов проверки корпуса
            corpus_check_list = []
            
            for transcribation in lemmatized_transcribations:
                # Cчетчик вхождения ключевых слов корпуса в транскрибации
                sep_found_words = set(sep_keyword_processor.extract_keywords(transcribation))
                # Счетчик вхождения мультислов в транскрибацию
                multiwords_counter = 0
                # Лист сбора обнаруженных мультислов
                multiwords_list = []

                if len(multicheck_words):

                    # Обработка наборов слов в multicheck_words (Удаление пробелов для удобства дальнейшей обработки)
                    multicheck_words = [milticheck.replace(' ', '') for milticheck in multicheck_words]
                    
                    for milticheck in multicheck_words:
                        # Процессор ключевых слов для отдельных слов(Для наборов слов)
                        milticheck_keyword_processor = KeywordProcessor()
                        # milticheck представляет собой объект типа str (прим. 'сказать,подсказать'). Поэтому перед добавлением его в add_keywords_from_list необходимо его привести к типу list
                        milticheck_keyword_processor.add_keywords_from_list(milticheck.split(','))
                        # Обнаружение набора слов в транскрибации
                        milticheck_found_words = set(milticheck_keyword_processor.extract_keywords(transcribation))
                        # Тернарный оператор. Если обнаружились слова из набора milticheck
                        # multiwords_counter увеличивается на единицу вне зависимости от того сколько слов из milticheck обнаружилось в транскрибации
                        multiwords_counter += 1 if len(milticheck_found_words) else multiwords_counter
                        # Добавление найденных пересечений в multiwords_list
                        multiwords_list.append(list(milticheck_found_words))
                
                total_corpus_check_values = len(corpus_check) 
                # Подсчет количества обнаруженных отдельных слов корпуса проверки
                total_sep_words_intersection = len(sep_found_words)  
                # Проверка количества общего пересечения с учетом найденных milticheck наборов
                total_corpus_intersections_count = total_sep_words_intersection + multiwords_counter
                # Сбор всех найденных пересечений по словам
                total_corpus_intersections_words = list(sep_found_words) + multiwords_list
                # Весь результат проверки транскрибации по корпусу собирается в один список и затем добавляется в corpus_check_list
                corpus_check_list.append([[total_corpus_intersections_count, total_corpus_check_values], total_corpus_intersections_words])
                if total_corpus_intersections_count >= threshhold_dict[corpus]:
                    metric.append(0)
                else: metric.append(1)
            # Добавление результата проверки транскрибаций по корпусу в transcribations_df
            tr_df[corpus] = corpus_check_list   
            tr_df[f'{corpus}_metric'] = metric 
            metrics_list.append(f'{corpus}_metric')
            
        tr_df['total'] = tr_df[metrics_list].sum(axis=1)
        tr_df.to_csv(file_path, index=False)
        print(tr_df.columns())
            
    except Exception as e:
        # Производится проверка наличия переменной в namespace
        key_word_checker_variables = ['corpus', 'transcribation', 'milticheck']
        key_word_checker_variables = {variable : str(globals()[variable]) for variable in  \
                                    key_word_checker_variables if variable in locals()}
        logging.error(f'Error at the step key_word_checker - {e} - iter - {key_word_checker_variables}')

def upload_to_db(file_path : str, table_name : str, driver : str):
    """"
    Выгрузка в БД
    """
    tr_df = pd.read_csv(file_path)
    params = urllib.parse.quote_plus(driver)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    tr_df['SYS_MOMENT'] = datetime.now()
    print(tr_df.head(5))
    try:
        tr_df.to_sql(table_name, engine, chunksize=200, if_exists='append', index=False)

    except Exception as e:
        logging.critical(f'Error at the step of upload_to_db(upload_to_db, __connection_to_db) - {e}')


# ------------ Определение DAG ------------ #


default_args = {'owner': 'Samigullin_Ildus',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(seconds=120)
                }

with DAG(dag_id="Transcripation_check",
        tags = ['STT'],
        description = "Оценка качества разговора диспетчера с лидом." ,
        schedule_interval='0 4,13 * * *', 
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args) as dag:

    query_to_db = PythonOperator(
                    task_id='query_to_db',
                    python_callable=query_to_db,
                    op_kwargs={
                        'sql_query': sql_query,
                        'columns': columns,
                        'file_path' : file_path,
                        'driver' : driver})
    
    data_volume = ShortCircuitOperator(
                    task_id="data_volume",
                    provide_context=True,
                    python_callable=data_volume)

    extract_data = PythonOperator(
                    task_id='extract_data',
                    python_callable=extract_data,
                    op_kwargs={'file_path' : file_path})

    virtualenv_task = PythonVirtualenvOperator(
                    task_id="key_word_checker",
                    python_callable=key_word_checker,
                    requirements=(lambda: open("/opt/airflow/requirements/tr_check_req/tr_check_requirements.txt").readlines())(),
                    system_site_packages=True,
                    provide_context=True,
                    op_kwargs={'file_path' : file_path,
                               'key_words_df_path' : f'{Variable.get("temp_files")}/STT/Key_words.xlsx',
                               'threshhold_dict' : threshhold_dict})

    upload_to_db = PythonOperator(
                    task_id='upload_to_db',
                    python_callable=upload_to_db,
                    op_kwargs={
                        'file_path' : file_path,
                        'table_name' : 'STT_check_metric_test2',
                        'driver' : driver})

    query_to_db >> data_volume >> extract_data >> virtualenv_task >> upload_to_db