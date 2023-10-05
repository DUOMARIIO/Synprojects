# Airflow modules import
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

# Base modules import
import pandas as pd 
import urllib
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import requests
import re


"""
DAG для перевода статусов студентов из approved в accepted.
- Сначала берутся данные по студентам из выгрузки по студентам ЛК2035.xlsx берутся студенты в статусе «Одобрен» и номером потока – 1.
- Делается по этим студентам get запрос на уточнение статуса и если возвращаемый статус approved то студент двигается дальше
- По отобранным студентам делается post запрос на обновление статусов.
По части студентов прилетает request status 400 хотя по ним произошло изменение статуса на accepted.
- Для решения проблемы описанной выше заново делается get запрос по студентам чтобы проверить из статусы
- Результат выгружается в БД

"""

# -------------------- Определение базовых переменных -------------------- #

# Наименование DAG
dag_name = 'university_2035_zachislenie'
# Файл куда будут писаться кастомные логи 
log_pth = f'{Variable.get("custom_logs")}/un2035/{dag_name}.txt'
# Токен для post и get запросов
myToken = Variable.get("University2035_API_Token")
# id платформы
platform_id = 1358
# url для get запросов
get_url = 'https://cat.2035.university/api/v5/course/enroll/'
# url для post запросов
post_url = 'https://cat.2035.university/api/v6/course/enroll/update/'

# -------------------- Определение функций\тасков -------------------- #

def sqlserver_connection():

    """ Создание подключения """
    
    # Драйвер для подклбчения к БД
    driver = 'Driver={ODBC Driver 18 for SQL Server}' + \
                ';TrustServerCertificate=Yes'+ \
                ';Server=' + 'MSK1-BIDB01.synergy.local' + \
                ';Database=' + 'Analytic' + \
                f';UID={Variable.get("tuz_login")}' +  \
                f';PWD={Variable.get("tuz_password")}'
    params = urllib.parse.quote_plus(driver)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    return engine

def course_checker(x,y):
    """ Функция для проверки сходства course id """
    if x==y:
        return 1
    else:
        return 0


def get_request(df, get_url, task_comment, task_start_time, myToken=myToken, platform_id=platform_id, log_pth=log_pth):

    # Список для сбора данных API запроса
    request_result = []
    # Бывают ситуации когда при отправке API запроса по ID студента случается обрыв сети и вылетает ошибка
    # Эту проблему можно решить просто повторно отправив запрос по данным студентам 
    # excepted_id - собираются Id студентов по которым влетели ошибки при API запросе
    excepted_id = []

    # Передача токена для запроса
    head = {'Authorization': 'Token {}'.format(myToken)}
    # Итерирование по строкам lk_df
    for _, row in df.iterrows():
        # Формирование словаря для запроса
        params = {
                'platform_id':platform_id,              # id платформы
                'unti_id':row['Unti ID'],               # UNTI_ID студента
                'external_course_id':row['ID курса']    # ID курса студента
                }
        try:
            # Запрос 
            res = requests.get(get_url, headers=head, params=params,timeout=80)

            # Для формирования итога запроса отбираются данные:
            # - UNTI_ID студента;
            # - статус запроса (Возможные статусы https://cat.u2035test.ru/api/docs/#api-v5-course-e, см. пункт по url запроса);
            # - статус студента (Возможные статусы https://cat.u2035test.ru/api/docs/#api-v5-course-e, см. пункт по url запроса);
            # - время запроса
            request_result.append([row['Unti ID'], res.json().get('application_status',res.status_code), res.status_code, task_start_time])

        except Exception as e:
            # При ошибке запроса UNTI_ID студента записывается в excepted_id и записывается в файл с логами
            excepted_id.append(row['Unti ID'])
            with open(log_pth, 'a') as f:
                f.write(f"{task_start_time} - {task_comment}:{e} - Unti ID:{row['Unti ID']}\n")
    
    return request_result, excepted_id



def data_volume(file_pth, query_to_courses, request_result_df_pth):
    """ 
    Считываются данные и определяется объем UNTI_ID для изменения статусов и выгрузки информации по приказам.
    Изначально считываются данные из файла file_pth (ЛК2035.xlsx) и отбираются UNTI со статусом заявки 'Одобрена'.
    По данным UNTI делается GET запрос и отбираются только UNTI со статусом 'approved'
    Результат записывается во временный файл

    Данная функция реализуется в DAG через ShortCircuitOperator и на каждом этапе есть проверка объема UNTI_ID для обновления 
    статусов и выгрузки данных по приказам.
    """
    task_start_time = datetime.now()
    # Чтение исходного файла
    lk_df = pd.read_excel(file_pth)
    lk_df.rename(columns={'Идентификатор потока': 'ID потока'}, inplace=True)
    # Для выгрузки данных по приказу отбираются только одобренные студенты
    lk_df = lk_df[lk_df['Статус'] == 'Одобрена']
    # Проверка объема UNTI_ID для обновления статусов и выгрузки данных по приказам
    # Если кол-во UNTI_ID со статусом 'Одобрена' == 0 - то можно прерывать исполнение DAG
    print(lk_df.shape)
    if lk_df.shape[0] == 0:
        return False
    print()

    # # Чтение файла с данными по потокам студентов и добавление информации по потоку в lk_df
    # potok_df =  pd.read_sql_query(query_to_courses, sqlserver_connection())
    # lk_df = lk_df.merge(potok_df, how='left', left_on='Курс', right_on='Курс')

    
    #######################################
    ############## Убрал временное решение, чтбы работал интенсив. 
    
    # Временное, убрать позже
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    ##lk_df = lk_df[lk_df['Номер потока'] == 1]
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    print(lk_df.columns)
    # GET запрос на выгрузку данных по студентам
    request_result, excepted_id = get_request(df=lk_df, get_url=get_url, task_comment='data_volume_task' ,task_start_time=task_start_time)

    # Повторные запросу по UNTI_ID у которых при запросе возникла ошибка
    if len(excepted_id) > 0:
        # DataFrame с UNTI_ID у которых при запросе возникла ошибка
        retry_df = lk_df[lk_df['Unti ID'].isin(excepted_id)]
        repeat_request, _ = get_request(df=retry_df, get_url=get_url, task_comment='data_volume_task_retry' ,task_start_time=task_start_time)
        # Добавление результата пофторного запроса в request_result
        request_result += repeat_request

    # Если список из результатов зпросов не пустой, то данные обрабатываются и сохраняются в файл
    if len(request_result) > 0 :
        # Перевод результата запроса в DataFrame
        request_result_df = pd.DataFrame(request_result,columns=['untiId','get_univ_status','get_request_status','get_request_sysmoment'])
        # Отбор только тех студентов, которые в статусе "approved"
        request_result_df = request_result_df[request_result_df['get_univ_status'] == 'approved']
        # Соединение с Dataframe lk_df для получения данных по 'ID курса' и 'ID потока'
        request_result_df = request_result_df.merge(lk_df, how='left', left_on='untiId', right_on='Unti ID')
        # Формирование итогового файла и его сохранение 
        request_result_df = request_result_df[['untiId', 'get_univ_status', 'get_request_status','ID курса', 'ID потока', 'get_request_sysmoment']]
        request_result_df.to_csv(request_result_df_pth, index=False)
        return True
    else:
        return False


def post_request(request_result_df_pth, mismatch_save_pth, post_url, myToken, log_pth, platform_id, post_request_result_df_pth):
    
    """ POST запросы на плотформу Университет2035 
        Данные для запроса формируются на основе результата предыдущего TASK.
        Для отправки post запроса на изменение статуса студента на accepted требуются след. данные:
        - platform_id (по умолчанию 1358);
        - UNTI_ID (есть в DataFrame сформированном в предыдущем TASK);
        - course_id (есть в DataFrame сформированном в предыдущем TASK);
        - status (переводим в статус accepted);
        - accepted_date (дата принятия);
        - admission_order_date (дата создания приказа (accepted_date=admission_order_date));
        - admission_order_number (номер приказа).
        accepted_date, admission_order_date, admission_order_number - информация берется из БД """

    task_start_time = datetime.now()
    # Чтение DataFrame сформированный в предыдущем TASK
    data_df = pd.read_csv(request_result_df_pth)
    # Для получения данных по accepted_date, admission_order_date, admission_order_number необходимо 
    # сформировать запрос в БД по untiId студентов. Для этого отраются untiId и приводятся к виду
    # по которому можно из отправить  в запрос
    logins_list = data_df['untiId'].values.tolist()
    logins_list = ', '.join([str(i) for i in logins_list])
    # Форма запроса в БД
    query = f"""
			select
            n.*
            from (
            SELECT  NM.UNTI_ID,
                NM.CONTACT_ID,  
                NM.DEAL_ID,
                COALESCE(CC.[PRIKAZ_NUM],DEAL.UF_PRIKAZ_NUMBER) [UF_PRIKAZ_NUMBER],
                COALESCE(CONVERT(date, CC.[PRIKAZ_DATE], 105),DEAL.UF_PRIKAZ_DATE) [UF_PRIKAZ_DATE],
                CC.STATUS_LK,
                CC.COURSE_ID_LK,
                CC.COURSE_ID_AKADA,
                CC.IS_SAME_COURSE_ID 
            FROM 
                (
                select * from [Analytic].[dbo].[NM_CP_INTENSIVE_FTP_PARSED] 
                union select * from NM_CPSH_FTP_PARSED 
                ) NM -- таблица с UNTI_ID студентов
            LEFT JOIN [stage].[CRM].[b_uts_crm_deal] Deal   -- таблица с датой и номером приказа зачисления
            ON NM.[DEAL_ID] = Deal.VALUE_ID
			LEFT JOIN [Analytic].[dbo].[NM_CP_ClientCourseList] CC
            ON NM.UNTI_ID = CC.UNTI_ID
			WHERE 1=1
            AND NM.UNTI_ID IN ({logins_list})
            AND Deal.VALUE_ID IS NOT NULL                 -- только те студенты по которым есть сделки
            AND CC.STOPPER=0
            )n
            WHERE
            (n.[UF_PRIKAZ_NUMBER] LIKE '42-%' OR n.[UF_PRIKAZ_NUMBER] LIKE '42/%')         -- только те приказы которые соответсвтуют формату
            and n.UF_PRIKAZ_DATE >= '2022-09-01' --Правки по дате приказа с 1 сентября 2022
            """
    # Запрос в БД на выгрузку данных 
    query_df = pd.read_sql_query(query, sqlserver_connection())
    # Перевод query_df['UNTI_ID'] в тип int для возможности джойна с DataFrame data_df 
    query_df['UNTI_ID'] = query_df['UNTI_ID'].astype(int)
    # Соединение таблиц  query_df и data_df  
    merged_data = query_df.merge(data_df, how='left', left_on='UNTI_ID', right_on='untiId')
    # # Удаление лишних данных из DataFrame
    merged_data.drop(columns=['untiId'], inplace=True)
    # Важное условие отправление POST запроса - сходство id курсов из всех источников
    # Первый источник это данные из БД Аналитик. Запрос в переменной query содержит колонку IS_SAME_COURSE_ID
    # Это флаг соответсвия Id курса в БД
    # Второй этап проверки - сверка id курса из актулаьной выгрузки ЛК2035 и id курса из АКАДА.
    # Формирование флага равенства id курса ЛК2035 и id курса из АКАДА
    merged_data['second_course_check'] = merged_data.apply(lambda x:course_checker(x['ID курса'], x['COURSE_ID_AKADA']), axis=1)
    # Те студенты у которых курсы не сошлись отбираются в отдельный DataFrame и сохраняются в эксель файл
    course_mismatch = merged_data.loc[(merged_data['IS_SAME_COURSE_ID'] == 0) | (merged_data['second_course_check'] == 0)]
    if course_mismatch.shape[0] > 0 :
        file_save_name = str(task_start_time)
        file_save_name = file_save_name[:file_save_name.find('.')]
        file_save_name = re.sub('[^0-9a-zA-Z]+', '_', file_save_name)
        mismatch_filename = f'{mismatch_save_pth}Несовпадение_потоков_{file_save_name}.xlsx'
        course_mismatch.to_excel(mismatch_filename, index=False)
    # Для дальнейших POST запросов отбираются только те студенты у которых все couse_id сошлись
    merged_data = merged_data.loc[(merged_data['IS_SAME_COURSE_ID'] == 1) & (merged_data['second_course_check'] == 1)]
    # Обработка merged_data
    merged_data.reset_index(inplace=True, drop=True)
    merged_data.drop(columns=['STATUS_LK', 'COURSE_ID_LK', 'COURSE_ID_AKADA', 'IS_SAME_COURSE_ID', 'second_course_check'],
                    inplace=True)
    # POST запрос требует передачу данных по accepted_date, admission_order_date в типе str
    merged_data['UF_PRIKAZ_DATE'] = merged_data['UF_PRIKAZ_DATE'].astype(str)
    # Список для сбора данных POST запросов
    post_request_result = []
    # Передача токена для запроса
    head = {'Authorization': 'Token {}'.format(myToken)}
    # Итерирование по строкам merged_data для post запросов
    for _, row in merged_data.iterrows():

        try:
            # Формирование словаря для запроса        
            params = {
                    "platform_id": platform_id,         # id платформы
                    "unti_id": row['UNTI_ID'],          # UNTI_ID студента
                    "course_id": row['ID курса'],       # ID курса студента
                    "status": "accepted",               # Перевод на статус accepted
                    "flow": row['ID потока'],           # id потока
                    "accept_date" : row['UF_PRIKAZ_DATE'],  # Дата принятия студента 
                    "admission_order_date" : row['UF_PRIKAZ_DATE'],  # дата создания приказа
                    "admission_order_number" : row['UF_PRIKAZ_NUMBER']  # Данные про приказу
                    } 
            # Запрос
            res = requests.post(post_url, headers=head, data=params)
            # Для формирования итога запроса отбираются данные:
            # - UNTI_ID студента;
            # - статус запроса (Возможные статусы https://cat.u2035test.ru/api/docs/#api-v5-course-e, см. пункт по url запроса);
            # - статус студента (Возможные статусы https://cat.u2035test.ru/api/docs/#api-v5-course-e, см. пункт по url запроса);
            # - время запроса
            post_request_result.append([row['UNTI_ID'], res.json().get('application_status',res.status_code), res.status_code, task_start_time])
            
        except Exception as e:
            # При ошибке запроса UNTI_ID студента записывается в файл с логами
            with open(log_pth, 'a') as f:
                f.write(f"{task_start_time} - POST_request:{e} - Unti ID:{row['UNTI_ID']}\n")
    # Формирование DataFrame по результатам POST запросов
    post_request_result_df = pd.DataFrame(post_request_result,
                                          columns=['untiId','post_univ_status','post_request_status','post_request_sysmoment'])
    # Соединение с таблицей merged_data для получения данных их предущего TASK
    post_request_result_df = post_request_result_df.merge(merged_data, how='left', left_on='untiId', right_on='UNTI_ID')
    post_request_result_df.drop(columns=['UNTI_ID'], inplace=True)
    # Сохранение результата 
    post_request_result_df.to_csv(post_request_result_df_pth, index=False)



def status_clarification(post_request_result_df_pth, result_df_pth):
    """Уточнение статусов студентов через GET запросы"""

    task_start_time = datetime.now()

    # Чтение DataFrame c результатом работы предыдущих TASKs
    status_clarification_df = pd.read_csv(post_request_result_df_pth)
    # Для отправки GET запросов нужно переименовать колонку с  untiId
    status_clarification_df.rename(columns={'untiId' : 'Unti ID'}, inplace=True)
    # Отправка GET запросов
    request_result, excepted_id = get_request(df=status_clarification_df, get_url=get_url,
                                              task_comment='status_clarification' ,task_start_time=task_start_time)
    # Повторные запросу по UNTI_ID у которых при запросе возникла ошибка
    if len(excepted_id) > 0:
        # DataFrame с UNTI_ID у которых при запросе возникла ошибка
        retry_df = status_clarification_df[status_clarification_df['Unti ID'].isin(excepted_id)]
        repeat_request, _ = get_request(df=retry_df, get_url=get_url, task_comment='data_volume_task_retry' ,task_start_time=task_start_time)
        # Добавление результата пофторного запроса в request_result
        request_result += repeat_request
        
    # Перевод результата запроса в вид DataFrame
    request_result_df = pd.DataFrame(request_result, 
                        columns=['Unti ID', 'post_get_univ_status', 'post_get_request_status', 'post_get_request_sysmoment'])
    # Соединение с таблицей status_clarification_df       
    request_result_df = request_result_df.merge(status_clarification_df, how='left', left_on='Unti ID', right_on='Unti ID')
    # Список требуемых колонок для формирования конечного DataFrame в удобном для работы порядке
    columns = ['Unti ID',
            'ID курса', 'ID потока', 'DEAL_ID', 'CONTACT_ID', 'UF_PRIKAZ_NUMBER', 'UF_PRIKAZ_DATE',

            'get_univ_status', 'get_request_status', 'get_request_sysmoment',

            'post_univ_status', 'post_request_status', 'post_request_sysmoment',

            'post_get_univ_status', 'post_get_request_status', 'post_get_request_sysmoment']
    # Формирование итоговой таблицы
    request_result_df = request_result_df[columns]
    # Сохранение результата
    request_result_df.to_csv(result_df_pth, index=False)


def upload_to_db(result_df_pth, tablename):
    """Выгрузка результата в БД"""
    # Чтение данных
    result_df = pd.read_csv(result_df_pth)
    # Добавление колонки с временем начала выгрузки в БД
    result_df['upload_to_db_sysmoment'] = datetime.now()
    # Выгрузка в БД
    result_df.to_sql(tablename, con=sqlserver_connection(), if_exists='append', index=False)






# ----------------------- Определение DAG ----------------------- #

default_args = {'owner': 'Samigullin_Ildus',
                'depends_on_past': False,       # Запуск вне зависимости от статуса прошедших запусков
                # 'retries': 2,                   # Кол-во перезапусков
                # 'retry_delay': timedelta(seconds=120)   # Задержка между перезапусками
                }

with DAG(dag_id=dag_name,
        tags = ['CF/CP'],
        description = "Зачисление студентов (выгрузка данных по приказам, перевод в статус 'accepted')" ,
        schedule_interval='0 7,11,14 * * *',
        start_date=days_ago(1),            
        catchup=False,              # Не нагонять пропущенные запуски
        default_args=default_args) as dag:

    data_volume_and_get_request = ShortCircuitOperator(
                task_id="data_volume_estimation",
                provide_context=True,
                python_callable=data_volume,
                op_kwargs={
                        'file_pth': "/var/lib/documents/88.das/03.Общая/ЛК_2035/ЛК2035.xlsx",
                        'query_to_courses' : 'SELECT * FROM [Analytic].[dbo].[ASS_FC_DP_COURSES]',
                        'request_result_df_pth' : f'{Variable.get("temp_files")}/un2035/{dag_name}_request_res.csv',
                        })

    post_request = PythonOperator(
                task_id='post_request',
                python_callable=post_request,
                op_kwargs={
                        'request_result_df_pth': f'{Variable.get("temp_files")}/un2035/{dag_name}_request_res.csv',
                        'mismatch_save_pth' : '/var/lib/documents/88.das/03.Общая/ЛК_2035/prikaz_upload/',
                        'post_url': post_url,
                        'platform_id' : platform_id,
                        'log_pth' : log_pth,
                        'myToken' : myToken,
                        'post_request_result_df_pth' : f'{Variable.get("temp_files")}/un2035/{dag_name}_post_request_res.csv'})
    
    status_clarification = PythonOperator(
            task_id='status_clarification',
            python_callable=status_clarification,
            op_kwargs={
                    'post_request_result_df_pth': f'{Variable.get("temp_files")}/un2035/{dag_name}_post_request_res.csv',
                    'result_df_pth' : f'{Variable.get("temp_files")}/un2035/{dag_name}_get_post_request.csv'})
    
    upload_to_db = PythonOperator(
            task_id='upload_to_db',
            python_callable=upload_to_db,
            op_kwargs={
                    'result_df_pth': f'{Variable.get("temp_files")}/un2035/{dag_name}_get_post_request.csv',
                    'tablename' : 'NM_CP_EnrollRequests'})


    data_volume_and_get_request >> post_request >> status_clarification >> upload_to_db