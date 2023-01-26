class Getdata:
    '''
    Getdata - пандасовский датасет (обучающая выборка из базы MS SQL) для LAMA модели скоринга лидов поступающих с лендера на вероятность СПАМА
    В качестве self атрибутов принимает названия таблиц в базе 
    Список методов:
                    ● getdata() - использует self атрибуты экземпляра класса (названия таблиц в базе)
    '''
    def __init__(self, table_1, table_2): # Названия исходных таблиц для формирования обучающей выборки
        self.table_1 = table_1
        self.table_2 = table_2
    #----------------------------------------- 
    def getdata(self): # Метод использует названия таблиц из базы для формирования пандас dataframe
        data_p1 = pd.DataFrame()
        data_p2 = pd.DataFrame()
        
        with pymssql.connect(host = 'MSK1-ADB01', database='Analytic') as conn: # Коннект к базе
            data_p1 = pd.read_sql_query("SELECT * FROM {} with (nolock)".format(self.table_1), conn) # Формирование датасета
        with pymssql.connect(host='MSK1-ADB01', database='Analytic', charset="cp1251") as conn: # Коннект к базе
            data_p2 = pd.read_sql_query("SELECT * FROM {} with (nolock)".format(self.table_2), conn) # Формирование датасета
        
        data_full = data_p1.merge(data_p2, left_on = 'id_lead', right_on = 'id_lead') #Объединение датафреймов в один
        
        return(data_full)
###########################
###########################
###########################
class PrepdataT:
    '''
    Класс выделяет нужные для обучения (основные) департаменты. генерирует фичи на основе  исходных данных. Размечает таргет.
    В качестве self атрибутов принимает сформированный датафрейм, временные границы для обучающей выборки. Принимает на вход исходный датафрейм, возвращает готовую 
    обучающую выборку.
    '''
    
    def __init__(self, df, first_date, last_date):
        self.df = df
        self.first_date = first_date
        self.last_date = last_date
    #----------------------------------------- 
    #выделю нужные департаменты для обучения
    need_department = \
    (
        'Школа Бизнеса Синергия'
        ,'Synergy Business School Dubai '
        ,'Департамент регионального бизнеса'
        ,'Департамент международного бизнеса'
        ,'Коммерческий департамент'
        ,'Коммерческий департамент 2'
        ,'МАП'
    )

    #-----------------------------------------  
    def phone_check(self, st): #1 создание из поля с телефоном фичи есть ли такой номер телефона через библиотеку
        if st is None or st=='':
            return False
        else:
            s = ''.join([str(elem) for elem in st if elem.isdigit()])
            if len(s)==11 and s[0]=='8':
                s = '+7' + s[1:]
            else:
                s = '+' + s[:]
            try:
                my_number = phonenumbers.parse(s)
                return phonenumbers.is_valid_number(my_number)
            except:
                return False 
    #-----------------------------------------    
    def title_check(self, st): #2 проверяем через доп.библиоетку о том что такое имя или фамилия существуют
        if st is None or st == '':
            return False
        else:
            
            for s in st.strip().split():
                if any(names.search(s).values()):
                    return True
            return False
    #-----------------------------------------                       
    def get_digit_unique_numbers(self, st): #3 уникальное количество цифр в номере/или любом другом объекте
        if st is None or st=='':
            return 0
        else:
            return len(set([x for x in st if x.isdigit()]))/len(st)
    #-----------------------------------------            
    def get_title_length(self, st):  #4 длина имени (все символы удаляя пробелы)
        if st is None or st=='':
            return 0
        else:
            s = st.replace(' ', '')
            return len(s)
    #-----------------------------------------             
    def get_alpha_UPPER_letters(self, st): #5 доля  заглавных букв в имени
        if st is None or st=='':
            return 0
        else:
            return sum(map(str.isupper,st))/len(st)
    #-----------------------------------------        
    def get_alpha_unique_letters(self, st): #6 уникальное количество букв в имени
        if st is None or st=='':
            return 0
        else:
            return len(set([x for x in st if x.isalpha()]))/len(st)
    #-----------------------------------------        
    def get_digit_length(self, st): #7 длина только цифр в номере
        if st is None or st=='':
            return 0
        else:
            s = ''.join([str(elem) for elem in st if elem.isdigit()])
            return len(s)        
    #-----------------------------------------
    def prepdataT(self): #8 создание новых фичей в датафрейме (применение функций проверки данных 1-7)
        
        self.df['phone_check'] = self.df['phone'].apply(self.phone_check)
        self.df['title_check'] = self.df['title'].apply(self.title_check)
        self.df['PHONE_digit_unique_share'] = self.df['phone'].apply(self.get_digit_unique_numbers)
        self.df['title_length']  = self.df['title'].apply(self.get_title_length)       
        self.df['title_alpha_upper_share'] = self.df['title'].apply(self.get_alpha_UPPER_letters)    
        self.df['title_alpha_unique_share'] = self.df['title'].apply(self.get_alpha_unique_letters)        
        self.df['PHONE_length'] = self.df['phone'].apply(self.get_digit_length)
        #-----------------------------------------

        work_data =  \
        (
            self.df
            .query('resp_depart_lv2 in @self.need_department')
        )
        #-----------------------------------------

        work_data['SPAM_flag'] = \
        (
            pd
            .Series(work_data['lead_status'] =='Спам')
            .map({False: 0, True: 1})
        )
        #-----------------------------------------

        train_df = \
        (
            work_data[(work_data['datetime_leadcreated'] >= self.first_date) 
                      &(work_data['datetime_leadcreated'] < self.last_date)]
        )
        #-----------------------------------------
        return(train_df)
###########################
'''
Класс обучения модели. В качестве self атрибутов принимает обучающую выборку
Список методов:
                ● train() - обучает LAMA бустинг модель на top_14_features 
                            сериализует модель, сохраняет ее в указанную директорию 
                            возвращает директорию   
'''
###########################
class Train:
    
    
    def __init__(self, train_df, pkl_filename):
        self.train_df = train_df
        self.pkl_filename = pkl_filename

    #-----------------------------------------      
    top_14_features = \
    [
        'phone_check',# проверка валидности номера
        'title_check',# проверка валидности имени
        'sourceCode', # название источника
        'flag_MERGE_LEAD',# Пустой мержлид показывает что клиент не был на сайте и на него не сформирован ID сессии
        'Region_IP',# название региона
        'PHONE_digit_unique_share',# количество уникальных цифр в номере
        'Country_IP',# название страны
        'email_domain',# домен почты 
        'title_length',# длина имени
        'flag_campaign',# 
        'title_alpha_upper_share',# доля заглавных букв в имени
        'title_alpha_unique_share',# доля уникальных букв в имени
        'hour',# час оставления лида
        'PHONE_length'# длина номера
    ]
    #-----------------------------------------       
    #--Создание модели
    def train(self): # конфигурация модели
        task = Task('binary', metric = roc_auc_score)
        roles = {'target': 'SPAM_flag'}
        automl = TabularAutoML(task = task, 
                               timeout = 600, # 1800 seconds = 30 minutes
                               cpu_limit = -1, # Optimal for Kaggle kernels
                               general_params = {'use_algos': [['catboost','lgb', 'lgb_tuned']], 'seed':42}
                              )
        automl.fit_predict(self.train_df[self.top_14_features + ['SPAM_flag']], roles = roles)
        
        
        # pkl_filename = r'C:\Users\DDubai\Desktop\spam_model\BB_model.pkl' 
        pickle.dump(automl, open(self.pkl_filename, 'wb')) # сериализация и сохранение модели
        return(self.pkl_filename)