class Json2frame:
    '''
    Класс для превращения вложенного джсона в пандас датафрейм. В качестве self атрибутов джсон 
    Методы:
            ● flatten_dict() - метод разворачивает вложенный json в одномерный словарь
            ● json2frame() - метод преобразует одномерный словарь в датафрейм. Возвращает датафрейм пандас
    '''
    def __init__(self, jsn):
        
        self.jsn = json.loads(jsn)

    #-----------------------------------------     
    def flatten_dict(self, d: MutableMapping, parent_key: str = '', sep: str ='_') -> MutableMapping:
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, MutableMapping):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    #----------------------------------------- 
    def json2frame(self): # Преобразует вложенный словарь в датафрейм пандас
         
        jess_dict_flatten = self.flatten_dict(self.jsn)
        
        df =  pd.DataFrame(data = [jess_dict_flatten.values()], columns = jess_dict_flatten.keys())
        return(df)

############################
'''
        Класс для препроцессинга сходного датафрейма данных (что был получен методом json2frame)
        в качестве self атрибута получает "сырой" датафрейм. Препроцессинг происходит точно так же как и на трейне
        Проверяется валидность данных лида.
'''
############################
class PrepdataP:
    
    def __init__(self, df):
        self.df = df

    top_14_features = \
        [
            'phone_check', #
            'title_check',#
            'sourceCode',#??
            'flag_MERGE_LEAD',#
            'Region_IP', # UF_CRM_1464341216
            'PHONE_digit_unique_share',
            'Country_IP', #UF_CRM_1417767787
            'email_domain',#
            'title_length',#
            'flag_campaign',#
            'title_alpha_upper_share',#
            'title_alpha_unique_share',#
            'hour',#
            'PHONE_length'#
        ]    

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
    def prepdataP(self): #8 создание новых фичей в датафрейме
        
        #создаем фичи на основе проверки 
        self.df['phone_check'] = self.df['FM_PHONE_n0_VALUE'].apply(self.phone_check)
        self.df['title_check'] = self.df['TITLE'].apply(self.title_check)
        self.df['PHONE_digit_unique_share'] = self.df['FM_PHONE_n0_VALUE'].apply(self.get_digit_unique_numbers)
        self.df['title_length']  = self.df['TITLE'].apply(self.get_title_length)       
        self.df['title_alpha_upper_share'] = self.df['TITLE'].apply(self.get_alpha_UPPER_letters)    
        self.df['title_alpha_unique_share'] = self.df['TITLE'].apply(self.get_alpha_unique_letters)        
        self.df['PHONE_length'] = self.df['FM_PHONE_n0_VALUE'].apply(self.get_digit_length)
        
        #переименовываем фичи
        try:
            self.df['sourceCode'] = self.df['UF_CRM_1417763889']
        except KeyError:
            self.df['sourceCode'] = None
        
        try:
            self.df['flag_MERGE_LEAD'] = self.df['UF_CRM_MERGE_LEAD'].apply(lambda x: 0 if x == "" else 1)
        except:
            self.df['flag_MERGE_LEAD'] = None

        try:
            self.df['Region_IP'] = self.df['UF_CRM_1464341216']
        except:
            self.df['Region_IP'] = None
        
        try:
            self.df['Country_IP'] = self.df['UF_CRM_1417767787']
        except:
            self.df['Country_IP'] = None
        
        try:
            self.df['email_domain'] = self.df['FM_EMAIL_n0_VALUE'].apply(lambda x: x.split("@")[1])
        except:
            self.df['email_domain'] = None
        

        try:
            self.df['flag_campaign'] = self.df['UF_CRM_1417763938'].apply(lambda x: 1 if x not in [np.NaN, ''] else 0)
        except:
            self.df['flag_campaign'] = None
        
        try:
             self.df['hour'] = self.df['DATE_CREATE'].apply(lambda x: int(x.split(" ")[1].split(":")[0]))
        except:
             self.df['hour'] = None
    
        #'Region_IP'= 'UF_CRM_1464341216'
        #'Country_IP' = 'UF_CRM_1417767787'
        
        return(self.df[self.top_14_features])
############################
'''
    Класс для скоринга лида lama моделькой
'''
############################
class  Predict:
    
    def __init__(self, df, pkl_filename): # В качестве атрибутов принимает датафрейм с фичами и путь до pickle файла с моделью
        self.df = df
        self.pkl_filename = pkl_filename
 
    def predict(self): # Десериализует модель и выдает скор
        

        model = pickle.load(open(self.pkl_filename, 'rb'))
        pred = model.predict(self.df)
        
        return(pred.data[:, 0])       