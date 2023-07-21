import os 

from datetime import datetime
import pandas as pd


class DataPrep:
    
    def __init__(self,
                 n_semester: int):

        '''
        :param n_semester: scoring semester number
        '''
        self.n_semester = n_semester
        
    # Берем данные за все семестры включая, семестр скоринга
    def _get_semester_df(self,
                         df: pd.DataFrame)-> pd.DataFrame:
        """
        Returns dataframe for train 
        :param df: raw dataframe
        return: pd.DataFrame 
        """
        try:
            df_sem_n = \
            (
            df
            [
                (df['EDU_FORM'] == 'Очная')&
                (df['ENROLL_DATE'] >pd.Timestamp(2018,1,1,0))&
                (df['ENROLL_SEMESTR'] == 1)&
                (df[f'STATUS_SEM_{self.n_semester -1}'] == 'Студент')&
                (
                    (df[f'STATUS_SEM_{self.n_semester}'] == 'Студент')|
                    (df[f'STATUS_SEM_{self.n_semester}'] == 'Отчисленный')|
                    (df[f'STATUS_SEM_{self.n_semester}'] == 'Академический отпуск')|
                    (df[f'STATUS_SEM_{self.n_semester}'] == 'Условно переведенный')
                )
            ]
            )
        except Exception as e:
            pass

        return df_sem_n
    

    
    # Генерация фичей из основного датафрейма для train и inference
    def make_features(self,
                      df: pd.DataFrame) -> pd.DataFrame:
             
        try:
            df.fillna(0,inplace = True)

            # Переводим др в возраст в днях
            now = datetime.now()
            df['BIRTHDAY'] = df['BIRTHDAY'].apply(lambda x: (now - x).days)

            #-----------Агрегация посещений-----------#

            # Номер последнего закрытого семестра (семестр таргета -1)
            sem = self.n_semester - 1

            # Все колонки посещений разбитые по причинам
            cols_R1 =  set(df.filter(regex = f'_R1_').columns)
            cols_R2 =  set(df.filter(regex = f'_R2_').columns)
            cols_R3 =  set(df.filter(regex = f'_R3_').columns)
            cols_R4 =  set(df.filter(regex = f'_R4_').columns)
            cols_R5=  set(df.filter(regex = f'_R5_').columns)
            cols_T = set(df.filter(regex = f'_TOTAL_').columns)

            # Колонки посещений разбитые по причинам последний семестр
            cols_R1_last_sem = set(df.filter(regex = f'S{sem}_R1_').columns)
            cols_R2_last_sem = set(df.filter(regex = f'S{sem}_R2_').columns)
            cols_R3_last_sem = set(df.filter(regex = f'S{sem}_R3_').columns)
            cols_R4_last_sem = set(df.filter(regex = f'S{sem}_R4_').columns)
            cols_R5_last_sem = set(df.filter(regex = f'S{sem}_R5_').columns)
            cols_T_last_sem = set(df.filter(regex = f'S{sem}_TOTAL_').columns)

            # Колонки посещений разбитые по причинам кроме последнего семестра
            cols_R1_except_last = list(cols_R1 - cols_R1_last_sem)
            cols_R2_except_last = list(cols_R2 - cols_R2_last_sem)
            cols_R3_except_last = list(cols_R3 - cols_R3_last_sem)
            cols_R4_except_last = list(cols_R4 - cols_R4_last_sem)
            cols_R5_except_last = list(cols_R5 - cols_R5_last_sem)
            cols_T_except_last =  list(cols_T - cols_T_last_sem)


            # Колонки посещений разбитые по причинам последний семестр
            cols_R1_last_sem = list(cols_R1_last_sem)
            cols_R2_last_sem = list(cols_R2_last_sem)
            cols_R3_last_sem = list(cols_R3_last_sem)
            cols_R4_last_sem = list(cols_R4_last_sem)
            cols_R5_last_sem = list(cols_R5_last_sem)
            cols_T_last_sem = list(cols_T_last_sem)

        except Exception as e:
            pass
        #---------Агрегация посещений за предыдущие семестры---------#
        try:
            # Общее число пар                       
            df[f'TOTAL_except_last'] = df[cols_T_except_last].sum(axis= 1)
            # Доля пропущенных                       
            df[f'ABS_rate_except_last'] = \
            (
                df
                [
                    cols_R1_except_last+
                    cols_R2_except_last+
                    cols_R3_except_last+
                    cols_R5_except_last
                ].sum(axis = 1)/df[f'TOTAL_except_last']
            )
            # Доля посещенных
            df[f'PRES_rate_except_last'] = df[cols_R4_except_last].sum(axis = 1)/df[f'TOTAL_except_last']

            #---------Агрегация посещений за последний семестр---------#
            # Общее число пар за последний семестр
            df[f'TOTAL_last_sem'] = df[cols_T_last_sem].sum(axis = 1)
            # Доля пропущенных за последний семестр
            df[f'ABS_rate_last_sem'] = \
            (
                df
                [
                    cols_R1_last_sem+
                    cols_R2_last_sem+
                    cols_R3_last_sem+
                    cols_R5_last_sem
                ].sum(axis = 1)/df[f'TOTAL_last_sem']
            )            
            # Доля посещенных за последний семестр                       
            df[f'PRES_rate_last_sem'] = df[cols_R4_last_sem].sum(axis = 1)/df[f'TOTAL_last_sem']

            # Динамика изменений посещаемости
            df['ABS_dynamic'] =  df[f'ABS_rate_last_sem']/df[f'ABS_rate_except_last'] - 1
            df['PRES_dynamic'] = df[f'PRES_rate_last_sem']/df[f'PRES_rate_except_last'] - 1

            return df
        
        except Exception as e:
            pass

        
    
    def set_target(self,                                                                            
                   df: pd.DataFrame) -> pd.DataFrame:
        
        """Set target according to semester result
        :param df: prepared pandas DataFrame

        return: same dataframe with binary target
        """
        
        # Задаем таргет
        df['target'] = \
        (
            pd
            .Series(df[f'STATUS_SEM_{self.n_semester}'] == 'Студент')
            .map({True: 0, False: 1})
        )

        return df
