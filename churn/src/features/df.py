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
    # Для того, чтобы в последствии задать таргет для обучения
    def get_semester_df(self,
                        df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns dataframe for train 
        :param df: raw dataframe
        return: pd.DataFrame 
        """

        df_semester = \
            (
                df[
                    (df['EDU_FORM'] == 'Очная')
                    &(df['ENROLL_SEMESTR'] == 1) 
                    &(df['ENROLL_DATE'] > pd.Timestamp(2018, 1, 1, 0)) 
                    &((df[f'STATUS_SEM_{self.n_semester -1}'] == 'Студент')
                     |(df[f'STATUS_SEM_{self.n_semester -1}'] == 'Условно переведенный'))
                ]
            )

        return df_semester

    # Генерация фичей из основного датафрейма для train и inference

    def make_features(self,
                      df: pd.DataFrame) -> pd.DataFrame:
        """
        prepare and generate features
        :param df: selected dataframe to prepare
        return: dataframe generated features 
        """
        try:
            # Fill attends df with 0 instead of Null to aggregate
            attends_df = df.iloc[:,74:]
            attends_df.fillna(0, inplace=True)
            df.iloc[:, 74:] = attends_df
            # Переводим др в возраст в днях
            now = datetime.now()
            df['BIRTHDAY'] = df['BIRTHDAY'].apply(lambda x: (now - x).days)

            #-----------Агрегация посещений-----------#

            # Номер последнего закрытого семестра (семестр таргета -1)
            last_sem = self.n_semester - 1
            cols_R1_except_last = []
            cols_R2_except_last = []
            cols_R3_except_last = []
            cols_R4_except_last = []
            cols_R5_except_last = []
            cols_T_except_last = []

            # if last_sem > 1:
            for sem in range(1, last_sem):
                # Все колонки посещений разбитые по причинам кроме последнего семестра
                cols_R1_except_last = cols_R1_except_last + (df.filter(regex=f'S{sem}_R2_')
                                                               .columns
                                                               .tolist()
                                                             )
                cols_R2_except_last = cols_R2_except_last + (df.filter(regex=f'S{sem}_R2_')
                                                               .columns
                                                               .tolist()
                                                             )
                cols_R3_except_last = cols_R3_except_last + (df.filter(regex=f'S{sem}_R3_')
                                                               .columns
                                                               .tolist()
                                                             )
                cols_R4_except_last = cols_R4_except_last + (df.filter(regex=f'S{sem}_R4_')
                                                               .columns
                                                               .tolist()
                                                             )
                cols_R5_except_last = cols_R5_except_last + (df.filter(regex=f'S{sem}_R5_')
                                                               .columns
                                                               .tolist()
                                                             )
                cols_T_except_last = cols_T_except_last + (df.filter(regex=f'S{sem}_TOTAL_')
                                                             .columns
                                                             .tolist()
                                                           )

            # Колонки посещений разбитые по причинам последний семестр
            cols_R1_last_sem = df.filter(regex=f'S{last_sem}_R1_').columns.tolist()
            cols_R2_last_sem = df.filter(regex=f'S{last_sem}_R2_').columns.tolist()
            cols_R3_last_sem = df.filter(regex=f'S{last_sem}_R3_').columns.tolist()
            cols_R4_last_sem = df.filter(regex=f'S{last_sem}_R4_').columns.tolist()
            cols_R5_last_sem = df.filter(regex=f'S{last_sem}_R5_').columns.tolist()
            cols_T_last_sem = df.filter(regex=f'S{last_sem}_TOTAL_').columns.tolist()

        except Exception as e:
            print(f'df {e}')
        #---------Агрегация посещений за предыдущие семестры---------#
        try:
            # Общее число пар
            df[f'TOTAL_except_last'] = df[cols_T_except_last].sum(axis=1)
            # Доля пропущенных
            df[f'ABS_rate_except_last'] = \
                (
                df
                [
                    cols_R1_except_last +
                    cols_R2_except_last +
                    cols_R3_except_last +
                    cols_R5_except_last].sum(axis=1)/df[f'TOTAL_except_last']
                )
            # Доля посещенных
            df[f'PRES_rate_except_last'] = df[cols_R4_except_last].sum(axis=1)/df[f'TOTAL_except_last']

            #---------Агрегация посещений за последний семестр---------#
            # Общее число пар за последний семестр
            df[f'TOTAL_last_sem'] = df[cols_T_last_sem].sum(axis=1)
            # Доля пропущенных за последний семестр
            df[f'ABS_rate_last_sem'] = \
                (
                df
                [
                    cols_R1_last_sem +
                    cols_R2_last_sem +
                    cols_R3_last_sem +
                    cols_R5_last_sem].sum(axis=1)/df[f'TOTAL_last_sem']
                )
            # Доля посещенных за последний семестр
            df[f'PRES_rate_last_sem'] = df[cols_R4_last_sem].sum(axis=1)/df[f'TOTAL_last_sem']

            # Динамика изменений посещаемости
            df['ABS_dynamic'] = df[f'ABS_rate_last_sem']/df[f'ABS_rate_except_last'] - 1
            df['PRES_dynamic'] = df[f'PRES_rate_last_sem']/df[f'PRES_rate_except_last'] - 1
            
            return df

        except Exception as e:
            print(f'df {e}')

    def set_target(self,
                   df: pd.DataFrame) -> pd.DataFrame:
        """Set target according to semester result
        :param df: prepared pandas DataFrame
        return: same dataframe with binary target
        """
        # Исключаем 0, которые вставили вместо NULL значений в методе make_features
        # 0 были вставлены для того, чтобы агрегировать и расчитать посещения
        df = df[df[f'STATUS_SEM_{self.n_semester}'] != 0]
        # Задаем таргет
        df['target'] = \
            (
            pd
            .Series(
                    (df[f'STATUS_SEM_{self.n_semester}'] == 'Отчисленный')|
                    (df[f'STATUS_SEM_{self.n_semester}'] == 'Академический отпуск')|
                    (df[f'STATUS_SEM_{self.n_semester}'] == 'Условно переведенный')
                   )
            .map({True: 1, False: 0})
        )
        return df
