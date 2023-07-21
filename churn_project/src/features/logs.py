import pandas as pd
from typing import List

class LogsPrep:
    
    def __init__(self,
                 logs_df: pd.DataFrame,
                 n_semester: int):
        """
        :param logs_df: logs dataframe from db
        :param n_semester: number of scoring semester
        """
        self.logs_df = logs_df
        self.n_semester = n_semester

    #------Берем логи за нужные семестры------#    
    def _logs_for_sem(self) -> pd.DataFrame:
        logs_df = self.logs_df[self.logs_df['semester_num'] <=float(self.n_semester - 1)]

        return logs_df
    
    # Генерация фичей из логов
    def logs_prepare(self) -> pd.DataFrame:
        
        #Логи за все семестры не включая текущий
        logs_df = self._logs_for_sem()
        
        #Логи за последний семестр
        logs_last = logs_df[logs_df['semester_num'] == float(self.n_semester - 1)]
        
        #Логи за все семестры кроме последнего
        logs_except_last = logs_df[logs_df['semester_num'] < float(self.n_semester - 1)]
        
        #Названия колонок с логами
        logs_cols = logs_df.columns[2:]
        
        #Словарик "средние по логам"
        agg_dict = dict.fromkeys(logs_cols, 'mean')
        
        #Берем среднее по логам за все семестры кроме последнего
        logs_except_last_agg = \
        (
            logs_except_last
            .groupby(by = 'ABIT_UID', as_index = False)
            .agg(agg_dict)
        )
        
        #Мерджим агг старых и новые
        new_logs_df = \
        (
            logs_except_last_agg
            .merge(logs_last, on = 'ABIT_UID', how = 'left')
        )
        
        #Отношение логов за последний семестр к среднему за предыдущие
        new_logs_df['logs_dynamic'] = new_logs_df['cnt_all_logs_y']/new_logs_df['cnt_all_logs_x'] - 1
        #Заполняем нулями для сокращения размерности 
        new_logs_df.fillna(0,inplace = True)
        
        return new_logs_df
    
    # Названия колонок с логами для последующего UMAP 
    def get_column_names(self,
                         logs_df: pd.DataFrame) -> tuple:
        """
        Extract column names of dataframe according to filter
        :param logs_df: dataframe with logs
        return: tuple with lists
        """
        logs_columns_except_last = logs_df.filter(regex = f'_x').columns.tolist()
        logs_columns_last_sem = logs_df.filter(regex = f'_y').columns.tolist()
        
        return (logs_columns_except_last,logs_columns_last_sem)
    