import pandas as pd


def marks_prepare(marks_df: pd.DataFrame,
                 n_semester: int) -> pd.DataFrame:
        
        
    # Переведем в верхний регистр ABIT_UID для последующего мерджа
    marks_df['abit_uid'] = marks_df['abit_uid'].apply(lambda x: x.upper())

    # Оценки за последний закрытый семестр
    marks_last = marks_df[marks_df['edu_sem'] == (n_semester - 1)]

    # Оценки за все семестры кроме последнего
    marks_except_last = marks_df[marks_df['edu_sem'] < float(n_semester - 1)]

    # Названия колонок с оценками
    cols = marks_df.columns[4:]

    # Словарик "средние по логам"
    agg_dict = dict.fromkeys(cols, 'mean')

    # Берем среднее по логам за все семестры кроме последнего
    marks_except_last_agg = \
    (
        marks_except_last
        .groupby(by = 'abit_uid', as_index = False)
        .agg(agg_dict)
    )

    # Мерджим агг старых и новые
    new_marks_df = \
    (
        marks_except_last_agg
        .merge(marks_last, on = 'abit_uid', how = 'left')
    )

    #Отношение логов за последний семестр к среднему за предыдущие
    new_marks_df['result_mark_dynamic'] = new_marks_df['mark_result_y']/new_marks_df['mark_result_x'] - 1
    new_marks_df['satisfact_mark_dynamic'] = new_marks_df['satisfact_mark_y']/new_marks_df['satisfact_mark_x'] - 1
    #Заполняем нулями для сокращения размерности 
    new_marks_df.fillna(0,inplace = True)

    return new_marks_df
