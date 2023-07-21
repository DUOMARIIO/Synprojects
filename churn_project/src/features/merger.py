import pandas as pd

def merge_data(df: pd.DataFrame,
               logs_df: pd.DataFrame,
               marks_df: pd.DataFrame) -> pd.DataFrame:
    
    """Merges main, marks, logs dataframes
    :param df: dataframe with student features ex (gender/age/abs-pres part e.t.c)
    :param logs_df: dataframe with logs 

    return: prepared merged dataframe
    """

    try:
        result_df = df.merge(logs_df, on = 'ABIT_UID', how = 'left')
        result_df = result_df.merge(marks_df, left_on = 'ABIT_UID', right_on='abit_uid', how = 'left')
        result_df.fillna(0, inplace = True)

        return result_df 
    except Exception as e:
        pass
        