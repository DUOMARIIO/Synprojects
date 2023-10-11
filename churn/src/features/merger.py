import pandas as pd


def merge_data(df: pd.DataFrame,
               logs_df: pd.DataFrame,
               marks_df: pd.DataFrame) -> pd.DataFrame:
    
    """Merges main, marks, logs dataframes
    :param df: dataframe with student features ex (gender/age/abs-pres part e.t.c)
    :param logs_df: dataframe with logs
    :param marks_df: dataframe with marks
    return: prepared merged dataframe
    """
    print(f'merger | start merge data')
    result_df = df.merge(logs_df, on='ABIT_UID', how='left')
    print(f'merger | merged main + logs {result_df.shape}')
    result_df = result_df.merge(marks_df, left_on='ABIT_UID', right_on='abit_uid', how='left')
    print(f'merger | merged main + logs + marks {result_df.shape}')
    print('merger finish')
    return result_df
