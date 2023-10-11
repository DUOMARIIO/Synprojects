import pandas as pd

from ..features import DataPrep, LogsPrep, marks_prepare, merge_data
from ..models import UmapTransformer
from ..util import Loader


def prepare_train_data(n_semester: int,
                       main_df: pd.DataFrame,
                       marks_df: pd.DataFrame,
                       logs_df: pd.DataFrame,
                       path: str) -> pd.DataFrame:
    """
    Предобработка данных для трейна и сохранение umap transformer в path
    :param main_df: main dataframe loaded from db with socdem data/ attends/payments e.t.c
    :param marks_df: marks dataframe loaded from db
    :param logs_df: logs dataframe loaded from db
    :param n_semester: scoring semester
    :param path: path to save umap transformer
    return: prepared pandas dataframe to train model
    """    
    #------------------PREPARE DATA------------------#
    # Processing raw data and preparing for model train
    print('data prep | start')
    df_processor = DataPrep(n_semester=n_semester)
    # Prepared main df
    new_main_df =\
        (
            df_processor.set_target(df=df_processor
                                    .make_features(df=df_processor
                                                   .get_semester_df(df=main_df)))
        )
    print(f'data prep | new_main_df {new_main_df.shape}')
    # Prepared marks table
    marks_df = marks_prepare(marks_df=marks_df,
                             n_semester=n_semester)
    print(f'data prep | marks_df {marks_df.shape}')
    #------------------PREPARE LOGS------------------#
    logs_processor = LogsPrep(logs_df=logs_df,
                              n_semester=n_semester)
    # Prepared logs table
    logs_df = logs_processor.logs_prepare()
    print(f'data prep | logs_df {logs_df.shape}')
    # Tuple with list of logs columns names
    logs_column_names = logs_processor.get_column_names(logs_df=logs_df)
    # Create umap transformer object
    umap_transformer = UmapTransformer(path= f"{path}/umap/umap_{n_semester}.pkl")
    # Fit umap transformer and save umap and save umap model
    umap_logs = umap_transformer.fit_transform(df= logs_df[logs_column_names[0]+
                                                           logs_column_names[1]])
    # Concat raw logs data with umaped logs
    full_logs_df = pd.concat([logs_df, umap_logs], axis=1)
    print(f'data prep | umap_logs {umap_logs.shape}')

    # Merging train dataframe
    train_df = merge_data(df=new_main_df,
                          logs_df=full_logs_df,
                          marks_df=marks_df)
    print(f'data prep | train_df {train_df.shape}')

    return train_df


def prepare_inference_data(main_df: pd.DataFrame,
                           marks_df: pd.DataFrame,
                           logs_df: pd.DataFrame,
                           n_semester: int,
                           path: str
                           ) -> pd.DataFrame:
    """
    Предобработка данных для inference     
    :param main_df: main dataframe loaded from db with soc-dem data/ attends / payments e.t.c
    :param marks_df: marks dataframe loaded from db
    :param logs_df: logs dataframe loaded from db
    :param n_semester: scoring semester
    :param path: path to load umap transformer
    return: prepared pandas dataframe to predict inference
    """


    #------------------PREPARE DATA------------------#
    # Processing raw data and preparing for model train
    df_processor = DataPrep(n_semester=n_semester)

    # Prepared main df
    main_df =\
        (
            df_processor.make_features(df = df_processor
                                       .get_semester_df(df = main_df))
        )
    # Prepared marks table
    marks_df = marks_prepare(marks_df= marks_df,
                             n_semester= n_semester)
    #------------------PREPARE LOGS------------------#
    logs_processor = LogsPrep(logs_df=logs_df,
                              n_semester=n_semester)
    # Prepared logs table
    logs_df = logs_processor.logs_prepare()
    # Tuple with list of logs columns names
    logs_column_names = logs_processor.get_column_names(logs_df=logs_df)

    # Loading umap model and transform inference logs data
    umap_logs = \
        (
            UmapTransformer(path=None)
            .transform(model = Loader().load_model(path= f"{path}/umap/umap_{n_semester}.pkl"),
                       df=logs_df[logs_column_names[0] +
                                  logs_column_names[1]])
        )

    full_logs_df = pd.concat([logs_df, umap_logs], axis=1)
    # Merging final df for inference
    inference_df = merge_data(df=main_df,
                              logs_df=full_logs_df,
                              marks_df=marks_df)
    
    return inference_df
    