from data import DataLoader
from features import DataPrep, LogsPrep, marks_prepare, merge_data, UmapTransformer
from util import Loader

import pandas as pd


class DataProcessor:

    def __init__(self, 
                 user: str,
                 password: str,
                 server: str,
                 database: str,
                 env_type: str
                ):
        """
        :param user: username or login to database access
        :param password: password to access database
        :param server: server name
        :param database: database name
        :param env_type: environment type
        """
        self.data_loader = DataLoader(user= user,
                                      password= password,
                                      server= server,
                                      database= database,
                                      env_type= env_type)
        
    def prepare_train_data(self,
                           main_table: str,
                           main_schema: str,
                           marks_table: str,
                           marks_schema: str,
                           logs_table: str,
                           logs_schema: str,
                           n_semester: int,
                           path: str) -> pd.DataFrame:
        """
        Предобработка данных для трейна и сохранение umap transformer
        :param main_table: main dataframe from db with soc-dem data/ attends / payments e.t.c
        :param main_schema: schema in db with main_table
        :param marks_table: marks dataframe from db
        :param marks_schema: schema in db with marks_table
        :param logs_table: logs dataframe from db
        :param logs_schema: dchema in db with logs_table
        :param n_semester: scoring semester
        :param path: path to save umap transformer
        """
        #------------------LOAD DATA------------------#
        # Loading raw data from Database
        # Table with main df
        main_df = self.data_loader.get_data(table_name= main_table, 
                                            schema= main_schema)
        # Table with raw marks
        marks_df = self.data_loader.get_data(table_name= marks_table,
                                             schema= marks_schema)
        # Table with raw logs
        logs_df = self.data_loader.get_data(table_name= logs_table,
                                            schema= logs_schema)
        
        #------------------PREPARE DATA------------------#
        # Processing raw data and preparing for model train
        df_processor = DataPrep(n_semester=n_semester)
        logs_processor = LogsPrep(logs_df=logs_df,
                                  n_semester=n_semester)
        # Prepared main df
        main_df =\
            (
                df_processor
                .set_target(df = df_processor
                            .make_features(df = df_processor
                                        ._get_semester_df(df = main_df)))
            )
        # Prepared marks table
        marks_df = marks_prepare(marks_df=marks_df,
                                n_semester=n_semester)
        #------------------PREPARE LOGS------------------#
        # Prepared logs table
        logs_df = logs_processor.logs_prepare()
        # Tupple with list of logs columns names
        logs_column_names = logs_processor.get_column_names(logs_df=logs_df)
        
        # Create umap transformer object
        umap_transformer = UmapTransformer(path= path)
        # Fit umap transformer and save umap and save umap model
        umap_logs = umap_transformer.fit_transform(df= logs_df[logs_column_names[0]+
                                                            logs_column_names[1]])

        # Merging train dataframe
        train_df = merge_data(df = main_df,
                            logs_df=umap_logs,
                            marks_df=marks_df)
        
        # Deleting separate df's from memory
        del main_df, logs_df, marks_df, umap_logs

        return train_df


    def prepare_inference_data(self,
                               main_table: str,
                               main_schema: str,
                               marks_table: str,
                               marks_schema: str,
                               logs_table: str,
                               logs_schema: str,
                               n_semester: int,
                               path: str) -> pd.DataFrame:

        """
        Предобработка данных для inference         
        :param main_table: main dataframe from db with soc-dem data/ attends / payments e.t.c
        :param main_schema: schema in db with main_table
        :param marks_table: marks dataframe from db
        :param marks_schema: schema in db with marks_table
        :param logs_table: logs dataframe from db
        :param logs_schema: dchema in db with logs_table
        :param n_semester: scoring semester
        :param path: path to load umap transformer
        """
        #------------------LOAD DATA------------------#
        # Loading raw data from Database
        # Table with main df
        main_df = self.data_loader.get_data(table_name= main_table, 
                                            schema= main_schema)
        # Table with raw marks
        marks_df = self.data_loader.get_data(table_name= marks_table,
                                             schema= marks_schema)
        # Table with raw logs
        logs_df = self.data_loader.get_data(table_name= logs_table,
                                            schema= logs_schema)
        
        #------------------PREPARE DATA------------------#
        # Processing raw data and preparing for model train
        df_processor = DataPrep(n_semester=n_semester)
        logs_processor = LogsPrep(logs_df=logs_df,
                                  n_semester=n_semester)
        
        # Prepared main df
        main_df =\
            (
                df_processor
                .make_features(df = df_processor
                               ._get_semester_df(df = main_df))
            )
        # Prepared marks table
        marks_df = marks_prepare(marks_df= marks_df,
                                 n_semester= n_semester)
        #------------------PREPARE LOGS------------------#
        # Prepared logs table
        logs_df = logs_processor.logs_prepare()
        # Tupple with list of logs columns names
        logs_column_names = logs_processor.get_column_names(logs_df=logs_df)

        # Loading umap model and transform inference logs data
        logs_df = \
            (
                UmapTransformer(path = None)
                .transform(model = Loader().load_model(path= path),
                           df = logs_df[logs_column_names[0]+
                                        logs_column_names[1]])
            )
        
        # Merging final df for inference
        inference_df = merge_data(df= main_df,
                                  logs_df= logs_df,
                                  marks_df= marks_df)
        # Deleting separate df's from memory
        del main_df, logs_df, marks_df, umap_logs
        
        return inference_df
    