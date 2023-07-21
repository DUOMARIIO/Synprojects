from connection import mssql_engine

import pandas as pd

class DataLoader:

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

        self.dwh_conn = mssql_engine(user= user,
                                     password= password,
                                     server=  server,
                                     database= database,
                                     env_type= env_type
                                    )

    def get_data(self,
                 table_name: str,
                 schema: str)-> pd.DataFrame:
        """
        Read table from sql database
        :param table_name: name of table in database
        :param schema: schema name in database
        return: pd.DataFrame
        """
        df = None
        try:
            df = pd.read_sql_table(table_name= table_name, 
                                   con= self.dwh_conn, 
                                   schema= schema)
                                
            return df
        
        except Exception as e:
            pass

        

    def load_data(self,
                  df: pd.DataFrame,
                  table_name: str) -> str:
        '''
        Upload dataframe to database
        :param df: df with scores to upload
        :param table_name
        return: str with number of scores uploaded
        '''
        try:
            df.to_sql(table_name,
                      con = self.dwh_conn,
                      if_exists='append',
                      chunksize=200,
                      index=False)
            
            return (f'uploaded {df.shape[0]} scores')  
        
        except Exception as e:
            pass
