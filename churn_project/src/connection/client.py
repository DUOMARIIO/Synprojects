from sqlalchemy import create_engine


def mssql_engine(user: str,
                 password: str,
                 server: str,
                 database: str,
                 env_type: str) -> object : 
    """ Create connection engine to MSSQL DB server
    :param user: user login
    :param password: user password
    :param server: server name
    :param database: database name
    :return sqlalchemy create_engine object
    """
    driver = f'Driver={env_type};\
                TrustServerCertificate=Yes;\
                Server={server};\
                database={database};\
                UID={user};\
                PWD={password}'
    
    return create_engine(f'mssql+pyodbc:///?odbc_connect={driver}') 
