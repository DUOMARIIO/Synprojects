from src import mssql_engine
from src import DataLoader
from src import Settings
from src import train_model

# cfg = Settings()
#
#
# data_loader = DataLoader(user=cfg.connection['user'],
#                 password=cfg.connection['password'],
#                 server=cfg.connection['server'],
#                 database=cfg.connection['database'],
#                 env_type=cfg.connection['env_type'])
#
# df = data_loader.get_data(table_name=cfg.database['main_table'],
#                           schema=cfg.database['main_schema'])
#
#
#
if __name__ == '__main__':
    train_model(n_semester=3)
