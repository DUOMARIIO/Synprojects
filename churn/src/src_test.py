from src import DataLoader
from config import Settings

cfg = Settings()
def test_pipeline():
    data_loader = DataLoader(user=cfg.connection['user'],
                    password=cfg.connection['password'],
                    server=cfg.connection['server'],
                    database=cfg.connection['database'],
                    env_type=cfg.connection['env_type'])
    df = data_loader.get_data(table_name=cfg.database['main_table'],
                              schema=cfg.connection['main_schema'])
    return (df.head())


if __name__ == '__main__':
    print(DataLoader)
    #print(test_pipeline())
