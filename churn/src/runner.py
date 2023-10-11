from .config import Settings
from .data import DataLoader
from .data_processing import prepare_inference_data, prepare_train_data
from .models import Model
from .util import Loader


def train_model(n_semester)-> None:
    """
    Prepare train dataframe and trains churn model
    save UMAP transformer and churn model to directory
    """
    print('runner start')
    cfg = Settings()
    # Creatind data loader object to load raw data/append scores to db
    data_loader = DataLoader(user= cfg.connection['user'],
                             password= cfg.connection['password'],
                             server= cfg.connection['server'],
                             database= cfg.connection['database'],
                             env_type= cfg.connection['env_type'])
    
    # Load, prepare train data and training+saving UMAP transfrormer
    # loading main dataframe with attends and soc-dem
    main_df=data_loader.get_data(table_name=cfg.database['main_table'],
                                 schema=cfg.database['main_schema'])
    print('runner 1 | main_df created')
    # Loading marks dataframe
    marks_df=data_loader.get_data(table_name=cfg.database['marks_table'],
                                  schema=cfg.database['marks_schema'])
    print('runner 2 | marks_df created')
    # Loading logs dataframe
    logs_df=data_loader.get_data(table_name= cfg.database['logs_table'],
                                 schema= cfg.database['logs_schema'])
    print('runner 3 | logs_df created')
    # Preparing full dataframe for model train
    train_df = prepare_train_data(n_semester= n_semester,
                                  path=cfg.path,
                                  main_df=main_df,
                                  marks_df=marks_df,
                                  logs_df=logs_df)
    print('runner 4 | train_df prepared')
    print(train_df.shape)
    # Creating model loader to save churn model
    model_loader = Loader()
    print('runner 5 | model loader created')
    # Training ml model
    ml_model = \
        (
            Model()
            .fit(df= train_df
                 [
                     cfg.model_columns['cat']+
                     cfg.model_columns['num']+
                     cfg.model_columns['logs']+
                     cfg.model_columns['target']
                 ]
                 )
         )
    print('runner 6 | ml model trained')
    # Saving ml model
    model_loader.save_model(model=ml_model,
                            path=f"{cfg.path}/ml/ml_{n_semester}.pkl")
    print('runner 7 | ml model saved')
    print('runner finish')
    return None


def make_prediction(n_semester):
    cfg = Settings()
    """
    Creating data loader object to load raw data/append scores to db
    """
    data_loader = DataLoader(user= cfg.connection['user'],
                             password= cfg.connection['password'],
                             server= cfg.connection['server'],
                             database= cfg.connection['database'],
                             env_type= cfg.connection['env_type'])
    # Load inference data 
    main_df= data_loader.get_data(table_name= cfg.database['main_table'], 
                                  schema= cfg.database['main_schema'])
    # Loading marks inference dataframe
    marks_df= data_loader.get_data(table_name= cfg.database['marks_table'],
                                   schema= cfg.database['marks_schema'])
    # Loading logs inference dataframe
    logs_df= data_loader.get_data(table_name= cfg.database['logs_table'],
                                  schema= cfg.database['logs_schema'])
    # Prepare inference data
    inference_df = prepare_train_data(n_semester= n_semester,
                                      path=cfg.path,
                                      main_df=main_df,
                                      marks_df=marks_df,
                                      logs_df=logs_df)

    # Loading trained churn model
    loaded_model = Loader().load_model(path= f"{cfg.path}/ml/ml_{n_semester}.pkl")
    # Get inference df with predicted churn scores
    inference_df = \
        (
            Model(model= loaded_model)
            .predict(df=inference_df
                     [
                         cfg.model_columns['cat']+
                         cfg.model_columns['num']+
                         cfg.model_columns['logs']
                     ]
                    )
        )
    # Loading inference data with scores to db
    data_loader.load_data(df= inference_df,
                          table_name= cfg.database['scores_table'])

    return None
