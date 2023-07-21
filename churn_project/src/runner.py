from data import DataLoader
from data_processing import DataProcessor
from model import Model
from util import Loader


def train_model()-> None:
    # Creating model loader
    model_loader = Loader()

    # Creating data processor to load and transform data
    data_processor = DataProcessor(user=None,
                                   password=None,
                                   server=None,
                                   database=None,
                                   env_type=None)
    
    train_df = data_processor.prepare_train_data(main_table=None,
                                                 main_schema=None,
                                                 marks_table=None,
                                                 marks_schema=None,
                                                 logs_table=None,
                                                 logs_schema=None,
                                                 n_semester=None,
                                                 path=None)
    
    # Training ml model
    ml_model = Model().train(df= train_df)

    # Saving ml model
    model_loader.save_model(model= ml_model,
                            path= None)
    # deleting data from memory
    del ml_model, train_df
    
    return None

def make_prediction():

    model_loader = Loader()
    data_loader = DataLoader(user=None,
                             password=None,
                             server=None,
                             database=None,
                             env_type=None)

    data_processor = DataProcessor(user=None,
                                   password=None,
                                   server=None,
                                   database=None,
                                   env_type=None)
    
    inference_df = data_processor.prepare_inference_data(main_table=None,
                                                         main_schema=None,
                                                         marks_table=None,
                                                         marks_schema=None,
                                                         logs_table=None,
                                                         logs_schema=None,
                                                         n_semester=None,
                                                         path=None)
    loaded_model = model_loader.load_model(path= None)

    inference_df = Model(model= loaded_model).predict(df=None)
    data_loader.load_data(df= inference_df,
                          table_name=None)

    return None
