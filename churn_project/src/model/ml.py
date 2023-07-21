from lightautoml.automl.presets.tabular_presets import TabularUtilizedAutoML
from lightautoml.tasks import Task
from sklearn.metrics import roc_auc_score
import pandas as pd


class Model:

    def __init__(self,
                 model: object = None):
        """
        :param model: ml model to load, default = None
        """
        self.model = model

    def train(self, 
              df: pd.DataFrame)-> object:
        
        """
        train churn model
        :param df: train dataframe
        return: trainder churn model
        """
        task = Task('binary', metric = roc_auc_score)
        roles = {'target': 'target'}
        
        automl = TabularUtilizedAutoML(task = task, 
                                       timeout = 1200, # 1800 seconds = 30 minutes
                                       cpu_limit = -1, # Optimal for Kaggle kernels
                                       general_params = {'use_algos': [[ 'lgb','cb','lgb_tuned','cb_tuned']]})

        automl.fit_predict(df)

        return automl
    
    def predict(self,
                df: pd.DataFrame)-> pd.DataFrame:
        """
        make prediction on inference dataframe
        :param df: inference dataframe
        return: inference dataframe with scores
        """
        # тут будет логика связанная с предиктом
        
        inference_predict = self.model.predict()
        df['churn_score'] = inference_predict.data[:,0]

        return df
    