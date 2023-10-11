import os
import pickle
from pathlib import Path


class Loader:


    def save_model(self,
                   model: object,
                   path: str)-> None:
        """
        save model to directory
        :param model: model to save
        :param path: path to save model
        return: None
        """

        pickle.dump(model, open(path, 'wb')) # сериализация и сохранение модели
        return None
        
        

    def load_model(self,
                   path: str)-> None:
        """
        load model from directory
        :param path: path to load model from
        """
        model = pickle.load(open(path, 'rb'))
        
        return model