from util import Loader

import pandas as pd
import umap


model_cols = None

class UmapTransformer:

    def __init__(self, 
                 path: str,
                 n_neighbor: int = 5,
                 n_component: int = 4):
        """
        :param path: path to save/load model or column transformer
        :param n_neighbor: number of neighbors in umap algorithm
        :param n_component: number of components- columns in result df

        :param model_loader: loads  and saves models
        """
        self.path = path
        self.n_neighbor = n_neighbor
        self.n_component = n_component
        self.model_loader = Loader()

        # self.umap_transformer = umap.UMAP(n_neighbors= n_neighbor,
        #                      n_components = n_component,
        #                      random_state=42) 
    
    def _umap_transformer(self)-> umap.UMAP:
        return umap.UMAP(n_neighbors=self.n_neighbor,
                         n_components = self.n_component,
                         random_state= 42)
    

    def fit(self,
            df: pd.DataFrame)-> umap.UMAP:
        """
        fits umap transformer 

        :param df: dataframe to umap train
        return: trained umap object
        """
        ump = self._umap_transformer()
        ump.fit(df)

        return ump

        
    def transform(self,
                  model: object,
                  df: pd.DataFrame)-> pd.DataFrame:
        """
        loads umap transformer and transfroms daataframe
        """
        transformed_df = model.transform(df)

        transformed_df = pd.DataFrame(transformed_df)
        
        # Переименовываем колонки из int в str формат
        umap_cols = transformed_df.columns.tolist() # Список названий колонок в int формате
        umap_cols = [str(i) for i in umap_cols] # Этот же список переводим в str
        transformed_df.columns = umap_cols # Перезаписываем имена колонок
        
        # Обнуляем индексы
        transformed_df.reset_index(drop=True,
                                   inplace=True)

        return transformed_df   

    def fit_transform(self,
                      df: pd.DataFrame,
                      save_flag:bool = True) -> pd.DataFrame:
        """
        :param df: dataframe to  umap fit and transform
        :param save_flag: flag to save model | defalt True
        """
        ump = self.fit(df= df)
        if save_flag:
            self.model_loader.save_model(model=ump,
                                         path=self.path)
        transformed_df = self.transform(model=ump,
                                        df=df)

        return(transformed_df)
    