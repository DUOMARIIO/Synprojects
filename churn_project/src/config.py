from pathlib import Path
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    log_file: str = Field(env="LOG_FILE")
    #-----------DB SETTINGS-----------#
    user: str = Field(env="USER")
    password: str = Field(env="PASSWORD")
    server: str = Field(env="DB_SERVER")
    database: str = Field(env="DATABASE")
    env_type: str = Field(env="ENV_TYPE")
    #-----------DATA SETTINGS-----------#
    main_table:str = Field(env="MAIN_TABLE")
    main_schema:str = Field(env="MAIN_SCHEMA")
    marks_table:str = Field(env="MARKS_TABLE")
    marks_schema:str = Field(env="MARKS_SCHEMA")
    logs_table:str = Field(env="LOGS_TABLE")
    logs_schema:str = Field(env="LOGS_SCHEMA")
    n_semester:str = Field(env="N_SEMESTER")
    scoring_table_name:str = Field(env="SCORING_TABLE_NAME")
    #-----------MODELS SETTINGS-----------#
    model_path:str = Field(env="MODEL_PATH")
    umap_path:str = filter(env="UMAP_PATH")

    
    
    class Config():
        env_file = f"{Path(__file__).parent.resolve().parents[0]}/.env"
        env_file_encoding='utf-8'
