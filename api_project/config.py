from pydantic import BaseSettings, PostgresDsn, validator
from typing import Optional, Any, Dict


class Settings(BaseSettings):
    BASE_ROUTE: str = '/asr_api'
    PROJECT_NAME: str = 'Synergy ASR API'

    POSTGRES_SERVER: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    SQLALCHEMY_DATABASE_URI: Optional[PostgresDsn] = None

    @validator('SQLALCHEMY_DATABASE_URI', pre=True)
    def assemble_db_connection(cls, v: Optional[str],
                               values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return PostgresDsn.build(
            scheme='postgresql',
            user=values.get('POSTGRES_USER'),
            password=values.get('POSTGRES_PASSWORD'),
            host=values.get('POSTGRES_SERVER'),
            path=f'/{values.get("POSTGRES_DB") or ""}',
        )


settings = Settings()
