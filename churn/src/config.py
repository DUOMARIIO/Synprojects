from pathlib import Path

import toml
from pydantic import BaseSettings


class Settings(BaseSettings):
    config_file: str = f"{Path(__file__).parent.resolve().parents[0]}/.toml"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.load_config()

    def load_config(self):
        with open(self.config_file, "r", encoding="utf-8") as conf_file:
            config_data = conf_file.read()
        self.__dict__.update(toml.loads(config_data))
