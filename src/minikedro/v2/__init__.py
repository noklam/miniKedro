from collections import UserDict
from omegaconf import OmegaConf


class ConfigLoader(UserDict):
    def __init__(self, data: dict):
        self.data = OmegaConf.create(data)
