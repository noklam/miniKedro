from collections import UserDict
from omegaconf import OmegaConf


class ConfigLoader(UserDict):
    def __init__(self, filepath):
        config = OmegaConf.load(filepath)
        self.data = OmegaConf.create(config)


if __name__ == "__main__":
    config_loader = ConfigLoader("src/minikedro/v2/config.yml")
    print(config_loader["companies"]["filepath"])
