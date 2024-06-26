from collections import UserDict
from omegaconf import OmegaConf
import importlib


class ConfigLoader(UserDict):
    def __init__(self, filepath):
        config = OmegaConf.load(filepath)
        self.data = OmegaConf.create(config)


class DataCatalog:
    def __init__(self, config_catalog: dict):
        self.datasets = {}
        for dataset_name, config in config_catalog.items():
            if isinstance(dataset_name, str) and dataset_name.startswith("_"):
                continue  # skip template value
            module = config.pop("type")  # pandas.CSVDataset

            # CSVDataset(**config) in code
            mod, dataset = module.split(".")  # pandas, CSVDataset
            mod = importlib.import_module(
                f"kedro_datasets.{mod}"
            )  # kedro_datasets.pandas (module)
            class_ = getattr(mod, dataset)  # kedro_datasets.pandas.CSVDataset
            self.datasets[dataset_name] = class_(**config)

    def load(self, dataset_name):
        return self.datasets[dataset_name].load()  # CSVDataset.load()
