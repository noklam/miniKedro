from collections import UserDict
from typing import Callable
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

    def save(self, data, dataset_name):
        self.datasets[dataset_name].save(data)


pipeline = list


class node(UserDict):
    def __init__(self, func, inputs, outputs, name=None):
        data = {"func": func, "inputs": inputs, "outputs": outputs, "name": name}
        super().__init__(data)


class Hook:
    def __init__(
        self,
        before_node_run: Callable | None = None,
        after_node_run: Callable | None = None,
    ):
        self.before_node_run = before_node_run
        self.after_node_run = after_node_run


class Hooks:
    def __init__(self, hooks: list[Hook]):
        self.hooks = hooks

    def before_node_run(self):
        for hook in self.hooks:
            if hook.before_node_run:
                hook.before_node_run()

    def after_node_run(self):
        for hook in self.hooks:
            if hook.after_node_run:
                hook.after_node_run()
