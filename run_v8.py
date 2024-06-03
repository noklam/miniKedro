if __name__ == "__main__":
    print("Start Pipeline")
    from minikedro.pipelines.data_processing.nodes import (
        create_model_input_table,
        preprocess_companies,
        preprocess_shuttles,
    )
    from rich.logging import RichHandler
    import logging
    import pandas as pd

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[RichHandler()],
    )
    logger = logging.getLogger("minikedro")

    from minikedro.v8 import ConfigLoader, DataCatalog, pipeline, node, Hooks, Hook

    config_loader = ConfigLoader("conf/base/catalog.yml")
    data_catalog = DataCatalog(config_loader.data)

    from minikedro.pipelines.data_processing_minikedro import create_pipeline

    nodes = create_pipeline()

    # Hook
    MLflowHook = Hook(lambda: print("MLflow: logging experiment"))
    ObservabilityHook = Hook(lambda: print("Telemetry: sending telemetry"))

    hooks = Hooks([MLflowHook, ObservabilityHook])
    for node_ in nodes:
        hooks.before_node_run()

        func = node_["func"]
        logger.info(f"Running {func.__name__}")
        inputs = node_["inputs"]
        if isinstance(inputs, str):
            inputs = [inputs]  # Make it iterable for convenience
        inputs = [data_catalog.load(input_) for input_ in inputs]
        outputs = func(*inputs)
        data_catalog.save(outputs, node_["outputs"])

        hooks.after_node_run()
