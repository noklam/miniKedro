from minikedro import DataCatalog


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
    import yaml

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[RichHandler()],
    )
    logger = logging.getLogger("minikedro")

    with open("conf/base/catalog.yml") as f:
        config = yaml.safe_load(f)
    from minikedro.v9 import ConfigLoader, DataCatalog, Hook, Hooks

    config_loader = ConfigLoader(config)
    data_catalog = DataCatalog(config_loader.data)

    from minikedro.pipelines.data_processing_minikedro import create_pipeline

    pipeline_ = create_pipeline()

    # Hook
    ObservabilitiyHook = Hook(
        lambda: print("Sending telemetry before node run"),
        lambda: print("Sending telemetry before node run"),
    )

    AnotherObservabilitiyHook = Hook(
        lambda: print("Sending telemetry to another server"),
    )

    hooks = Hooks([ObservabilitiyHook, AnotherObservabilitiyHook])

    # Kedro Runner
    for node_ in pipeline_:
        func = node_["func"]
        logger.info(f"Running {func.__name__}")

        hooks.before_node_run()
        # Core node execution logic
        inputs = node_["inputs"]
        if isinstance(inputs, str):
            inputs = [inputs]  # Make it iterable for convenience
        inputs = [data_catalog.load(input_) for input_ in inputs]
        outputs = func(*inputs)
        data_catalog.save(outputs, node_["outputs"])

        hooks.after_node_run()