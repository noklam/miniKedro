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

    from minikedro.v5 import ConfigLoader, DataCatalog
    config_loader = ConfigLoader("src/minikedro/v5/config.yml")
    data_catalog = DataCatalog(config_loader.data)

    steps = [
        {
            "func": preprocess_companies,
            "inputs": "companies",
            "outputs": "preprocessed_companies",
        },
        {
            "func": preprocess_shuttles,
            "inputs": "shuttles",
            "outputs": "preprocessed_shuttles",
        },
        {
            "func": create_model_input_table,
            "inputs": ["preprocessed_shuttles", "preprocessed_companies", "reviews"],
            "outputs": "model_input_table",
        },
    ]

    for step in steps:
        func = step["func"]
        logger.info(f"Running {func.__name__}")
        inputs = step["inputs"]
        if isinstance(inputs, str):
            inputs = [inputs]  # Make it iterable for convenience
        inputs = [data_catalog.load(input_) for input_ in inputs]
        outputs = func(*inputs)
        data_catalog.save(outputs, step["outputs"])
