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

    from minikedro.v6 import ConfigLoader, DataCatalog, pipeline, node

    config_loader = ConfigLoader("src/minikedro/v6/config.yml")
    data_catalog = DataCatalog(config_loader.data)

    nodes = pipeline([
    node(**{
            "func": preprocess_companies,
            "inputs": "companies",
            "outputs": "preprocessed_companies",
        }
    ),
    node(**{
            "func": preprocess_shuttles,
            "inputs": "shuttles",
            "outputs": "preprocessed_shuttles",
        }
    ),
    node(**{
            "func": create_model_input_table,
            "inputs": ["preprocessed_shuttles", "preprocessed_companies", "reviews"],
            "outputs": "model_input_table",
        }
    )
    ])

    for node_ in nodes:
        func = node_["func"]
        logger.info(f"Running {func.__name__}")
        inputs = node_["inputs"]
        if isinstance(inputs, str):
            inputs = [inputs]  # Make it iterable for convenience
        inputs = [data_catalog.load(input_) for input_ in inputs]
        outputs = func(*inputs)
        data_catalog.save(outputs, node_["outputs"])

    # logger.info("Running preprocess_companies")
    # processed_companies = preprocess_companies(data_catalog.load("companies"))
    # data_catalog.save(processed_companies, "preprocessed_companies")

    # logger.info("Running shuttles")
    # processed_shuttles = preprocess_shuttles(data_catalog.load("shuttles"))
    # data_catalog.save(processed_companies, "preprocessed_shuttles")

    # logger.info("Running create_model_input_table")
    # model_input_table = create_model_input_table(
    #     processed_shuttles, processed_companies, data_catalog.load("reviews")
    # )
    # data_catalog.save(processed_companies, "model_input_table")
