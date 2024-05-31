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

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[RichHandler()],
    )
    logger = logging.getLogger("minikedro")

    config = {
        "companies": {
            "filepath": "${_base_folder}/01_raw/companies.csv",
            "type": "pandas.CSVDataset",
        },
        "reviews": {
            "filepath": "${_base_folder}/01_raw/reviews.csv",
            "type": "pandas.CSVDataset",
        },
        "shuttles": {
            "filepath": "${_base_folder}/01_raw/shuttles.xlsx",
            "type": "pandas.ExcelDataset",
        },
        "_base_folder": "data",
        "preprocessed_companies": {
            "type": "pandas.ParquetDataset",
            "filepath": "${_base_folder}/02_intermediate/preprocessed_companies.pq",
        },
        "preprocessed_shuttles": {
            "type": "pandas.ParquetDataset",
            "filepath": "${_base_folder}/02_intermediate/preprocessed_shuttles.pq",
        },
        "model_input_table": {
            "type": "pandas.ParquetDataset",
            "filepath": "${_base_folder}/03_primary/model_input_table.pq",
        },
    }
    from minikedro.v4 import ConfigLoader, DataCatalog

    config_loader = ConfigLoader(config)
    data_catalog = DataCatalog(config_loader.data)

    # companies = data_catalog.load("companies")
    # shuttles = data_catalog.load("shuttles")
    # reviews = data_catalog.load("reviews")

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
