from minikedro.v3 import DataCatalog


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

    from minikedro.v4 import ConfigLoader, DataCatalog

    config_loader = ConfigLoader("src/minikedro/v4/config.yml")
    data_catalog = DataCatalog(config_loader.data)

    logger.info("Running preprocess_companies")
    processed_companies = preprocess_companies(data_catalog.load("companies"))
    data_catalog.save(processed_companies, "preprocessed_companies")

    logger.info("Running preprocess_shuttles")
    processed_shuttles = preprocess_shuttles(data_catalog.load("reviews"))
    data_catalog.save(processed_companies, "preprocessed_shuttles")

    logger.info("Running create_model_input_table")
    model_input_table = create_model_input_table(
        processed_shuttles, processed_companies, data_catalog.load("shuttles")
    )
    data_catalog.save(processed_companies, "model_input_table")
