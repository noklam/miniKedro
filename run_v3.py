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

    from minikedro.v3 import ConfigLoader, DataCatalog

    config_loader = ConfigLoader("src/minikedro/v3/config.yml")
    data_catalog = DataCatalog(config_loader.data)

    companies = data_catalog.load("companies")
    reviews = data_catalog.load("reviews")
    shuttles = data_catalog.load("shuttles")

    logger.info("Running preprocess_companies")
    processed_companies = preprocess_companies(companies)
    logger.info("Running preprocess_shuttles")
    processed_shuttles = preprocess_shuttles(shuttles)
    logger.info("Running create_model_input_table")
    model_input_table = create_model_input_table(
        processed_shuttles, processed_companies, reviews
    )
