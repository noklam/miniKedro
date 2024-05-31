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

    companies = data_catalog.load("companies")
    reviews = data_catalog.load("reviews")
    shuttles = data_catalog.load("shuttles")

    logger.info("Running preprocess_companies")
    processed_companies = preprocess_companies(companies)
    data_catalog.save(processed_companies, "preprocessed_companies")

    logger.info("Running shuttles")
    processed_shuttles = preprocess_shuttles(shuttles)
    data_catalog.save(processed_companies, "preprocessed_shuttles")

    logger.info("Running create_model_input_table")
    model_input_table = create_model_input_table(
        processed_shuttles, processed_companies, reviews
    )
    data_catalog.save(processed_companies, "model_input_table")
