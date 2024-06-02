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
        "companies": {"filepath": "data/01_raw/companies.csv"},
        "reviews": {"filepath": "data/01_raw/reviews.csv"},
        "shuttles": {"filepath": "data/01_raw/shuttles.xlsx"},
    }

    companies = pd.read_csv(config["companies"]["filepath"])
    reviews = pd.read_csv(config["reviews"]["filepath"])
    shuttles = pd.read_excel(config["shuttles"]["filepath"])

    logger.info("Running preprocess_companies")
    processed_companies = preprocess_companies(companies)
    logger.info("Running preprocess_shuttles")
    processed_shuttles = preprocess_shuttles(shuttles)
    logger.info("Running create_model_input_table")
    model_input_table = create_model_input_table(
        processed_shuttles, processed_companies, reviews
    )
