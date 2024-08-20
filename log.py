import logging
from datetime import datetime

# Setup logging
logging.basicConfig(filename='etl_project_log.txt', level=logging.INFO)

def log_progress(stage, message):
    """
    Logs the progress of the ETL process with the current stage and a message.

    Parameters:
    stage (str): The current stage of the ETL process (e.g., 'EXTRACTION', 'TRANSFORMATION', 'LOADING', 'QUERY').
    message (str): A descriptive message of the current progress.
    """
    logging.info(f"{datetime.now()} - {stage}: {message}")

# Example usage in your ETL process:
def extract_gdp_data(url):
    log_progress("EXTRACTION", "Starting data extraction.")
    # (rest of the code)

def load_data_to_csv(data, filename):
    log_progress("LOADING", "Starting to load data into CSV.")
    # (rest of the code)

def run_query(conn):
    log_progress("QUERY", "Running query to find countries with GDP > 100 Billion USD.")
    # (rest of the code)


def transform_data(data):
    log_message("Starting data transformation.")
    
    transformed_data = []
    for country, gdp in data:
        # Example transformation: Standardize country names to title case
        country = country.title()
        
        # Example transformation: Round GDP values to 2 decimal places
        gdp = round(gdp, 2)
        
        transformed_data.append((country, gdp))
    
    log_message("Data transformation completed.")
    return transformed_data