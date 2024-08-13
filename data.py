import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(filename='etl_project_log.txt', level=logging.INFO)

def log_message(message):
    logging.info(f"{datetime.now()}: {message}")

# Step 1: Data Extraction
def extract_gdp_data(url):
    log_message("Starting data extraction.")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract the table
    table = soup.find('table', {'class': 'wikitable'})
    rows = table.find_all('tr')
    
    data = []
    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) >= 3:  # Ensure there are at least 3 columns
            country = cols[1].text.strip()
            gdp = cols[2].text.strip().replace(",", "").replace("†", "")
            
            # Check if GDP value is numeric
            try:
                if 'million' in gdp.lower():
                    gdp_value = float(gdp.split()[0]) / 1000  # Convert Million USD to Billion USD
                else:
                    gdp_value = float(gdp.split()[0])
            except ValueError:
                log_message(f"Skipping row with non-numeric GDP value: {gdp} for country {country}")
                continue
            
            data.append((country, gdp_value))
        else:
            log_message(f"Skipping row with unexpected format: {row}")
    
    log_message("Data extraction completed.")
    return data

# Step 2: Data Transformation (Handled in extraction)

# Step 3: Data Loading
def load_data_to_csv(data, filename):
    log_message("Starting to load data into CSV.")
    df = pd.DataFrame(data, columns=['Country', 'GDP_USD_Billion'])
    df.to_csv(filename, index=False)
    log_message("Data loaded into CSV successfully.")

def load_data_to_db(data, db_filename):
    log_message("Starting to load data into database.")
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Countries_by_GDP (
            Country TEXT,
            GDP_USD_Billion REAL
        )
    ''')
    cursor.executemany('INSERT INTO Countries_by_GDP (Country, GDP_USD_Billion) VALUES (?, ?)', data)
    conn.commit()
    log_message("Data loaded into database successfully.")
    return conn

# Step 4: Run Query
def run_query(conn):
    log_message("Running query to find countries with GDP > 100 Billion USD.")
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Countries_by_GDP WHERE GDP_USD_Billion > 100')
    result = cursor.fetchall()
    log_message(f"Query completed. {len(result)} countries found.")
    return result

# Main execution
if __name__ == '__main__':
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    
    # Extract the data
    data = extract_gdp_data(url)
    
    # Load the data
    load_data_to_csv(data, 'Countries_by_GDP.csv')
    conn = load_data_to_db(data, 'World_Economies.db')
    
    # Run the query
    result = run_query(conn)
    for row in result:
        print(row)
    
    # Close the database connection
    conn.close()
    log_message("ETL process completed successfully.")
