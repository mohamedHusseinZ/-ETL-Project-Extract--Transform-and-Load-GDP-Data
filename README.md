# -ETL-Project-Extract--Transform-and-Load-GDP-Data

ETL Project: Extract, Transform, and Load GDP Data
Overview
This project demonstrates a complete ETL (Extract, Transform, Load) pipeline. The script extracts GDP data for countries from a Wikipedia page, transforms the data to make it suitable for analysis, and loads it into both a CSV file and an SQLite database. Finally, a query is run to find countries with a GDP greater than 100 billion USD.

Features
Data Extraction: Scrapes a table of countries and their GDP (nominal) from a Wikipedia page using requests and BeautifulSoup.
Data Transformation: Cleans and converts GDP values from strings to numeric values, handling different formats (e.g., million, billion).
Data Loading:
Exports data to a CSV file.
Inserts data into an SQLite database.
Querying: Performs a SQL query to find countries with a GDP greater than 100 billion USD.
Requirements
Python 3.x
Required packages:
requests
beautifulsoup4
pandas
sqlite3 (comes with Python)
logging (comes with Python)
To install the required packages, run:

bash
Copy code
pip install requests beautifulsoup4 pandas
How to Run
Clone or download the script.

Run the script:
The script will:

Fetch GDP data from the specified Wikipedia page.
Transform and clean the data.
Save the data to both a CSV file and an SQLite database.
Execute a query to retrieve countries with a GDP greater than 100 billion USD.
Example command to run the script:

bash
Copy code
python etl_gdp.py
Output:

A CSV file named Countries_by_GDP.csv will be generated in the current directory.
An SQLite database named World_Economies.db will be created, containing a table Countries_by_GDP.
The results of the SQL query will be printed in the console.
Project Structure
etl_gdp.py: The main script that handles the ETL process.
etl_project_log.txt: A log file that records the ETL steps, including any errors or skipped rows.
Logging
The script uses Python's logging module to log the following:

Start and end of data extraction.
Rows that are skipped due to non-numeric GDP values.
Data loading progress into CSV and the database.
Results of the query and overall process completion.
Logs are saved in etl_project_log.txt for traceability and debugging.

Files Generated
Countries_by_GDP.csv: A CSV file containing countries and their GDP values in billions of USD.
World_Economies.db: An SQLite database containing a Countries_by_GDP table with the same data.
etl_project_log.txt: A log file with timestamps for each operation.
License
This project is licensed under the MIT License.
