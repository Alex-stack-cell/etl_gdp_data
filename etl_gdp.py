import pandas as pd
from pandas import DataFrame
import requests
from bs4 import BeautifulSoup
import sqlite3
import numpy
from datetime import datetime
from typing import List, Dict, Any

MILLION_TO_BILLION = 0.001

csv_file = 'Countries_by_GDP.csv'
db_table = 'Countries_by_GDP'
db_file = 'World_Economies.db'

countries_gdp_attributes = ['Country', 'GDP_USD_billion']

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

conn = sqlite3.connect(db_file)

''' This function extracts the required
information from the website and saves it to a dataframe. The
function returns the dataframe for further processing. '''


def extract(url: str, table_attribs: List[str]) -> DataFrame:
    df = DataFrame(columns=table_attribs)
    try:
        response = requests.get(url).text
        html_page_data = BeautifulSoup(response, 'html.parser')
        table_data = html_page_data.find_all('table', {'class': 'wikitable'})
        table_body = table_data[0].find('tbody')
        rows = table_body.find_all('tr')

        for row in rows:
            cols = row.find_all('td')
            if (cols and  # Not an empty row
                len(cols) > 2 and  # Has at least 3 columns
                cols[0].find('a') and  # First column contains hyperlink
                    cols[2].get_text(strip=True) != '—'):  # Third column not '—'

                row_dict = {}
                row_dict['Country'] = cols[0].get_text(
                    strip=True)  # Column 0: Country
                row_dict['GDP_USD_billion'] = cols[2].get_text(
                    strip=True)  # Column 2: IMF Estimate

                df = pd.concat([df, pd.DataFrame([row_dict])],
                               ignore_index=True)

    except requests.exceptions.HTTPError as http_error:
        print(f"Http error : {http_error}")
    except requests.exceptions.ConnectionError as conn_error:
        print(f"Connection error: {conn_error}")
    except requests.exceptions.Timeout as time_out:
        print(f"Timeout error, {time_out}")
    except requests.exceptions.RequestException as request_error:
        print(f"Request exception, {request_error}")

    finally:
        return df


''' This function converts the GDP information from Currency
format to float value, transforms the information of GDP from
USD (Millions) to USD (Billions) rounding to 2 decimal places.
The function returns the transformed dataframe.'''


def transform(df: DataFrame) -> DataFrame:
    df['GDP_USD_billion'] = df['GDP_USD_billion'].apply(lambda x: round(float(x.replace(',', '')) * MILLION_TO_BILLION, 2))
    return df


''' This function saves the final dataframe as a `CSV` file 
in the provided path. Function returns nothing.'''


def load_to_csv(df: DataFrame, csv_path: str) -> None:
    df.to_csv(csv_path, index=False)


''' This function saves the final dataframe as a database table
with the provided name. Function returns nothing.'''


def load_to_db(df: DataFrame, sql_connection: str, table_name: str) -> None:
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


''' This function runs the stated query on the database table and
prints the output on the terminal. Function returns nothing. '''


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''


def log_progress(message: str) -> None:
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second     
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) # format timestamp
    with open('etl_project_log.txt', 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')

log_progress("STARTING ETL PROCESS")

log_progress("EXTRACTING DATA")
df:DataFrame = extract(url, countries_gdp_attributes)
log_progress("DATA EXTRACTED SUCCESSFULLY")

log_progress("TRANSFORMING DATA")
transform(df)
log_progress("DATA TRANSFORMED SUCCESSFULLY")

log_progress("LOADING DATA TO CSV")
load_to_csv(df, csv_file)
log_progress("DATA LOADED TO CSV SUCCESSFULLY")

log_progress("LOADING DATA TO DB")
load_to_db(df, conn, db_table)
log_progress("DATA LOADED TO DB SUCCESSFULLY")

log_progress("RUNNING QUERY")
run_query(f"SELECT * FROM {db_table} WHERE GDP_USD_billion > 100", conn)
log_progress("QUERY RUN SUCCESSFULLY")

log_progress("ETL COMPLETED SUCCESSFULLY")