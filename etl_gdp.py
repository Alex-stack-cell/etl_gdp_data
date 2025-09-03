import pandas as pd
from pandas import DataFrame
import requests
from bs4 import BeautifulSoup
import sqlite3
import numpy
from datetime import datetime

csv_file = 'Countries_by_GDP.csv'
db_table = 'Countries_by_GDP'
db_file = 'World_Economies.db'

countries_gdp_attributes = ['Country', 'GDP_USD_billion']

url = "'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'"

''' This function extracts the required
information from the website and saves it to a dataframe. The
function returns the dataframe for further processing. '''
def extract(url: str, table_attribs: [str]) -> DataFrame:
    pass

''' This function converts the GDP information from Currency
format to float value, transforms the information of GDP from
USD (Millions) to USD (Billions) rounding to 2 decimal places.
The function returns the transformed dataframe.'''
def transform(df: DataFrame) -> DataFrame:
    pass
''' This function saves the final dataframe as a `CSV` file 
in the provided path. Function returns nothing.'''
def load_to_csv(df: DataFrame, csv_path:str) -> None:
    pass
''' This function saves the final dataframe as a database table
with the provided name. Function returns nothing.'''
def load_to_db(df: DataFrame, sql_connection:str, table_name:str) -> None:
    pass
''' This function runs the stated query on the database table and
prints the output on the terminal. Function returns nothing. '''
def run_query(query_statement: str, sql_connection:str) ->None:
    pass
''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
def log_progress(message: str) -> None:
    pass