# GDP Data ETL Pipeline

An ETL (Extract, Transform, Load) pipeline that processes GDP data from Wikipedia, transforms the values from millions to billions USD, and stores the results in both CSV and database formats.

## Objectives

1. **Extract**: Write a data extraction function to retrieve GDP information from the Wikipedia URL
2. **Transform**: Convert GDP information from 'Million USD' to 'Billion USD' 
3. **Load**: Store the transformed data in both CSV file and SQLite database formats

## Data Source

- **URL**: https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29
- **Source**: Wikipedia - List of countries by GDP (nominal)

## Output Files

- **CSV**: `Countries_by_GDP.csv` - GDP data in CSV format
- **Database**: `World_Economies.db` - SQLite database with `Countries_by_GDP` table

## Data Schema

| Column | Description |
|--------|-------------|
| Country | Country name |
| GDP_USD_billion | GDP value in billions USD (converted from millions) |

## Functions

- `extract(url, table_attribs)` - Extracts GDP data from Wikipedia
- `transform(df)` - Converts GDP from millions to billions USD
- `load_to_csv(df, csv_path)` - Saves data to CSV file
- `load_to_db(df, sql_connection, table_name)` - Saves data to SQLite database
- `run_query(query_statement, sql_connection)` - Executes SQL queries
- `log_progress(message)` - Logs execution progress

## Setup

1. Clone the repository
2. Create virtual environment: `python -m venv venv`
3. Activate virtual environment: `source venv/bin/activate` (Unix) or `venv\Scripts\activate` (Windows)
4. Install dependencies: `pip install -r requirements.txt`

## Usage

Run the ETL pipeline:

```bash
python etl_gdp.py
```

## Dependencies

- pandas - Data manipulation and analysis
- requests - HTTP library for web scraping
- beautifulsoup4 - HTML parsing
- numpy - Numerical computing
- sqlite3 - Database operations (built-in)