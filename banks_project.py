import requests
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
import json

def log_progress(message: str) -> str:
    """
    Logs a message with a timestamp to a file named 'code_log.txt'.

    Parameters
    ----------
    message : str
        The message to be logged.

    Notes
    -----
    The timestamp format used is 'Year-Monthname-Day-Hour-Minute-Second'.
    The log file 'code_log.txt' is opened in append mode, so new messages are added to the end of the file.
    """
    timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt','a') as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url: str, table_attributes: list[str]) -> pd.DataFrame:
    """
    Extracts data from a table in a webpage and returns it as a pandas DataFrame.

    Parameters
    ----------
    url : str
        The URL of the webpage containing the table.
    table_attributes : list of str
        The column names for the DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the extracted data.

    Notes
    -----
    The function sends a GET request to the specified URL, parses the HTML content using BeautifulSoup,
    and extracts data from the first table (`<tbody>`) found in the page. It assumes that the table rows
    (`<tr>`) contain data cells (`<td>`) and that the second cell contains a link (`<a>`) with the bank name.
    The extracted data is concatenated into a DataFrame with the specified column names.
    """
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns = table_attributes)
    table = data.find_all('tbody')
    rows = table[0].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            bank_name = col[1].find_all('a')
            if bank_name is not None:
                data_dict = {'Name': bank_name[1].contents[0],
                             'MC_USD_Billion': col[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    
    return df


def transform(df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """
    Transforms the given DataFrame by converting market capitalization values from USD to GBP, EUR, and INR.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing market capitalization in USD billions.
    csv_path : str
        Path to the CSV file containing currency exchange rates.

    Returns
    -------
    pandas.DataFrame
        DataFrame with additional columns for market capitalization in GBP, EUR, and INR billions.
    """
    #Extract currency exchange rates from csv to dict
    currency_exchange = pd.read_csv(csv_path)
    currency_exchange_dict = currency_exchange.set_index('Currency').to_dict()['Rate']
    
    #USD billion format
    usd_billion = df['MC_USD_Billion'].tolist()
    usd_billion = [float(''.join((x.split('\n')[0]).split(','))) for x in usd_billion]
    df['MC_USD_Billion'] = usd_billion
    
    #GBP billion format
    df['MC_GBP_Billion'] = [np.round(x*currency_exchange_dict['GBP'],2) for x in df['MC_USD_Billion']]
    
    #EUR billion format
    df['MC_EUR_Billion'] = [np.round(x*currency_exchange_dict['EUR'],2) for x in df['MC_USD_Billion']]
    
    #INR billion format
    df['MC_INR_Billion'] = [np.round(x*currency_exchange_dict['INR'],2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Saves the given DataFrame to a CSV file at the specified output path.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to be saved to a CSV file.
    output_path : str
        Path where the CSV file will be saved.

    Returns
    -------
    None
    """
    df.to_csv(output_path)


def load_to_db(df: pd.DataFrame, sql_connection: sqlite3.connect, table_name: str) -> None:
    """
    Loads the given DataFrame into a SQL database table.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to be loaded into the SQL database.
    sql_connection : sqlalchemy.engine.Engine
        SQLAlchemy engine object for the database connection.
    table_name : str
        Name of the table where the DataFrame will be loaded.

    Returns
    -------
    None
    """
    df.to_sql(table_name, sql_connection, if_exists='append', index=False)


def run_query(query_statement: str, sql_connection: sqlite3.connect) -> None:
    """
    Executes a SQL query and prints the query statement and its output.

    Parameters
    ----------
    query_statement : str
        The SQL query to be executed.
    sql_connection : Any
        The SQL connection object to use for executing the query.

    Returns
    -------
    None
        This function does not return any value. It prints the query statement and its output.
    """
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

if __name__ == '__main__':

    with open('env_variables.json','r') as file:
        data = json.load(file)

    url = data['url']
    table_attributes = data['table_attributes']
    csv_path = data['csv_path']
    output_path = data['output_path']
    db_name = data['db_name']
    table_name = data['table_name']

    log_progress('Preliminaries complete. Initiating ETL process')

    df = extract(url, table_attributes)
    print(df)

    log_progress('Data extraction complete. Initiating Transformation process')

    df = transform(df, csv_path)
    print(df)

    log_progress('Data transformation complete. Initiating loading process')

    load_to_csv(df, output_path)

    log_progress('Data saved to CSV file')

    sql_connection = sqlite3.connect(db_name)

    log_progress('SQL Connection initiated.')

    load_to_db(df, sql_connection, table_name)

    log_progress('Data loaded to Database as table. Running the query')

    query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"

    run_query(query_statement, sql_connection)

    log_progress('Process Complete.')

    sql_connection.close()