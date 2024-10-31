# ETL Process for Bank Market Capitalization Data

This Python script extracts bank market capitalization data from a given website, transforms it into multiple currencies, and loads the processed data into a CSV file and SQL database for further analysis.

## Features

- **Data Extraction**: Extracts bank names and market capitalization (USD) from a webpage.
- **Data Transformation**: Converts USD market capitalization to GBP, EUR, and INR based on the exchange rates provided in a CSV file.
- **Data Loading**: Saves the transformed data to a CSV file and a SQL database.
- **Logging**: Logs each step of the process to a file `code_log.txt` for tracking progress and debugging.

## Prerequisites

- Python 3.6+
- Required Python libraries:
  - `requests`
  - `BeautifulSoup`
  - `pandas`
  - `numpy`
  - `sqlite3`
- Install dependencies using:
  ```bash
  pip install requests beautifulsoup4 pandas numpy
  ```

## Environment Setup

Create an `env_variables.json` file with the following structure:

```json
{
    "url": "your_target_url",
    "table_attributes": ["Name", "MC_USD_Billion"],
    "csv_path": "path/to/exchange_rates.csv",
    "output_path": "path/to/output.csv",
    "db_name": "bank_data.db",
    "table_name": "Largest_banks"
}
```

- **url**: The webpage URL with the table data.
- **table_attributes**: Column names for the extracted DataFrame.
- **csv_path**: Path to the CSV file containing currency exchange rates.
- **output_path**: Path to save the output CSV file.
- **db_name**: Name of the SQLite database file.
- **table_name**: Name of the table to store data in the database.

## Usage

Run the script with:

```bash
python your_script_name.py
```

This script will:
1. Extract data from the specified webpage.
2. Transform the market capitalization into different currencies.
3. Save the results in a CSV file and SQL database.
4. Log progress to `code_log.txt`.

## .gitignore

Include the following in your `.gitignore` to avoid committing sensitive data or unnecessary files:

```plaintext
*.db
*.csv
code_log.txt
env_variables.json
__pycache__/
*.pyc
```

## License

This project is licensed under the MIT License.