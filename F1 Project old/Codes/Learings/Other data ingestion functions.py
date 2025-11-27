import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import os
from dotenv import load_dotenv
import requests

# --- Step 0: Load environment variables for secure credentials ---
# This looks for a .env file in the same directory as this script
# and loads the key-value pairs as environment variables.
load_dotenv()

# --- Database Connection Details (Retrieved from .env) ---
# These will be None if not found in .env, handled in functions.
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# --- Helper Function for Database Connection ---
def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine for PostgreSQL using .env credentials.
    Returns:
        sqlalchemy.engine.Engine or None: The database engine if successful, None otherwise.
    """
    if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
        print("Error: One or more database credentials (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD) are missing from .env.")
        print("Please ensure your .env file is in the same directory and contains all required entries.")
        return None

    # *** THIS IS THE KEY CHANGE ***
    # Use SQLAlchemy's URL object to construct the connection string safely
    # This handles special characters in passwords better than f-strings.
    db_url_object = URL.create(
        drivername="postgresql",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )

    try:
        # Pass the URL object to create_engine
        engine = create_engine(db_url_object)
        # Attempt to connect to test the engine immediately
        with engine.connect() as connection:
            print(f"Successfully connected to PostgreSQL database: {DB_NAME}")
        return engine
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        print(f"Attempted connection to: postgresql://{DB_USER}:*****@{DB_HOST}:{DB_PORT}/{DB_NAME}") # Still print for context
        return None
def get_data_from_postgresql(table_name, query=None):
    """
    Fetches data from a PostgreSQL table into a pandas DataFrame.
    Args:
        table_name (str): The name of the table to fetch if no custom query is provided.
        query (str, optional): A custom SQL query to execute. If None, selects all from table_name.
    Returns:
        pd.DataFrame: The fetched data, or an empty DataFrame on error.
    """
    engine = get_db_engine()
    if engine is None:
        return pd.DataFrame()

    try:
        if query:
            print(f"Executing custom query on '{table_name}': {query}")
            df = pd.read_sql(query, engine)
        else:
            print(f"Fetching all data from table: {table_name}")
            df = pd.read_sql(f"SELECT * FROM {table_name};", engine)

        print(f"Data from '{table_name}' loaded successfully. Shape: {df.shape}")
        return df

    except Exception as e:
        print(f"Error fetching data from table '{table_name}': {e}")
        print("Possible reasons: Table does not exist, query error, or permissions issue.")
        return pd.DataFrame()
    finally:
        if engine:
            engine.dispose() # Ensure connection is closed after use
def get_data_from_csv(file_path):
    """
    Reads data from a CSV file into a pandas DataFrame.
    Args:
        file_path (str): The path to the CSV file.
    Returns:
        pd.DataFrame: The data from the CSV file, or an empty DataFrame on error.
    """
    try:
        print(f"\nAttempting to read CSV file: {file_path}")
        df = pd.read_csv(file_path)
        print(f"CSV '{file_path}' loaded successfully. Shape: {df.shape}")
        return df
    except FileNotFoundError:
        print(f"Error: CSV file not found at {file_path}. Please ensure the file exists and the path is correct.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading CSV file {file_path}: {e}")
        return pd.DataFrame()
def get_data_from_excel(file_path, sheet_name=0):
    """
    Reads data from an Excel file into a pandas DataFrame.
    Args:
        file_path (str): The path to the Excel file.
        sheet_name (str or int, optional): The name or index of the sheet to read. Defaults to the first sheet (0).
    Returns:
        pd.DataFrame: The data from the Excel file, or an empty DataFrame on error.
    """
    try:
        print(f"\nAttempting to read Excel file: {file_path} (Sheet: {sheet_name})")
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        print(f"Excel '{file_path}' loaded successfully. Shape: {df.shape}")
        return df
    except FileNotFoundError:
        print(f"Error: Excel file not found at {file_path}. Please ensure the file exists and the path is correct.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading Excel file {file_path}: {e}")
        return pd.DataFrame()
def get_data_from_ergast_api(endpoint):
    """
    Fetches data from the Ergast F1 API and attempts to convert it to a DataFrame.
    Note: Ergast API responses are nested and may require further flattening.
    Args:
        endpoint (str): The specific API endpoint (e.g., 'current/last/results.json').
    Returns:
        pd.DataFrame: The fetched data, or an empty DataFrame on error.
    """
    base_url = "http://ergast.com/api/f1/"
    api_url = f"{base_url}{endpoint}"

    try:
        print(f"\nAttempting to fetch data from Ergast API: {api_url}")
        response = requests.get(api_url)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()

        # This part requires knowledge of Ergast API's nested JSON structure.
        # This example specifically extracts results from the last race.
        if 'MRData' in data and 'RaceTable' in data['MRData'] and 'Races' in data['MRData']['RaceTable']:
            races = data['MRData']['RaceTable']['Races']
            if races and 'Results' in races[0]:
                df = pd.DataFrame(races[0]['Results'])
                print(f"Data from Ergast API loaded successfully. Shape: {df.shape}")
                return df
            else:
                print("No 'Results' found in the API response or unexpected structure for races endpoint.")
                return pd.DataFrame()
        else:
            print("Unexpected API response structure for MRData or RaceTable.")
            # Uncomment the next line to inspect the full JSON response:
            # import json; print(json.dumps(data, indent=2))
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Ergast API: {e}")
        return pd.DataFrame()
    except KeyError as e:
        print(f"Error parsing JSON structure from Ergast API: Missing key {e}. Check API endpoint and expected structure.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred while fetching from API: {e}")
        return pd.DataFrame()
    
# Main function
if __name__ == '__main__':
    df_constructors = get_data_from_postgresql(table_name='constructors')
    df_circuits_csv = get_data_from_csv('data/circuits.csv')
    df_drivers_excel = get_data_from_excel('data/drivers_data.xlsx', sheet_name='Sheet1')
    df_api_last_race_results = get_data_from_ergast_api('current/last/results.json')
