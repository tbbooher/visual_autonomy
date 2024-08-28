import os
import pandas as pd
from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Google Sheets and PostgreSQL connection settings
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
SHEET_NAME = os.getenv('SHEET_NAME')
POSTGRES_USER = os.getenv('DATABASE_USER')
POSTGRES_PASSWORD = os.getenv('DATABASE_PASSWORD')
POSTGRES_DB = os.getenv('CURRENT_DB_NAME')
POSTGRES_HOST = os.getenv('DATABASE_HOST')
POSTGRES_PORT = os.getenv('LOCAL_DATABASE_PORT')
CURRENT_TABLE_NAME = os.getenv('CURRENT_TABLE_NAME')

# PostgreSQL connection string
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def load_data_from_google_sheet():
    try:
        # Authenticate and connect to Google Sheets API
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Read data from Google Sheet
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME).execute()
        values = result.get('values', [])
        
        if not values:
            logging.error("No data found in the Google Sheet.")
            return None
        
        # Ensure all rows have the correct number of columns
        expected_num_columns = len(values[0])
        cleaned_values = []
        for row in values[1:]:
            if len(row) < expected_num_columns:
                row.extend([''] * (expected_num_columns - len(row)))  # Fill missing values
            elif len(row) > expected_num_columns:
                row = row[:expected_num_columns]  # Truncate extra values
            cleaned_values.append(row)
        
        # Convert data to a pandas DataFrame
        df = pd.DataFrame(cleaned_values, columns=values[0])  # Use first row as column names
        return df

    except Exception as e:
        logging.error(f"An error occurred while loading data from Google Sheets: {e}")
        return None

def split_dependencies_and_unroll(df):
    try:
        # Split the 'Dependency' column into multiple rows
        df['Dependency'] = df['Dependency'].str.split(', ')
        df = df.explode('Dependency')
        return df
    except Exception as e:
        logging.error(f"An error occurred while splitting dependencies: {e}")
        return df  # Return the original DataFrame if something goes wrong

def load_data_into_postgres(df):
    try:
        # Create SQLAlchemy engine to connect to PostgreSQL
        engine = create_engine(DATABASE_URL)
        
        # Load DataFrame into PostgreSQL (create table if it doesn't exist, append data if it does)
        df.to_sql(CURRENT_TABLE_NAME, engine, if_exists='replace', index=False)
        logging.info("Data loaded into PostgreSQL successfully.")
    except Exception as e:
        logging.error(f"An error occurred while loading data into PostgreSQL: {e}")

if __name__ == "__main__":
    # Step 1: Load data from Google Sheets
    logging.info("Loading data from Google Sheets...")
    data_df = load_data_from_google_sheet()
    if data_df is None:
        logging.error("Failed to load data from Google Sheets. Exiting.")
        exit(1)
    
    # Step 2: Split dependencies and unroll the data
    logging.info("Splitting dependencies and unrolling data...")
    processed_df = split_dependencies_and_unroll(data_df)
    
    # Step 3: Load processed data into PostgreSQL
    logging.info("Loading data into PostgreSQL...")
    load_data_into_postgres(processed_df)
    logging.info("Process completed successfully.")
