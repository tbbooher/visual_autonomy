# get_data.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
import logging
from data_formatter import split_dependencies_and_unroll, create_and_populate_all_programs_table, create_and_populate_company_tables
from process_sankey import create_and_populate_sankey_data_table  # Import from process_sankey.py

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
    logging.info(f"SPREADSHEET_ID: {SPREADSHEET_ID}")
    logging.info(f"SHEET_NAME: {SHEET_NAME}")    
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME).execute()
        values = result.get('values', [])
        
        if not values:
            logging.error("No data found in the Google Sheet.")
            return None
        
        expected_num_columns = len(values[0])
        cleaned_values = []
        for row in values[1:]:
            if len(row) < expected_num_columns:
                row.extend([''] * (expected_num_columns - len(row)))  # Fill missing values
            elif len(row) > expected_num_columns:
                row = row[:expected_num_columns]  # Truncate extra values
            cleaned_values.append(row)
        
        df = pd.DataFrame(cleaned_values, columns=values[0])  # Use first row as column names
        return df

    except Exception as e:
        logging.error(f"An error occurred while loading data from Google Sheets: {e}")
        return None

if __name__ == "__main__":
    logging.info("Loading data from Google Sheets...")
    data_df = load_data_from_google_sheet()
    if data_df is None:
        logging.error("Failed to load data from Google Sheets. Exiting.")
        exit(1)
    
    logging.info("Splitting dependencies and unrolling data...")
    processed_df = split_dependencies_and_unroll(data_df)
    
    engine = create_engine(DATABASE_URL)
    
    logging.info("Creating and populating all_programs table...")
    create_and_populate_all_programs_table(processed_df, engine)
    
    logging.info("Creating and populating company tables...")
    create_and_populate_company_tables(processed_df, engine)
    
    logging.info("Creating and populating sankey_data table...")
    create_and_populate_sankey_data_table(processed_df, engine)
    
    logging.info("Process completed successfully.")
