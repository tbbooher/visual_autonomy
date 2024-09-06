# get_data.py: the script that loads data from Google Sheets and populates the database tables.
# tim booher, 2021-09-07
# license: public domain

import os
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
from db_connection import get_postgres_engine, get_google_sheet_service
import logging
from data_formatter import (
    create_and_populate_all_programs_table,
    create_and_populate_company_tables,
    create_and_populate_dependency_table,  # Import the new function
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data_from_google_sheet():  
    try:
        # creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        # service = build('sheets', 'v4', credentials=creds)
        service = get_google_sheet_service()
        SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
        SHEET_NAME = os.getenv('SHEET_NAME')

        result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME).execute()
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

        # Ensure the column name is 'id' and not 'ID'
        if 'ID' in df.columns:
            df.rename(columns={'ID': 'id'}, inplace=True)

        # Ensure the 'id' column is of integer type
        df['id'] = df['id'].astype(int)
        
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
    
    engine = get_postgres_engine()
    
    logging.info("Creating and populating all_programs table...")
    create_and_populate_all_programs_table(data_df, engine)
    
    logging.info("Creating and populating company tables...")
    create_and_populate_company_tables(data_df, engine)
    
    logging.info("Creating and populating program_dependencies table...")
    create_and_populate_dependency_table(data_df, engine)
    
    logging.info("Process completed successfully.")
