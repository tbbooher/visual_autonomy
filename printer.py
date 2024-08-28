# filename: run_print_levels.py

import os
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
import logging
from data_formatter import split_dependencies_and_unroll
from print_levels import build_graph, print_program_levels  # Import functions from print_levels.py

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Google Sheets connection settings
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
SHEET_NAME = os.getenv('SHEET_NAME')

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
    
    # Create a mapping from program IDs to program names
    id_to_name = pd.Series(processed_df['Program Name'].values, index=processed_df['ID']).to_dict()
    
    # Build the graph and print program levels
    G = build_graph(processed_df, id_to_name)
    print_program_levels(G)
    
    logging.info("Program levels printed successfully.")
