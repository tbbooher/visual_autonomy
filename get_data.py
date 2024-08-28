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

def split_dependencies_and_unroll(df):
    try:
        df['Dependency'] = df['Dependency'].str.split(', ')
        df = df.explode('Dependency')
        
        df['Total Funding (m)'] = pd.to_numeric(df['Total Funding (m)'], errors='coerce')
        
        df['Start Year'] = pd.to_datetime(df['Start Year'], format='%Y', errors='coerce')
        df['End Year'] = pd.to_datetime(df['End Year'], format='%Y', errors='coerce')
        
        df = add_before_after_states(df)
        
        return df
    except Exception as e:
        logging.error(f"An error occurred while processing data: {e}")
        return df

def add_before_after_states(df):
    df['Before State'] = df['Program Name']
    df['After State'] = df['Dependency'].fillna('End')  # Use 'End' if no dependency exists
    return df

def generate_sankey_output(df):
    try:
        output_lines = []

        for _, row in df.iterrows():
            before_state = row['Before State']
            after_state = row['After State']
            funding = row['Total Funding (m)']
            org = row['Org']
            theme = row['Theme']

            # Skip rows without proper values
            if not before_state or not after_state or pd.isna(funding):
                continue

            # Format the line for the sankey diagram, including additional context
            line = f"{before_state} [{int(funding)}] {after_state} // Org: {org}, Theme: {theme}"
            output_lines.append(line)
        
        return "\n".join(output_lines)
    except Exception as e:
        logging.error(f"An error occurred while generating Sankey output: {e}")
        return ""

if __name__ == "__main__":
    logging.info("Loading data from Google Sheets...")
    data_df = load_data_from_google_sheet()
    if data_df is None:
        logging.error("Failed to load data from Google Sheets. Exiting.")
        exit(1)
    
    logging.info("Splitting dependencies and unrolling data...")
    processed_df = split_dependencies_and_unroll(data_df)
    
    logging.info("Generating Sankey diagram output...")
    sankey_output = generate_sankey_output(processed_df)
    
    # Output the Sankey diagram data to a text file
    with open("sankey_diagram.txt", "w") as file:
        file.write(sankey_output)
    
    logging.info("Sankey diagram data has been written to sankey_diagram.txt")
