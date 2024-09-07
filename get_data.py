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
from sqlalchemy import text
from data_formatter import (
    create_and_populate_all_programs_table,
    create_and_populate_company_tables,
    create_and_populate_dependency_table,  # Import the new function
)

# make sure to create this view in the database
# CREATE VIEW program_company_value AS
# SELECT
#     ap.id AS program_id,
#     pc.company_id,
#     ap.total_funding_m / ap.num_companies AS program_value
# FROM
#     all_programs ap
# JOIN
#     program_company pc ON ap.id = pc.program_id;

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

def create_views(engine):
    try:
        with engine.connect() as conn:
            # Create the 'program_company_yearly_value' view
            conn.execute(text("""
                CREATE OR REPLACE VIEW program_company_yearly_value AS
                SELECT
                    pc.company_id,
                    ap.id AS program_id,
                    make_date(gs.year, 1, 1) AS year,
                    (ap.total_funding_m / NULLIF(ap.num_companies, 0)) / 
                    (EXTRACT(YEAR FROM ap.end_year) - EXTRACT(YEAR FROM ap.start_year) + 1) AS yearly_value
                FROM
                    all_programs ap
                JOIN
                    program_company pc ON ap.id = pc.program_id
                JOIN
                    company c ON pc.company_id = c.id
                JOIN
                    GENERATE_SERIES(
                        EXTRACT(YEAR FROM ap.start_year)::INT,
                        EXTRACT(YEAR FROM ap.end_year)::INT
                    ) AS gs(year) 
                ON gs.year IS NOT NULL
                WHERE
                    ap.total_funding_m IS NOT NULL AND 
                    ap.num_companies IS NOT NULL AND 
                    ap.num_companies > 0 AND 
                    ap.start_year IS NOT NULL AND 
                    ap.end_year IS NOT NULL;
            """))
            logging.info("View 'program_company_yearly_value' created successfully.")
            
            # Create the 'program_company_value' view
            conn.execute(text("""
                CREATE OR REPLACE VIEW program_company_value AS
                SELECT
                    ap.id AS program_id,
                    pc.company_id,
                    ap.total_funding_m / ap.num_companies AS program_value
                FROM
                    all_programs ap
                JOIN
                    program_company pc ON ap.id = pc.program_id;
            """))
            logging.info("View 'program_company_value' created successfully.")
    
    except Exception as e:
        logging.error(f"An error occurred while creating views: {e}")
        raise e

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

    logging.info("Creating views...")
    create_views(engine)
    
    logging.info("Process completed successfully.")
