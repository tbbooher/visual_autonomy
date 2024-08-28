# get_data.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
import logging
from data_formatter import split_dependencies_and_unroll, create_and_populate_all_programs_table, create_and_populate_company_tables

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

def build_sankey_rows(program_name, current_level, df, sankey_rows, visited, id_to_name, levels_dict):
    """Recursive function to determine levels and build source-target relationships for the Sankey diagram."""
    program_rows = df[df['Program Name'] == program_name]

    if program_rows.empty or program_name in visited:
        logging.info(f"No further dependencies for {program_name} or already visited.")
        # Mark it as an end node by filling up to level 5
        levels_dict[program_name] = [None] * (current_level - 1) + [program_name] + [None] * (5 - current_level)
        sankey_rows.append({
            'source': program_name,
            'target': program_name,
            'level': current_level,
            'value': program_rows['Total Funding (m)'].sum() if not program_rows.empty else 0,
            'theme': program_rows['Theme'].iloc[0] if not program_rows.empty else None,
            'total_funding': program_rows['Total Funding (m)'].sum() if not program_rows.empty else 0,
            'start_year': program_rows['Start Year'].min() if not program_rows.empty else None,
            'end_year': program_rows['End Year'].max() if not program_rows.empty else None,
            **{f'level_{i+1}': level_name for i, level_name in enumerate(levels_dict[program_name])}
        })
        return

    visited.add(program_name)
    levels_dict[program_name] = [None] * (current_level - 1) + [program_name] + [None] * (5 - current_level)

    for _, row in program_rows.iterrows():
        dependencies = str(row['Dependency']).split(',') if pd.notna(row['Dependency']) else []

        if not dependencies or dependencies == ['']:
            logging.info(f"No dependencies for {row['Program Name']}. Terminating at self.")
            levels_dict[row['Program Name']] = levels_dict[program_name][:current_level] + [row['Program Name']] + [None] * (5 - current_level)
            sankey_rows.append({
                'source': row['Program Name'],
                'target': row['Program Name'],
                'level': current_level,
                'value': row['Total Funding (m)'],
                'theme': row['Theme'],
                'total_funding': row['Total Funding (m)'],
                'start_year': row['Start Year'],
                'end_year': row['End Year'],
                **{f'level_{i+1}': level_name for i, level_name in enumerate(levels_dict[row['Program Name']])}
            })
        else:
            for dependency in dependencies:
                dependency = dependency.strip()  # Clean up the dependency name
                if dependency.isdigit():  # If dependency is an ID, convert to program name
                    dependency_name = id_to_name.get(dependency, None)
                    if dependency_name:
                        levels_dict[dependency_name] = levels_dict[program_name][:current_level] + [dependency_name] + [None] * (5 - current_level - 1)
                        sankey_rows.append({
                            'source': dependency_name,
                            'target': row['Program Name'],
                            'level': current_level,
                            'value': row['Total Funding (m)'],
                            'theme': row['Theme'],
                            'total_funding': row['Total Funding (m)'],
                            'start_year': row['Start Year'],
                            'end_year': row['End Year'],
                            **{f'level_{i+1}': level_name for i, level_name in enumerate(levels_dict[dependency_name])}
                        })
                        build_sankey_rows(dependency_name, current_level + 1, df, sankey_rows, visited.copy(), id_to_name, levels_dict)
                    else:
                        logging.warning(f"Dependency ID '{dependency}' not found in Program Names. Skipping.")
                else:
                    logging.warning(f"Dependency '{dependency}' is not a valid ID. Skipping.")


def create_and_populate_sankey_data_table(df, engine):
    try:
        # Drop the sankey_data table to completely replace it
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sankey_data CASCADE"))
            conn.commit()

        # Create the sankey_data table
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sankey_data (
                    source TEXT,
                    target TEXT,
                    level INT,
                    value DOUBLE PRECISION,
                    theme TEXT,
                    total_funding DOUBLE PRECISION,
                    start_year DATE,
                    end_year DATE,
                    level_1 TEXT,
                    level_2 TEXT,
                    level_3 TEXT,
                    level_4 TEXT,
                    level_5 TEXT
                )
            """))
            conn.commit()

        # Create a mapping from program IDs to program names
        id_to_name = pd.Series(df['Program Name'].values, index=df['ID']).to_dict()

        sankey_rows = []
        levels_dict = {}

        # Build the sankey rows starting with each unique program
        unique_programs = df['Program Name'].unique()
        for program in unique_programs:
            build_sankey_rows(program, 1, df, sankey_rows, set(), id_to_name, levels_dict)

        # Create DataFrame from sankey_rows and insert into the database
        sankey_df = pd.DataFrame(sankey_rows).drop_duplicates()

        sankey_df.to_sql('sankey_data', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        logging.info("Sankey data table populated successfully.")

    except Exception as e:
        logging.error(f"An error occurred while creating or populating sankey_data table: {e}")


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
