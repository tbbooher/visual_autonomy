import os
import pandas as pd
from sqlalchemy import create_engine, text
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

print(os.getcwd())  # Print current working directory

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

def split_dependencies_and_unroll(df):
    try:
        df['Dependency'] = df['Dependency'].str.split(', ')
        df = df.explode('Dependency')
        
        # df['Total Funding (m)'] = df['Total Funding (m)'].replace({r'\\\$': '', ',': ''}, regex=True)
        df['Total Funding (m)'] = df['Total Funding (m)'].replace({r'[^\d.]': ''}, regex=True)
        df['Total Funding (m)'] = pd.to_numeric(df['Total Funding (m)'], errors='coerce')        

        df['Start Year'] = pd.to_datetime(df['Start Year'], format='%Y', errors='coerce').dt.to_period('M').dt.to_timestamp(how='start')  # Convert to first day of the month
        df['End Year'] = pd.to_datetime(df['End Year'], format='%Y', errors='coerce').dt.to_period('M').dt.to_timestamp(how='end')  # Convert to last day of the month
        
        df = add_before_after_states(df)
        
        return df
    except Exception as e:
        logging.error(f"An error occurred while processing data: {e}")
        return df

def add_before_after_states(df):
    df['Before State'] = df['Program Name']
    # Set 'After State' to dependency if it exists; otherwise, set to the same as 'Before State'
    df['After State'] = df['Dependency'].fillna(df['Before State'])
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

            # Format the line for the Sankey diagram, including additional context
            line = f"{before_state} [{int(funding)}] {after_state} // Org: {org}, Theme: {theme}"
            output_lines.append(line)
        
        return "\n".join(output_lines)
    except Exception as e:
        logging.error(f"An error occurred while generating Sankey output: {e}")
        return ""

def create_and_populate_all_programs_table(df, engine):
    try:
        # Drop the all_programs table to completely replace it
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS all_programs CASCADE"))
            conn.commit()  # Ensure the drop command is committed

        # Create the all_programs table with Before State and After State columns
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS all_programs (
                    "Program Name" TEXT,
                    "Org" TEXT,
                    "Description" TEXT,
                    "Impact" TEXT,
                    "Status" TEXT,
                    "Companies" TEXT,
                    "Total Funding (m)" DOUBLE PRECISION,
                    "Start Year" DATE,
                    "End Year" DATE,
                    "ID" TEXT,
                    "Dependency" TEXT,
                    "Theme" TEXT,
                    "Importance" TEXT,
                    "Notes with Applied" TEXT,
                    "Before State" TEXT,
                    "After State" TEXT
                )
            """))
            conn.commit()  # Ensure the create command is committed

        # Insert the data into the all_programs table
        df.to_sql('all_programs', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        logging.info("All programs table populated successfully.")

        # Output the number of rows inserted
        logging.info(f"Inserted {len(df)} rows into the all_programs table.")
    except Exception as e:
        logging.error(f"An error occurred while creating or populating all_programs table: {e}")

def create_and_populate_company_tables(df, engine):
    try:
        # Drop the tables to completely replace them
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS program_company"))
            conn.execute(text("DROP TABLE IF EXISTS company CASCADE"))
            conn.commit()  # Ensure the drop commands are committed

        # Recreate the tables
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS company (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS program_company (
                    program_id TEXT,
                    company_id INT,
                    PRIMARY KEY (program_id, company_id),
                    FOREIGN KEY (company_id) REFERENCES company(id)
                )
            """))
            conn.commit()  # Ensure the create commands are committed

        # Extract and split company names
        company_names = set()
        program_company_rows = []
        for _, row in df.iterrows():
            program_id = row['ID']
            companies = row['Companies']
            if pd.notna(companies):
                for company in set(companies.split(', ')):
                    company = company.strip()
                    company_names.add(company)
                    program_company_rows.append({'program_id': program_id, 'company_name': company})

        # Insert unique companies into the 'company' table
        company_df = pd.DataFrame(list(company_names), columns=['name'])
        company_df.to_sql('company', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        # Create a mapping of company names to IDs
        with engine.connect() as conn:
            company_map = pd.read_sql('SELECT id, name FROM company', conn)
            company_map = dict(zip(company_map['name'], company_map['id']))

        # Populate the 'program_company' join table
        program_company_rows = [{'program_id': row['program_id'], 'company_id': company_map[row['company_name']]} 
                                for row in program_company_rows if row['company_name'] in company_map]
        
        # Remove duplicates from program_company_rows
        program_company_df = pd.DataFrame(program_company_rows).drop_duplicates()
        program_company_df.to_sql('program_company', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        logging.info("Company and program_company tables populated successfully.")
    except Exception as e:
        logging.error(f"An error occurred while populating company tables: {e}")

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
    
    # Create SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine(DATABASE_URL)
    
    # Populate all_programs table
    logging.info("Creating and populating all_programs table...")
    create_and_populate_all_programs_table(processed_df, engine)
    
    # Populate company and program_company tables
    logging.info("Creating and populating company tables...")
    create_and_populate_company_tables(processed_df, engine)
    
    logging.info("Process completed successfully.")
