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
    df['After State'] = df['Dependency'].fillna(df['Before State'])
    return df

def create_and_populate_all_programs_table(df, engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS all_programs CASCADE"))
            conn.commit()

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
            conn.commit()

        df.to_sql('all_programs', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        logging.info("All programs table populated successfully.")
        logging.info(f"Inserted {len(df)} rows into the all_programs table.")
    except Exception as e:
        logging.error(f"An error occurred while creating or populating all_programs table: {e}")

def create_and_populate_company_tables(df, engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS program_company"))
            conn.execute(text("DROP TABLE IF EXISTS company CASCADE"))
            conn.commit()

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
            conn.commit()

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

        company_df = pd.DataFrame(list(company_names), columns=['name'])
        company_df.to_sql('company', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        with engine.connect() as conn:
            company_map = pd.read_sql('SELECT id, name FROM company', conn)
            company_map = dict(zip(company_map['name'], company_map['id']))

        program_company_rows = [{'program_id': row['program_id'], 'company_id': company_map[row['company_name']]} 
                                for row in program_company_rows if row['company_name'] in company_map]

        program_company_df = pd.DataFrame(program_company_rows).drop_duplicates()
        program_company_df.to_sql('program_company', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        logging.info("Company and program_company tables populated successfully.")
    except Exception as e:
        logging.error(f"An error occurred while populating company tables: {e}")

def create_and_populate_sankey_data_table(df, engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sankey_data"))
            conn.commit()

        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sankey_data (
                    "Source" TEXT,
                    "Target" TEXT,
                    "Level" INT,
                    "Value" DOUBLE PRECISION,
                    "Theme" TEXT,
                    "Total Funding (m)" DOUBLE PRECISION,
                    "Start Year" DATE,
                    "End Year" DATE
                )
            """))
            conn.commit()

        sankey_rows = []

        # Find all unique programs to handle the dependencies correctly
        all_programs = df['Program Name'].unique()
        
        # Process each program to trace its dependency chain
        for program in all_programs:
            level = 0
            current_program = program

            # Use a set to track visited programs to avoid infinite loops
            visited_programs = set()

            while True:
                if current_program in visited_programs:
                    # If we detect a loop, terminate by linking back to the program itself
                    sankey_rows.append({
                        'Source': current_program,
                        'Target': current_program,
                        'Level': level,
                        'Value': df[df['Program Name'] == current_program]['Total Funding (m)'].sum(),
                        'Theme': df[df['Program Name'] == current_program]['Theme'].values[0] if not df[df['Program Name'] == current_program]['Theme'].empty else None,
                        'Total Funding (m)': df[df['Program Name'] == current_program]['Total Funding (m)'].sum(),
                        'Start Year': df[df['Program Name'] == current_program]['Start Year'].min(),
                        'End Year': df[df['Program Name'] == current_program]['End Year'].max()
                    })
                    break

                visited_programs.add(current_program)
                dependencies = df[df['Program Name'] == current_program]['Dependency'].dropna().tolist()
                
                if not dependencies:
                    # If there are no dependencies, link the program to itself
                    sankey_rows.append({
                        'Source': current_program,
                        'Target': current_program,
                        'Level': level,
                        'Value': df[df['Program Name'] == current_program]['Total Funding (m)'].sum(),
                        'Theme': df[df['Program Name'] == current_program]['Theme'].values[0] if not df[df['Program Name'] == current_program]['Theme'].empty else None,
                        'Total Funding (m)': df[df['Program Name'] == current_program]['Total Funding (m)'].sum(),
                        'Start Year': df[df['Program Name'] == current_program]['Start Year'].min(),
                        'End Year': df[df['Program Name'] == current_program]['End Year'].max()
                    })
                    break

                next_program = None
                for dep in dependencies:
                    if dep != current_program:
                        sankey_rows.append({
                            'Source': current_program,
                            'Target': dep,
                            'Level': level,
                            'Value': df[df['Program Name'] == current_program]['Total Funding (m)'].sum(),
                            'Theme': df[df['Program Name'] == current_program]['Theme'].values[0] if not df[df['Program Name'] == current_program]['Theme'].empty else None,
                            'Total Funding (m)': df[df['Program Name'] == current_program]['Total Funding (m)'].sum(),
                            'Start Year': df[df['Program Name'] == current_program]['Start Year'].min(),
                            'End Year': df[df['Program Name'] == current_program]['End Year'].max()
                        })
                        next_program = dep
                        level += 1
                        break

                if not next_program or next_program == current_program:
                    # If no new program is found or if it loops back to the current, terminate
                    sankey_rows.append({
                        'Source': current_program,
                        'Target': current_program,
                        'Level': level,
                        'Value': df[df['Program Name'] == current_program]['Total Funding (m)'].sum(),
                        'Theme': df[df['Program Name'] == current_program]['Theme'].values[0] if not df[df['Program Name'] == current_program]['Theme'].empty else None,
                        'Total Funding (m)': df[df['Program Name'] == current_program]['Total Funding (m)'].sum(),
                        'Start Year': df[df['Program Name'] == current_program]['Start Year'].min(),
                        'End Year': df[df['Program Name'] == current_program]['End Year'].max()
                    })
                    break

                current_program = next_program

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
