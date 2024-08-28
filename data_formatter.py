import pandas as pd
import logging
from sqlalchemy import text

def split_dependencies_and_unroll(df):
    """
    Split the 'Dependency' column and unroll it into multiple rows for each dependency.
    Converts funding to numeric and date columns to the correct format.
    """    
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
    """
    Add 'Before State' and 'After State' columns to denote program state transitions.
    """
    df['Before State'] = df['Program Name']
    df['After State'] = df['Dependency'].fillna(df['Before State'])
    return df

def create_and_populate_all_programs_table(df, engine):
    """
    Create and populate the 'all_programs' table in the PostgreSQL database.
    """    
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS all_programs CASCADE"))
            conn.commit()

        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS all_programs (
                    id SERIAL PRIMARY KEY,  -- Change ID to be a primary key
                    "Program Name" TEXT,
                    "Org" TEXT,
                    "Description" TEXT,
                    "Impact" TEXT,
                    "Status" TEXT,
                    "Companies" TEXT,
                    "Total Funding (m)" DOUBLE PRECISION,
                    "Start Year" DATE,
                    "End Year" DATE,
                    "Dependency" TEXT,
                    "Theme" TEXT,
                    "Importance" TEXT,
                    "Notes with Applied" TEXT,
                    "Before State" TEXT,
                    "After State" TEXT
                )
            """))
            conn.commit()

        df['ID'] = pd.to_numeric(df['ID'], errors='coerce').fillna(0).astype(int)  # Ensure ID is numeric and not null
        df.to_sql('all_programs', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        logging.info("All programs table populated successfully.")
        logging.info(f"Inserted {len(df)} rows into the all_programs table.")
    except Exception as e:
        logging.error(f"An error occurred while creating or populating all_programs table: {e}")


def create_and_populate_dependency_table(df, engine):
    """
    Create and populate the 'program_dependencies' table in the PostgreSQL database.
    This table establishes relationships between programs and their dependencies.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS program_dependencies CASCADE"))
            conn.commit()

        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS program_dependencies (
                    id SERIAL PRIMARY KEY,
                    program_id INT,  -- Use INT to match the 'id' column in all_programs
                    dependency_id INT,  -- Use INT to match the 'id' column in all_programs
                    UNIQUE (program_id, dependency_id),
                    FOREIGN KEY (program_id) REFERENCES all_programs(id),
                    FOREIGN KEY (dependency_id) REFERENCES all_programs(id)
                )
            """))
            conn.commit()

        # Filter for rows where there is a valid dependency
        dependency_df = df[df['Dependency'].notna() & (df['Dependency'] != '')].copy()
        dependency_df = dependency_df[['ID', 'Dependency']].drop_duplicates()

        dependency_df.columns = ['program_id', 'dependency_id']
        dependency_df['program_id'] = pd.to_numeric(dependency_df['program_id'], errors='coerce').fillna(0).astype(int)
        dependency_df['dependency_id'] = pd.to_numeric(dependency_df['dependency_id'], errors='coerce').fillna(0).astype(int)

        dependency_df.to_sql('program_dependencies', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        logging.info("Program dependencies table populated successfully.")
    except Exception as e:
        logging.error(f"An error occurred while creating or populating program_dependencies table: {e}")


def create_and_populate_company_tables(df, engine):
    """
    Create and populate the 'company' and 'program_company' tables in the PostgreSQL database.
    """    
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