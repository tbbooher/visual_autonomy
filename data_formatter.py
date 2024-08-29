import pandas as pd
import logging
from sqlalchemy import text

def create_and_populate_all_programs_table(df, engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS program_company CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS program_dependencies CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS all_programs CASCADE"))
            conn.commit()

        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS all_programs (
                    id SERIAL PRIMARY KEY,
                    program_name TEXT,
                    short_name TEXT,
                    org TEXT,
                    description TEXT,
                    impact TEXT,
                    status TEXT,
                    companies TEXT,
                    total_funding_m DOUBLE PRECISION,
                    start_year DATE,
                    end_year DATE,
                    dependency TEXT,
                    theme TEXT,
                    importance TEXT,
                    notes_with_applied TEXT
                )
            """))
            conn.commit()

        # some cleaning
        df['Total Funding (m)'] = df['Total Funding (m)'].replace({r'[^\d.]': ''}, regex=True)
        df['Total Funding (m)'] = pd.to_numeric(df['Total Funding (m)'], errors='coerce')

        df['id'] = df['id'].astype(int)

        df['Start Year'] = pd.to_datetime(df['Start Year'], format='%Y', errors='coerce').dt.to_period('M').dt.to_timestamp(how='start')
        df['End Year'] = pd.to_datetime(df['End Year'], format='%Y', errors='coerce').dt.to_period('M').dt.to_timestamp(how='end')
        
        if not df['id'].is_unique:
            raise ValueError("IDs are not unique.")

        if not df['id'].apply(lambda x: isinstance(x, int)).all():
            raise TypeError("IDs are not integers.")

        df.rename(columns={
            'Program Name': 'program_name',
            'Short Name': 'short_name',
            'Org': 'org',
            'Description': 'description',
            'Impact': 'impact',
            'Status': 'status',
            'Companies': 'companies',
            'Total Funding (m)': 'total_funding_m',
            'Start Year': 'start_year',
            'End Year': 'end_year',
            'Dependency': 'dependency',
            'Theme': 'theme',
            'Importance': 'importance',
            'Notes with Applied': 'notes_with_applied'
        }, inplace=True)

        df.to_sql('all_programs', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        logging.info("All programs table populated successfully.")
        logging.info(f"Inserted {len(df)} rows into the all_programs table.")
    except Exception as e:
        logging.error(f"An error occurred while creating or populating all_programs table: {e}")

def create_and_populate_dependency_table(df, engine):
    """
    Create and populate the 'program_dependencies' table in the PostgreSQL database.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS program_dependencies CASCADE"))
            conn.commit()

        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS program_dependencies (
                    id SERIAL PRIMARY KEY,
                    program_id INT,
                    dependency_id INT,
                    UNIQUE (program_id, dependency_id),
                    FOREIGN KEY (program_id) REFERENCES all_programs(id),
                    FOREIGN KEY (dependency_id) REFERENCES all_programs(id)
                )
            """))
            conn.commit()

        dependency_df = df[df['dependency'].notna() & (df['dependency'] != '')].copy()
        dependency_df = dependency_df[['id', 'dependency']].drop_duplicates()

        # Assuming 'Dependency' column contains comma-separated dependency IDs
        dependency_rows = set()  # Using a set to store unique (program_id, dependency_id) pairs
        for _, row in dependency_df.iterrows():
            program_id = row['id']
            dependencies = row['dependency']
            for dependency_id in dependencies.split(','):
                dependency_id = dependency_id.strip()
                try:
                    pair = (program_id, int(dependency_id))
                    if pair not in dependency_rows:  # Check if this pair is already added
                        dependency_rows.add(pair)
                except ValueError:
                    logging.warning(f"Invalid dependency ID: {dependency_id}")

        # Convert the set back to a DataFrame
        dependency_df = pd.DataFrame(list(dependency_rows), columns=['program_id', 'dependency_id'])

        # Only insert dependencies with valid program IDs
        with engine.connect() as conn:
            valid_program_ids = pd.read_sql('SELECT id FROM all_programs', conn)['id'].tolist()
            dependency_df = dependency_df[
                dependency_df['program_id'].isin(valid_program_ids) &
                dependency_df['dependency_id'].isin(valid_program_ids)
            ]

        if not dependency_df.empty:
            dependency_df.to_sql('program_dependencies', engine, if_exists='append', index=False, method='multi', chunksize=1000)
            logging.info("Program dependencies table populated successfully.")
        else:
            logging.warning("No valid dependencies found to populate.")

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
                    program_id INT,
                    company_id INT,
                    PRIMARY KEY (program_id, company_id),
                    FOREIGN KEY (program_id) REFERENCES all_programs(id),
                    FOREIGN KEY (company_id) REFERENCES company(id)
                )
            """))
            conn.commit()

        company_names = set()
        program_company_rows = []
        for _, row in df.iterrows():
            program_id = row['id']
            companies = row.get('companies', '')
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
