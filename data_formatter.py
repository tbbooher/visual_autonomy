import pandas as pd
import logging
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_and_populate_all_programs_table(df, engine):
    """
    Create and populate the 'all_programs' table in the PostgreSQL database.
    """
    try:
        with engine.connect() as conn:
            # Drop existing tables to reset the environment
            conn.execute(text("DROP TABLE IF EXISTS program_company CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS program_dependencies CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS all_programs CASCADE"))
            conn.commit()
            logging.info("Dropped existing tables: program_company, program_dependencies, all_programs.")

        with engine.connect() as conn:
            # Create the all_programs table
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
            logging.info("Created table: all_programs.")

        # Rename columns to match database schema
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

        # Clean DataFrame
        df = df.dropna(how='all')  # Remove completely empty rows
        df.columns = df.columns.str.strip()  # Strip any leading/trailing spaces from column names
        logging.info(f"Columns available in the DataFrame: {df.columns.tolist()}")

        # Validate 'id' column
        if 'id' not in df.columns:
            logging.error("'id' column is missing from the DataFrame.")
            raise KeyError("'id' column is missing.")

        if df['id'].isnull().any():
            logging.error("Some rows have null IDs, which may cause issues.")
            logging.error(f"Problematic rows:\n{df[df['id'].isnull()]}")
            raise ValueError("Null IDs found.")

        if not df['id'].apply(lambda x: str(x).isdigit()).all():
            logging.error("Some rows have non-integer IDs.")
            logging.error(f"Problematic rows:\n{df[~df['id'].apply(lambda x: str(x).isdigit())]}")
            raise ValueError("Non-integer IDs found.")

        df['id'] = pd.to_numeric(df['id'], errors='coerce').astype(int)

        # Check for duplicate IDs
        duplicates = df[df.duplicated(subset='id', keep=False)]
        if not duplicates.empty:
            logging.error(f"Duplicate IDs found: {duplicates['id'].tolist()}")
            raise ValueError(f"Duplicate IDs detected: {duplicates['id'].tolist()}")

        # Ensure 'total_funding_m' column exists
        if 'total_funding_m' not in df.columns:
            logging.error("'total_funding_m' column is missing from the DataFrame.")
            raise KeyError("'total_funding_m' column is missing.")

        # Clean 'total_funding_m' column
        df['total_funding_m'] = df['total_funding_m'].replace({r'[^\d.]': ''}, regex=True)
        df['total_funding_m'] = pd.to_numeric(df['total_funding_m'], errors='coerce')

        # Convert start_year and end_year to full date format (e.g., '2024' -> '2024-01-01')
        df['start_year'] = pd.to_datetime(df['start_year'], format='%Y', errors='coerce').dt.strftime('%Y-01-01')
        df['end_year'] = pd.to_datetime(df['end_year'], format='%Y', errors='coerce').dt.strftime('%Y-12-31')

        # Log rows where 'total_funding_m' is missing or invalid
        invalid_funding = df[df['total_funding_m'].isnull()]
        if not invalid_funding.empty:
            logging.warning(f"Invalid or missing 'total_funding_m' in rows: {invalid_funding[['id', 'total_funding_m']].to_dict(orient='records')}")

        # Process 'Start Year' and 'End Year'
        for date_col in ['start_year', 'end_year']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], format='%Y', errors='coerce')
            else:
                logging.warning(f"'{date_col}' column is missing from the DataFrame.")
                df[date_col] = pd.NaT

        # Select only the columns that exist in the DataFrame to avoid KeyErrors
        expected_columns = [
            'id', 'program_name', 'short_name', 'org', 'description', 'impact', 'status',
            'companies', 'total_funding_m', 'start_year', 'end_year', 'dependency',
            'theme', 'importance', 'notes_with_applied'
        ]
        existing_columns = [col for col in expected_columns if col in df.columns]
        df = df[existing_columns]

        # Insert data into all_programs table
        df.to_sql('all_programs', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        logging.info("All programs table populated successfully.")
        logging.info(f"Inserted {len(df)} rows into the all_programs table.")

    except Exception as e:
        logging.error(f"An error occurred while creating or populating all_programs table: {e}")
        raise e  # Re-raise the exception for handling upstream

def create_and_populate_dependency_table(df, engine):
    """
    Create and populate the 'program_dependencies' table in the PostgreSQL database.
    """
    try:
        with engine.connect() as conn:
            # Drop existing table
            conn.execute(text("DROP TABLE IF EXISTS program_dependencies CASCADE"))
            conn.commit()
            logging.info("Dropped existing table: program_dependencies.")

        with engine.connect() as conn:
            # Create the program_dependencies table
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
            logging.info("Created table: program_dependencies.")

        # Filter rows with dependencies
        dependency_df = df[df['dependency'].notna() & (df['dependency'] != '')].copy()
        dependency_df = dependency_df[['id', 'dependency']].drop_duplicates()

        # Assuming 'dependency' column contains comma-separated dependency IDs
        dependency_rows = set()  # Using a set to store unique (program_id, dependency_id) pairs
        for _, row in dependency_df.iterrows():
            program_id = row['id']
            dependencies = row['dependency']
            for dependency_id in dependencies.split(','):
                dependency_id = dependency_id.strip()
                if dependency_id:
                    try:
                        dep_id_int = int(dependency_id)
                        dependency_rows.add((program_id, dep_id_int))
                    except ValueError:
                        logging.warning(f"Invalid dependency ID: '{dependency_id}' in program ID {program_id}")

        # Convert the set to a DataFrame
        dependency_df = pd.DataFrame(list(dependency_rows), columns=['program_id', 'dependency_id'])

        # Validate program IDs
        with engine.connect() as conn:
            valid_program_ids = pd.read_sql('SELECT id FROM all_programs', conn)['id'].tolist()

        # Filter out invalid program IDs
        valid_dependency_df = dependency_df[
            dependency_df['program_id'].isin(valid_program_ids) &
            dependency_df['dependency_id'].isin(valid_program_ids)
        ]

        invalid_dependencies = dependency_df[
            ~dependency_df['program_id'].isin(valid_program_ids) |
            ~dependency_df['dependency_id'].isin(valid_program_ids)
        ]
        if not invalid_dependencies.empty:
            logging.warning(f"Foreign key violations found for dependencies: {invalid_dependencies.to_dict(orient='records')}")

        if not valid_dependency_df.empty:
            valid_dependency_df.to_sql('program_dependencies', engine, if_exists='append', index=False, method='multi', chunksize=1000)
            logging.info("Program dependencies table populated successfully.")
        else:
            logging.warning("No valid dependencies found to populate.")

    except Exception as e:
        logging.error(f"An error occurred while creating or populating program_dependencies table: {e}")
        raise e

def create_and_populate_company_tables(df, engine):
    """
    Create and populate the 'company' and 'program_company' tables in the PostgreSQL database.
    """
    try:
        with engine.connect() as conn:
            # Drop existing tables
            conn.execute(text("DROP TABLE IF EXISTS program_company CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS company CASCADE"))
            conn.commit()
            logging.info("Dropped existing tables: program_company, company.")

        with engine.connect() as conn:
            # Create the company table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS company (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE
                )
            """))
            # Create the program_company table
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
            logging.info("Created tables: company, program_company.")

        # Extract unique company names
        company_names = set()
        program_company_rows = []
        for _, row in df.iterrows():
            program_id = row['id']
            companies = row.get('companies', '')
            if pd.notna(companies):
                for company in set(companies.split(',')):  # Split by comma without space to be safe
                    company = company.strip()
                    if company:  # Ensure company name is not empty
                        company_names.add(company)
                        program_company_rows.append({'program_id': program_id, 'company_name': company})

        # Insert unique company names into the company table
        company_df = pd.DataFrame(list(company_names), columns=['name'])
        if not company_df.empty:
            company_df.to_sql('company', engine, if_exists='append', index=False, method='multi', chunksize=1000)
            logging.info(f"Inserted {len(company_df)} unique companies into the company table.")
        else:
            logging.warning("No companies found to insert into the company table.")

        # Fetch company IDs
        with engine.connect() as conn:
            company_map_df = pd.read_sql('SELECT id, name FROM company', conn)
            company_map = dict(zip(company_map_df['name'], company_map_df['id']))

        # Map program-company relationships
        program_company_rows_mapped = [
            {'program_id': row['program_id'], 'company_id': company_map.get(row['company_name'])}
            for row in program_company_rows
            if row['company_name'] in company_map
        ]

        # Remove any rows where company_id could not be mapped
        valid_program_company_rows = [row for row in program_company_rows_mapped if row['company_id'] is not None]

        if not valid_program_company_rows:
            logging.error("No valid program-company associations found to insert.")
            return

        # Create DataFrame and remove duplicates
        program_company_df = pd.DataFrame(valid_program_company_rows).drop_duplicates()

        # Validate program IDs
        with engine.connect() as conn:
            valid_program_ids = pd.read_sql('SELECT id FROM all_programs', conn)['id'].tolist()

        # Filter out invalid program IDs
        valid_program_company_df = program_company_df[
            program_company_df['program_id'].isin(valid_program_ids)
        ]

        invalid_program_company_df = program_company_df[
            ~program_company_df['program_id'].isin(valid_program_ids)
        ]
        if not invalid_program_company_df.empty:
            logging.error(f"Foreign key violation: Invalid program IDs {invalid_program_company_df['program_id'].unique().tolist()}")
            raise ValueError("Invalid program ID references.")

        # Insert into program_company table
        if not valid_program_company_df.empty:
            valid_program_company_df.to_sql('program_company', engine, if_exists='append', index=False, method='multi', chunksize=1000)
            logging.info("Program_company table populated successfully.")
            logging.info(f"Inserted {len(valid_program_company_df)} rows into the program_company table.")
        else:
            logging.warning("No valid program-company associations found to insert after filtering.")

    except Exception as e:
        logging.error(f"An error occurred while populating company tables: {e}")
        raise e