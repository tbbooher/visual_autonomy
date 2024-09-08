import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection settings
POSTGRES_USER = os.getenv('DATABASE_USER')
POSTGRES_PASSWORD = os.getenv('DATABASE_PASSWORD')
POSTGRES_DB = os.getenv('CURRENT_DB_NAME')
POSTGRES_HOST = os.getenv('DATABASE_HOST')
POSTGRES_PORT = os.getenv('LOCAL_DATABASE_PORT')

# PostgreSQL connection string
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Initialize PostgreSQL connection
engine = create_engine(DATABASE_URL)

def extract_and_process_data():
    """
    Extract data from PostgreSQL using a direct SQL query, process it, and prepare for Sankey diagram.
    """
    query = """
        SELECT 
            source_program.short_name AS source,
            target_program.short_name AS target,
            source_program.total_funding_m AS source_funding,
            target_program.total_funding_m AS target_funding,
            COALESCE(source_program.total_funding_m, target_program.total_funding_m) AS value,
            source_program.theme AS source_theme,
            target_program.theme AS target_theme,
            source_program.program_name AS source_program_name,
            target_program.program_name AS target_program_name,
            source_program.companies AS source_companies,
            target_program.companies AS target_companies,
            source_program.description AS source_description,
            target_program.description AS target_description
        FROM 
            all_programs AS source_program
        JOIN 
            all_programs AS target_program ON source_program.id = target_program.id;
    """
    
    # Execute the SQL query
    df = pd.read_sql(query, engine)

    # Initialize data structures for output
    data = []
    text_output = []
    
    # Process each row to build the relationship and funding flow
    for _, row in df.iterrows():
        source = row['source']
        target = row['target']
        source_funding = row['source_funding']
        target_funding = row['target_funding']
        value = row['value']
        source_theme = row['source_theme']
        target_theme = row['target_theme']
        source_program_name = row['source_program_name']
        target_program_name = row['target_program_name']
        source_companies = row['source_companies']
        target_companies = row['target_companies']
        source_description = row['source_description']
        target_description = row['target_description']
        
        # Append to list for JSON output
        data.append({
            "source": source,
            "target": target,
            "source_funding": source_funding,
            "target_funding": target_funding,
            "value": value,
            "source_theme": source_theme,
            "target_theme": target_theme,
            "source_program_name": source_program_name,
            "target_program_name": target_program_name,
            "source_companies": source_companies,
            "target_companies": target_companies,
            "source_description": source_description,
            "target_description": target_description
        })
        
        # Prepare text output correctly
        if source:
            text_output.append(f"{source} (Source Funding: {source_funding}) -> {target} (Target Funding: {target_funding}) [{value}] Source Theme: {source_theme}, Target Theme: {target_theme}")
        else:
            text_output.append(f"{target} [{value}] Source Theme: {source_theme}, Target Theme: {target_theme}")

    # Display the text output
    print("\n".join(text_output))
    
    return data

# Get the processed data
sankey_data = extract_and_process_data()

# Save the data to a JSON file for D3.js consumption
with open('flow_data.json', 'w') as f:
    json.dump(sankey_data, f, indent=4)  # Pretty print JSON for better readability
