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
    # SQL query to extract source, target, source funding, target funding, value, and theme
    query = """
            SELECT 
                source_program.short_name AS source,
                target_program.short_name AS target,
                source_program.total_funding_m AS source_funding,
                target_program.total_funding_m AS target_funding,
                COALESCE(source_program.total_funding_m, target_program.total_funding_m) AS value,
                target_program.theme AS theme
            FROM 
                program_dependencies
            JOIN 
                all_programs AS source_program ON program_dependencies.dependency_id = source_program.id
            JOIN 
                all_programs AS target_program ON program_dependencies.program_id = target_program.id
            WHERE 
                target_program.theme = 'Combat Autonomy';
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
        theme = row['theme']
        
        # Append to list for JSON output
        data.append({
            "source": source,
            "target": target,
            "source_funding": source_funding,
            "target_funding": target_funding,
            "value": value,
            "theme": theme
        })
        
        # Prepare text output correctly
        if source:
            text_output.append(f"{source} (Source Funding: {source_funding}) -> {target} (Target Funding: {target_funding}) [{value}] {theme}")
        else:
            text_output.append(f"{target} [{value}] {theme}")

    # Display the text output
    print("\n".join(text_output))
    
    return data

# Get the processed data
sankey_data = extract_and_process_data()

# Save the data to a JSON file for D3.js consumption
with open('flow_data.json', 'w') as f:
    json.dump(sankey_data, f, indent=4)  # Pretty print JSON for better readability
