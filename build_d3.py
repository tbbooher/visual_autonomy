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
    Extract data from PostgreSQL, process it, and prepare for Sankey diagram.
    """
    # Extract all programs and dependencies data
    all_programs_df = pd.read_sql("SELECT * FROM all_programs", engine)
    program_dependencies_df = pd.read_sql("SELECT * FROM program_dependencies", engine)
    
    # Merge programs with their dependencies
    merged_df = pd.merge(all_programs_df, program_dependencies_df, left_on='id', right_on='program_id', how='left')
    merged_df = pd.merge(merged_df, all_programs_df[['id', 'short_name']], left_on='dependency_id', right_on='id', how='left', suffixes=('', '_source'))
    
    # Initialize data structures for output
    data = []
    text_output = []
    
    # Process each program to build the relationship and funding flow
    for _, row in merged_df.iterrows():
        source = row['short_name_source'] if pd.notna(row['short_name_source']) else None
        target = row['short_name']
        value = row['total_funding_m']
        theme = row['theme']
        
        # Append to list for JSON output
        data.append({
            "source": source,
            "target": target,
            "value": value,
            "theme": theme
        })
        
        # Prepare text output
        if source:
            text_output.append(f"{target} [{value}] {theme}")
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
