import pandas as pd
import json
from db_connection import get_postgres_engine

def extract_and_process_data():
    """
    Extract data from PostgreSQL using a direct SQL query, process it, and prepare for Sankey diagram.
    """
    # Initialize PostgreSQL connection using db_connection module
    engine = get_postgres_engine()

    # SQL query to extract source, target, source funding, target funding, value, and themes
    query = """
            SELECT 
                source_program.short_name AS source,
                target_program.short_name AS target,
                source_program.org AS source_org,
                source_program.program_name AS source_name,
                target_program.program_name AS target_name,
                source_program.total_funding_m AS source_funding,
                target_program.total_funding_m AS target_funding,
                COALESCE(source_program.total_funding_m, target_program.total_funding_m) AS value,
                source_program.theme AS source_theme,
                target_program.theme AS target_theme,
                source_program.companies AS source_companies,
                source_program.description AS source_description
            FROM 
                program_dependencies
            JOIN 
                all_programs AS source_program ON program_dependencies.dependency_id = source_program.id
            JOIN 
                all_programs AS target_program ON program_dependencies.program_id = target_program.id;
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
        source_companies = row['source_companies']
        source_description = row['source_description']
        source_name = row['source_name']
        target_name = row['target_name']
        source_org = row['source_org']
        
        # Append to list for JSON output
        data.append({
            "source": source,
            "target": target,
            "source_funding": source_funding,
            "target_funding": target_funding,
            "value": value,
            "source_theme": source_theme,
            "target_theme": target_theme,
            "source_companies": source_companies,
            "source_description": source_description,
            "source_name": source_name,
            "target_name": target_name,
            "source_org": source_org
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
