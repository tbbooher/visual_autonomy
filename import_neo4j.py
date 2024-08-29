import os
import pandas as pd
from sqlalchemy import create_engine
from neo4j import GraphDatabase
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection settings
POSTGRES_USER = os.getenv('DATABASE_USER')
POSTGRES_PASSWORD = os.getenv('DATABASE_PASSWORD')
POSTGRES_DB = os.getenv('CURRENT_DB_NAME')
POSTGRES_HOST = os.getenv('DATABASE_HOST')
POSTGRES_PORT = os.getenv('LOCAL_DATABASE_PORT')

# Neo4j connection settings
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')

# PostgreSQL connection string
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Initialize PostgreSQL connection
engine = create_engine(DATABASE_URL)

# Initialize Neo4j driver
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def extract_data_from_postgres():
    """
    Extract data from PostgreSQL tables into DataFrames.
    """
    logging.info("Extracting data from PostgreSQL...")
    
    all_programs_df = pd.read_sql("SELECT * FROM all_programs", engine)
    program_dependencies_df = pd.read_sql("SELECT * FROM program_dependencies", engine)
    program_company_df = pd.read_sql("SELECT * FROM program_company", engine)
    company_df = pd.read_sql("SELECT * FROM company", engine)
    
    return all_programs_df, program_dependencies_df, program_company_df, company_df

def load_data_into_neo4j(all_programs_df, program_dependencies_df, program_company_df, company_df):
    """
    Load data into Neo4j from DataFrames.
    """
    logging.info("Loading data into Neo4j...")
    
    with neo4j_driver.session() as session:
        # Clear existing data
        session.run("MATCH (n) DETACH DELETE n")
        logging.info("Existing data in Neo4j has been cleared.")

        # Create Program nodes
        for _, row in all_programs_df.iterrows():
            session.run("""
                MERGE (p:Program {id: $id})
                SET p.program_name = $program_name,
                    p.short_name = $short_name,
                    p.org = $org,
                    p.description = $description,
                    p.impact = $impact,
                    p.status = $status,
                    p.total_funding_m = $total_funding_m,
                    p.start_year = $start_year,
                    p.end_year = $end_year,
                    p.theme = $theme,
                    p.importance = $importance,
                    p.notes_with_applied = $notes_with_applied
            """, parameters=row.to_dict())
        
        # Create Company nodes and relationships
        for _, row in company_df.iterrows():
            session.run("MERGE (c:Company {id: $id, name: $name})", parameters=row.to_dict())

        for _, row in program_company_df.iterrows():
            session.run("""
                MATCH (p:Program {id: $program_id})
                MATCH (c:Company {id: $company_id})
                MERGE (p)-[:ASSOCIATED_WITH]->(c)
            """, parameters=row.to_dict())

        # Create Dependency relationships
        for _, row in program_dependencies_df.iterrows():
            session.run("""
                MATCH (p1:Program {id: $program_id})
                MATCH (p2:Program {id: $dependency_id})
                MERGE (p1)-[:DEPENDS_ON]->(p2)
            """, parameters=row.to_dict())

if __name__ == "__main__":
    # Extract data from PostgreSQL
    all_programs_df, program_dependencies_df, program_company_df, company_df = extract_data_from_postgres()
    
    # Load data into Neo4j
    load_data_into_neo4j(all_programs_df, program_dependencies_df, program_company_df, company_df)

    logging.info("ETL process completed successfully.")
