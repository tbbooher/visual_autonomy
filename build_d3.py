import json
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get Neo4j credentials from environment variables
neo4j_user = os.getenv("NEO4J_USER")
neo4j_password = os.getenv("NEO4J_PASSWORD")

# Initialize the Neo4j driver with credentials from the environment
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=(neo4j_user, neo4j_password))

# Function to get flow data
def get_flow_data():
    query = """
    MATCH (p:Program)
    OPTIONAL MATCH (p)-[r:DEPENDS_ON]->(p2:Program)
    RETURN p.id AS program_id, p2.id AS dependency_id, p.total_funding_m AS funding
    """
    
    with neo4j_driver.session() as session:
        result = session.run(query)
        data = []
        for record in result:
            data.append({
                "source": record["program_id"],
                "target": record["dependency_id"],
                "value": record["funding"]
            })
    return data

# Get the flow data
flow_data = get_flow_data()

# Save the data to a JSON file for D3.js consumption
with open('flow_data.json', 'w') as f:
    json.dump(flow_data, f)

# Close the Neo4j driver
neo4j_driver.close()