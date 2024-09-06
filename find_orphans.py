# run_query.py
# this code just runs a sql query to find orphaned programs in the database.

from db_connection import get_postgres_engine
import pandas as pd

def run_query():
    # Get the PostgreSQL engine
    engine = get_postgres_engine()

    # Define the SQL query
    query = """
    WITH no_outbound AS (
        SELECT ap.id
        FROM all_programs ap
        LEFT JOIN program_dependencies pd ON ap.id = pd.program_id
        WHERE pd.program_id IS NULL
    ),
    no_inbound AS (
        SELECT ap.id
        FROM all_programs ap
        LEFT JOIN program_dependencies pd ON ap.id = pd.dependency_id
        WHERE pd.dependency_id IS NULL
    )
    SELECT ap.*
    FROM all_programs ap
    WHERE ap.id IN (SELECT id FROM no_outbound)
    AND ap.id IN (SELECT id FROM no_inbound);
    """

    # Execute the query and fetch the result into a DataFrame
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    
    # Display the result
    print(result)

if __name__ == "__main__":
    run_query()
