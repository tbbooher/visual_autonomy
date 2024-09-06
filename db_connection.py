# db_connection.py

import os
from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_postgres_engine():
    """
    Creates and returns a connection engine to PostgreSQL.
    """
    POSTGRES_USER = os.getenv('DATABASE_USER')
    POSTGRES_PASSWORD = os.getenv('DATABASE_PASSWORD')
    POSTGRES_DB = os.getenv('CURRENT_DB_NAME')
    POSTGRES_HOST = os.getenv('DATABASE_HOST')
    POSTGRES_PORT = os.getenv('LOCAL_DATABASE_PORT')

    DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    engine = create_engine(DATABASE_URL)
    return engine

def get_google_sheet_service():
    """
    Creates and returns the Google Sheets API service.
    """
    SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
    
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    service = build('sheets', 'v4', credentials=creds)
    return service
