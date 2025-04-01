# Import necessary libraries
import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv  # New dependency

# Load environment variables from .env file
load_dotenv()

def xml_to_sql(xml_file_path, table_name):
    """
    Reads XML file and uploads it to SQL Server database
    Parameters:
    xml_file_path (str): Path to XML file
    table_name (str): Target table name
    """
    
    # Get credentials from environment variables
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')

    # Read XML file using pandas
    try:
        df = pd.read_xml(xml_file_path)
        print("XML file successfully read!")
        print("Data preview:")
        print(df.head())
    except Exception as e:
        print(f"Error reading XML file: {e}")
        return

    # Build connection string dynamically
    connection_string = f"""
        DRIVER={{SQL Server}};
        SERVER={server};
        DATABASE={database};
    """
    
    # Add authentication based on available credentials
    if username and password:
        connection_string += f"UID={username};PWD={password};"
    else:
        connection_string += "Trusted_Connection=yes;"
        print("Using Windows authentication")
