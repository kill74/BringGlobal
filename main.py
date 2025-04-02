import pandas as pd
import os
import pyodbc

# This will load the variables from the .env file (Its more secure)
def load_env(file_path='.env'):
    """
    Reads a .env file and sets environment variables
    Parameters:
    file_path (str): Path to .env file (default: current directory)
    """
    try:
        with open(file_path) as f: # Used the abreviation becouse its more ez to type hehe
            for line in f:
                # This will skip comments and empty lines
                if line.strip() and not line.startswith('#'):
                    # Split each line into key/value pairs
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
        print("Environment variables loaded successfully!")
    except FileNotFoundError:
        print(f"No .env file found at {file_path} - using system environment variables")
    except Exception as e:
        print(f"Error loading .env file: {e}")

def xml_to_sql(xml_path, table_name):
    """
    Main function to process XML and upload to SQL Server
    Parameters:
    xml_path (str): Path to XML file
    table_name (str): Name of SQL table to create/update
    """

    # Load environment variables
    load_env()
    
    # Get database credentials from environment from the .env file
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    
    # It will read the XML file and see if everything is good 
    # And it will print the first 5 lines of the XML file using "df.head"
    if os.path.exists(xml_path):
        print(f"\nReading XML file: {xml_path}")
        df = pd.read_xml(xml_path)
        print("XML data preview:")
        print(df.head())
    # If something is wrong its gonna appear this error
    else:
        print(f"\nError: XML file not found at {xml_path}")
        return

    # Create SQL connection string

    conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};"
    
    # Add authentication method
    if username and password:
        conn_str += f"UID={username};PWD={password};"
        auth_type = "SQL Server Authentication"
    else:
        conn_str += "Trusted_Connection=yes;"
        auth_type = "Windows Authentication"
    
    print(f"\nConnecting using: {auth_type}")

    # Connect and upload data
    try:
        # Create database connection
        # It will use the pyodbc library to conect to a sql server DataBase
        with pyodbc.connect(conn_str) as conn:
            print("Connection successful! Uploading data...")
            
            # Use pandas to_sql for easy upload
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists='replace',  # Options: 'fail', 'replace', 'append'
                index=False
            )
            print(f"\nSuccess! {len(df)} rows uploaded to {table_name}")
            
    except Exception as e:
        print(f"\nDatabase error: {e}")

# Run the script
if __name__ == "__main__":
    # Configuration 
    XML_FILE = "data.xml"      # Path to XML file
    SQL_TABLE = "xml_data"     # Name for SQL table
    
    xml_to_sql(XML_FILE, SQL_TABLE)