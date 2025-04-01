# Import required libraries
import pandas as pd
import os

# Helper function to load environment variables from .env file
def load_env(file_path='.env'):
    """
    Reads a .env file and sets environment variables
    Parameters:
    file_path (str): Path to .env file (default: current directory)
    """
    try:
        with open(file_path) as f:
            for line in f:
                # Skip comments and empty lines
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
    # 1. Load environment variables
    load_env()
    
    # 2. Get database credentials from environment
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    
    # 3. Read XML file
    try:
        print(f"\nReading XML file: {xml_path}")
        df = pd.read_xml(xml_path)
        print("XML data preview:")
        print(df.head())
    except Exception as e:
        print(f"\nError reading XML: {e}")
        return

    # 4. Create SQL connection string
    conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};"
    
    # Add authentication method
    if username and password:
        conn_str += f"UID={username};PWD={password};"
        auth_type = "SQL Server Authentication"
    else:
        conn_str += "Trusted_Connection=yes;"
        auth_type = "Windows Authentication"
    
    print(f"\nConnecting using: {auth_type}")

    # 5. Connect and upload data
    try:
        # Create database connection
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
    # Configuration (modify these as needed)
    XML_FILE = "data.xml"      # Path to your XML file
    SQL_TABLE = "xml_data"     # Name for your SQL table
    
    xml_to_sql(XML_FILE, SQL_TABLE)