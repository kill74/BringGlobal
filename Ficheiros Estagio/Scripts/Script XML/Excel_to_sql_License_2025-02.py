import pandas as pd
import pyodbc
import logging

# Configure logging to track execution and errors
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_sql():
    """Establishes a connection to SQL Server."""
    try:
        # Define connection string for SQL Server (Windows authentication)
        connection_string = 'DRIVER={SQL Server};SERVER=localhost,1433;DATABASE=master;Trusted_Connection=yes;'
        conn = pyodbc.connect(connection_string)
        logging.info("Connected to SQL Server")
        return conn
    except pyodbc.Error as e:
        # Log error and raise exception if connection fails
        logging.error(f"Failed to connect to SQL Server: {e}")
        raise

def create_table_if_not_exists(table_name, conn):
    """Creates the FACT_ASSETS table if it doesn't exist, based on the provided diagram."""
    cursor = conn.cursor()
    # SQL statement to create FACT_ASSETS table with columns matching the diagram
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
    BEGIN
        CREATE TABLE {table_name} (
            assets_id INT PRIMARY KEY,           -- Primary key for unique asset identification
            prdct_id INT,                        -- Foreign key for product (placeholder for now)
            loc_id INT,                          -- Foreign key for location (placeholder for now)
            brng_id INT,                         -- Foreign key linking to DIM_EMPL
            id_prjt INT,                         -- Foreign key for project (placeholder for now)
            id_date INT,                         -- Foreign key for date (placeholder for now)
            asset_cost DECIMAL(18,2),            -- Cost of the asset with 2 decimal places
            asset_start_date DATE,               -- Start date of the asset
            asset_end_date DATE,                 -- End date of the asset
            FOREIGN KEY (brng_id) REFERENCES DIM_EMPL(brng_id)  -- Enforce referential integrity
        )
    END
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    logging.info(f"Table {table_name} checked/created")

def import_excel_to_sql(excel_file, table_name):
    """Imports data from an Excel file into the FACT_ASSETS table."""
    # Read the Excel file into a pandas DataFrame
    df = pd.read_excel(excel_file)
    
    # Rename columns to match the FACT_ASSETS table structure
    df = df.rename(columns={
        "Bring ID": "brng_id",                  # Maps to brng_id (FK to DIM_EMPL)
        "License value (EUR)": "asset_cost",    # Maps to asset_cost
        "StartDate": "asset_start_date",        # Maps to asset_start_date
        "EndDate": "asset_end_date"             # Maps to asset_end_date
    })
    
    # Generate unique assets_id for each row (since not provided in Excel)
    df["assets_id"] = range(1, len(df) + 1)
    df["prdct_id"] = 0  
    df["loc_id"] = 0    
    df["id_prjt"] = 0   
    df["id_date"] = 0   
    
    # Select and order columns to match the table schema
    df = df[["assets_id", "prdct_id", "loc_id", "brng_id", "id_prjt", "id_date", 
             "asset_cost", "asset_start_date", "asset_end_date"]]
    
    # Convert data types to match SQL Server schema
    df["assets_id"] = pd.to_numeric(df["assets_id"], errors='coerce')      
    df["prdct_id"] = pd.to_numeric(df["prdct_id"], errors='coerce')       
    df["loc_id"] = pd.to_numeric(df["loc_id"], errors='coerce')            
    df["brng_id"] = pd.to_numeric(df["brng_id"], errors='coerce')          
    df["id_prjt"] = pd.to_numeric(df["id_prjt"], errors='coerce')         
    df["id_date"] = pd.to_numeric(df["id_date"], errors='coerce')         
    df["asset_cost"] = pd.to_numeric(df["asset_cost"], errors='coerce')    
    df["asset_start_date"] = pd.to_datetime(df["asset_start_date"], errors='coerce')  
    df["asset_end_date"] = pd.to_datetime(df["asset_end_date"], errors='coerce')     
    
    # Connect to SQL Server and create table if it doesn't exist
    conn = connect_to_sql()
    create_table_if_not_exists(table_name, conn)
    cursor = conn.cursor()
    
    # Prepare data for bulk insertion using executemany
    values = [tuple(row) for _, row in df.iterrows()]
    placeholders = ', '.join(['?' for _ in df.columns])  # Create placeholders for SQL query
    sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
    
    try:
        # Perform bulk insertion for better performance
        cursor.executemany(sql, values)
        conn.commit()
        logging.info(f"Data imported into table {table_name}")
    except pyodbc.Error as e:
        # Log any errors during insertion
        logging.error(f"Error importing data: {e}")
    
    # Clean up resources
    cursor.close()
    conn.close()

def main():
    """Main function to execute the import process."""
    excel_file = "2025-02 - License split.xlsx"  # Path to the Excel file
    table_name = "FACT_ASSETS"                   # Target table name
    import_excel_to_sql(excel_file, table_name)  # Run the import process

if __name__ == "__main__":
    main()  # Entry point of the script