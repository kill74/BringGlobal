import xml.etree.ElementTree as ET
import pyodbc
import pandas as pd
import os
import glob

def load_config(config_file="config.xml"):
    """Loads configuration from XML file"""
    tree = ET.parse(config_file)
    root = tree.getroot()

    config = {
        "server": root.find("./database/server").text,
        "port": root.find("./database/port").text,
        "database": root.find("./database/database_name").text,
        "trusted_connection": root.find("./database/trusted_connection").text.lower() == "yes",
        "table_name": root.find("./database/table").attrib["name"],
        "columns": [],
        "excel_path": root.find("./excel/path").text
    }

    # Handle sheet name if specified (optional)
    sheet_name_element = root.find("./excel/sheet_name")
    config["excel_sheet_name"] = sheet_name_element.text if sheet_name_element is not None else 0

    for col in root.findall("./database/table/columns/column"):
        config["columns"].append({
            "name": col.attrib["name"],
            "type": col.attrib["type"],
            "excel_column": col.attrib["excel_column"],
            "default": col.attrib.get("default", None)
        })

    return config

def connect_to_sql(config):
    """Connects to SQL Server"""
    connection_string = f"DRIVER={{SQL Server}};SERVER={config['server']},{config['port']};DATABASE={config['database']};"
    if config["trusted_connection"]:
        connection_string += "Trusted_Connection=yes;"
    return pyodbc.connect(connection_string)

def create_table_if_not_exists(config, conn):
    """Creates table in SQL Server according to configuration"""
    cursor = conn.cursor()
    
    column_definitions = ", ".join([f"{col['name']} {col['type']}" for col in config["columns"]])
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{config['table_name']}')
    BEGIN
        CREATE TABLE {config['table_name']} ({column_definitions})
    END
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()

def read_excel_to_dataframe(excel_file, config):
    """Reads Excel file and transforms to DataFrame using configuration"""
    try:
        print(f"Reading file: {excel_file}")
        
        # Read the Excel file
        df = pd.read_excel(
            excel_file, 
            sheet_name=config["excel_sheet_name"]
        )
        
        # Create a new DataFrame with only the configured columns
        result_df = pd.DataFrame()
        
        for col in config["columns"]:
            excel_col = col["excel_column"]
            sql_col = col["name"]
            default_value = col["default"]
            
            # Check if the Excel column exists
            if excel_col in df.columns:
                result_df[sql_col] = df[excel_col]
            else:
                print(f"Warning: Column '{excel_col}' not found in Excel file '{os.path.basename(excel_file)}'. Using default value.")
                result_df[sql_col] = default_value
                
            # Apply data type conversions
            if "DECIMAL" in col["type"] and default_value is not None:
                try:
                    result_df[sql_col] = pd.to_numeric(result_df[sql_col], errors='coerce')
                    result_df[sql_col] = result_df[sql_col].fillna(float(default_value))
                except (ValueError, TypeError):
                    print(f"Error converting values in column {sql_col} to numeric. Using default {default_value}")
                    result_df[sql_col] = float(default_value)
                    
        return result_df
        
    except Exception as e:
        print(f"Error reading Excel file '{excel_file}': {str(e)}")
        return None

def import_dataframe_to_sql(df, config, conn):
    """Imports DataFrame to SQL Server"""
    if df is None or df.empty:
        print("No data to import.")
        return 0, 0
    
    cursor = conn.cursor()
    
    # Handle default values for empty cells
    for col in config["columns"]:
        default_value = col.get("default")
        if default_value is not None:
            if "DECIMAL" in col["type"]:
                try:
                    default_value = float(default_value)
                except (ValueError, TypeError):
                    default_value = 0.00
            df[col["name"]] = df[col["name"]].fillna(default_value)
        else:
            df[col["name"]] = df[col["name"]].fillna("N/A")
    
    # Insert rows one by one
    rows_inserted = 0
    rows_failed = 0
    
    for _, row in df.iterrows():
        values = tuple(row)
        placeholders = ', '.join(['?' for _ in values])
        column_names = ', '.join([col for col in df.columns])
        sql = f"INSERT INTO {config['table_name']} ({column_names}) VALUES ({placeholders})"
        
        try:
            cursor.execute(sql, values)
            rows_inserted += 1
        except pyodbc.Error as e:
            print(f"Error inserting row: {str(e)}")
            print(f"Data: {values}")
            rows_failed += 1
    
    conn.commit()
    return rows_inserted, rows_failed

def import_excel_to_sql(config):
    """Imports data from Excel file to SQL Server"""
    try:
        excel_path = config["excel_path"]
        
        # Check if path exists
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel path not found: {excel_path}")
        
        # Connect to database
        conn = connect_to_sql(config)
        
        # Create table if it doesn't exist
        create_table_if_not_exists(config, conn)
        
        # Process path based on whether it's a file or directory
        if os.path.isfile(excel_path):
            # Process single file
            df = read_excel_to_dataframe(excel_path, config)
            rows_inserted, rows_failed = import_dataframe_to_sql(df, config, conn)
            print(f"Processed file: {rows_inserted} rows inserted, {rows_failed} rows failed")
        
        elif os.path.isdir(excel_path):
            # Process all Excel files in directory
            excel_files = glob.glob(os.path.join(excel_path, "*.xlsx")) + glob.glob(os.path.join(excel_path, "*.xls"))
            
            if not excel_files:
                print(f"No Excel files found in directory: {excel_path}")
                conn.close()
                return
                
            print(f"Found {len(excel_files)} Excel file(s) to process")
            
            total_rows_inserted = 0
            total_rows_failed = 0
            files_processed = 0
            files_failed = 0
            
            for excel_file in excel_files:
                try:
                    df = read_excel_to_dataframe(excel_file, config)
                    
                    if df is not None:
                        rows_inserted, rows_failed = import_dataframe_to_sql(df, config, conn)
                        
                        total_rows_inserted += rows_inserted
                        total_rows_failed += rows_failed
                        files_processed += 1
                        
                        print(f"Processed '{os.path.basename(excel_file)}': {rows_inserted} rows inserted, {rows_failed} rows failed")
                    else:
                        files_failed += 1
                except Exception as e:
                    print(f"Failed to process file '{excel_file}': {str(e)}")
                    files_failed += 1
            
            print("\nImport summary:")
            print(f"Files processed successfully: {files_processed}")
            print(f"Files failed: {files_failed}")
            print(f"Total rows inserted: {total_rows_inserted}")
            print(f"Total rows failed: {total_rows_failed}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error during import process: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        config = load_config("config.xml")
        import_excel_to_sql(config)
        print("Process completed successfully.")
    except Exception as e:
        print(f"Process failed: {str(e)}")