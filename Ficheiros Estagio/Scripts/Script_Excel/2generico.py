import xml.etree.ElementTree as ET
import pyodbc
import pandas as pd

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
        "excel_file_path": root.find("./excel/file_path").text,
        "excel_sheet_name": root.find("./excel/sheet_name").text
    }

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

def read_excel_to_dataframe(config):
    """Reads Excel file and transforms to DataFrame using configuration"""
    try:
        # Read the Excel file
        df = pd.read_excel(
            config["excel_file_path"], 
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
                print(f"Warning: Column '{excel_col}' not found in Excel file. Using default value.")
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
        print(f"Error reading Excel file: {str(e)}")
        raise

def import_excel_to_sql(config):
    """Imports data from Excel to SQL Server"""
    try:
        # Read Excel into DataFrame
        df = read_excel_to_dataframe(config)
        
        # Connect to database
        conn = connect_to_sql(config)
        
        # Create table if it doesn't exist
        create_table_if_not_exists(config, conn)
        
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
        cursor.close()
        conn.close()
        
        print(f"Import complete. {rows_inserted} rows inserted successfully. {rows_failed} rows failed.")
        
    except Exception as e:
        print(f"Error during import process: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        config = load_config("genericoexcelContribuicoes.xml")
        import_excel_to_sql(config)
        print("Process completed successfully.")
    except Exception as e:
        print(f"Process failed: {str(e)}")