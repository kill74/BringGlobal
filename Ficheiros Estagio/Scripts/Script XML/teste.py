import xml.etree.ElementTree as ET
import pyodbc
import pandas as pd
import os
from typing import Dict, List, Optional, Any, Union


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration settings from an XML file.
    
    Args:
        config_file: Path to the XML configuration file
        
    Returns:
        Dictionary containing all configuration parameters
    """
    tree = ET.parse(config_file)
    root = tree.getroot()

    # Common database configuration
    config = {
        "server": root.find("./database/server").text,
        "port": root.find("./database/port").text,
        "database": root.find("./database/database_name").text,
        "trusted_connection": root.find("./database/trusted_connection").text.lower() == "yes",
        "table_name": root.find("./database/table").attrib["name"],
        "columns": [],
        "source_type": "unknown"
    }

    # Detect source type (XML or Excel)
    if root.find("./xml") is not None:
        config["source_type"] = "xml"
        config["namespace"] = root.find("./xml/namespace").attrib.get("uri", "")
        config["root_path"] = root.find("./xml/root_path").text
        config["file_path"] = root.find("./xml/file_path").text
    elif root.find("./excel") is not None:
        config["source_type"] = "excel"
        config["file_path"] = root.find("./excel/file_path").text
        config["sheet_name"] = root.find("./excel/sheet_name").text

    # Load column definitions
    for col in root.findall("./database/table/columns/column"):
        column_config = {
            "name": col.attrib["name"],
            "type": col.attrib["type"],
            "default": col.attrib.get("default", None)
        }
        
        # XML-specific attributes
        if config["source_type"] == "xml":
            column_config["xpath"] = col.attrib.get("xpath", "")
            column_config["attribute"] = col.attrib.get("attribute", "")
        # Excel-specific attributes
        elif config["source_type"] == "excel":
            column_config["source_name"] = col.attrib.get("source_name", col.attrib["name"])
            
        config["columns"].append(column_config)

    return config


def connect_to_sql(config: Dict[str, Any]) -> pyodbc.Connection:
    """
    Establish connection to SQL Server database.
    
    Args:
        config: Database configuration dictionary
        
    Returns:
        Active database connection
    """
    connection_string = f"DRIVER={{SQL Server}};SERVER={config['server']},{config['port']};DATABASE={config['database']};"
    if config["trusted_connection"]:
        connection_string += "Trusted_Connection=yes;"
    return pyodbc.connect(connection_string)


def create_table_if_not_exists(config: Dict[str, Any], conn: pyodbc.Connection) -> None:
    """
    Create database table if it doesn't already exist.
    
    Args:
        config: Configuration with table and column definitions
        conn: Database connection
    """
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


def parse_xml_to_dataframe(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Parse XML file into a pandas DataFrame based on configuration.
    
    Args:
        config: XML configuration with namespace and xpath mappings
        
    Returns:
        DataFrame containing extracted data
    """
    tree = ET.parse(config["file_path"])
    root = tree.getroot()
    namespace = {"ns": config["namespace"]} if config["namespace"] else {}

    data = []
    for item in root.findall(config["root_path"], namespace):
        row = {}
        for col in config["columns"]:
            if "xpath" not in col:
                continue
                
            elem = item.find(col["xpath"], namespace)
            
            # Get value based on configuration
            if elem is not None:
                if col.get("attribute"):
                    value = elem.attrib.get(col["attribute"])
                else:
                    value = elem.text
            else:
                value = col.get("default")

            # Apply type conversion for numeric types
            if "DECIMAL" in col["type"] or "FLOAT" in col["type"] or "NUMERIC" in col["type"]:
                try:
                    value = float(value) if value not in [None, "", "NaN"] else float(col.get("default", 0.00))
                except (ValueError, TypeError):
                    print(f"Warning: Error converting '{value}' to float in column {col['name']}. Using default {col.get('default', 0.00)}")
                    value = float(col.get("default", 0.00))
            
            row[col["name"]] = value
        data.append(row)

    return pd.DataFrame(data)


def normalize_name(name: str) -> str:
    """
    Normalize column names by removing spaces, accents, and symbols.
    
    Args:
        name: Original column name
        
    Returns:
        Normalized column name
    """
    if not isinstance(name, str):
        return str(name)
        
    replacements = {
        " ": "_", "-": "_", "(": "", ")": "", "º": "", "ç": "c", "é": "e", "á": "a",
        "ó": "o", "ã": "a", "ú": "u", "í": "i", "â": "a", "ê": "e", "ô": "o"
    }
    result = name
    for old, new in replacements.items():
        result = result.replace(old, new)
    return result.strip()


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize all column names in a DataFrame.
    
    Args:
        df: DataFrame with original column names
        
    Returns:
        DataFrame with normalized column names
    """
    df.columns = [normalize_name(col) for col in df.columns]
    return df


def find_column(df: pd.DataFrame, source_name: str) -> Optional[str]:
    """
    Find column in DataFrame that matches the source name after normalization.
    
    Args:
        df: DataFrame to search in
        source_name: Column name to find
        
    Returns:
        Actual column name if found, None otherwise
    """
    normalized_df_cols = {normalize_name(col): col for col in df.columns}
    normalized_source = normalize_name(source_name)
    return normalized_df_cols.get(normalized_source)


def parse_excel_to_dataframe(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Parse Excel file into a DataFrame based on configuration.
    
    Args:
        config: Excel configuration with file path and column mappings
        
    Returns:
        DataFrame containing extracted data
    """
    if not os.path.exists(config['file_path']):
        raise FileNotFoundError(f"File not found: {config['file_path']}")

    # Try reading the Excel file with different header rows
    df = None
    for skip_rows in range(0, 5):
        try:
            df = pd.read_excel(config['file_path'], sheet_name=config.get('sheet_name', 0), 
                              skiprows=skip_rows, dtype=str, engine="openpyxl")
            df = normalize_column_names(df)
            if len(df.columns) > 0 and not all(col.startswith('Unnamed') for col in df.columns):
                break  # Found valid columns
        except Exception as e:
            continue

    if df is None or len(df.columns) == 0:
        raise ValueError(f"Could not read valid data from Excel file: {config['file_path']}")

    # Map Excel columns to SQL column names
    selected_columns = {}
    for col in config["columns"]:
        if "source_name" not in col:
            continue
            
        found_col = find_column(df, col["source_name"])
        if found_col:
            selected_columns[col["name"]] = found_col

    # Create and fill output DataFrame with mapped columns
    result_df = pd.DataFrame()
    for col in config["columns"]:
        col_name = col["name"]
        if col_name in selected_columns:
            result_df[col_name] = df[selected_columns[col_name]]
        else:
            print(f"Warning: Column '{col_name}' not found. Creating with default value.")
            result_df[col_name] = col.get("default", "")

    # Apply type conversions
    for col in config["columns"]:
        if "DECIMAL" in col["type"].upper() or "FLOAT" in col["type"].upper() or "NUMERIC" in col["type"].upper():
            result_df[col["name"]] = pd.to_numeric(result_df[col["name"]], errors="coerce").fillna(float(col.get("default", 0)))
        else:
            result_df[col["name"]] = result_df[col["name"]].fillna(col.get("default", "")).astype(str)

    return result_df


def import_data_to_sql(df: pd.DataFrame, config: Dict[str, Any]) -> None:
    """
    Import DataFrame data into SQL Server table.
    
    Args:
        df: DataFrame containing data to import
        config: Database configuration
    """
    conn = connect_to_sql(config)
    create_table_if_not_exists(config, conn)
    
    cursor = conn.cursor()
    
    # Prepare SQL INSERT statement
    column_names = [col["name"] for col in config["columns"]]
    placeholders = ', '.join(['?'] * len(column_names))
    sql = f"INSERT INTO {config['table_name']} ({', '.join(column_names)}) VALUES ({placeholders})"
    
    # Insert row by row
    rows_inserted = 0
    for _, row in df.iterrows():
        values = tuple(row[col_name] for col_name in column_names)
        try:
            cursor.execute(sql, values)
            rows_inserted += 1
        except pyodbc.Error as e:
            print(f"Error inserting row: {e}")
            
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Successfully imported {rows_inserted} rows into table '{config['table_name']}'")


def import_file_to_sql(config_file: str) -> None:
    """
    Main function to import data from XML or Excel to SQL Server.
    
    Args:
        config_file: Path to XML configuration file
    """
    # Load configuration
    config = load_config(config_file)
    
    # Process based on source type
    if config["source_type"] == "xml":
        print(f"Importing XML file: {config['file_path']}")
        df = parse_xml_to_dataframe(config)
    elif config["source_type"] == "excel":
        print(f"Importing Excel file: {config['file_path']}")
        df = parse_excel_to_dataframe(config)
    else:
        raise ValueError(f"Unsupported source type: {config['source_type']}")
        
    # Import to SQL
    import_data_to_sql(df, config)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        # Default configuration file
        config_file = input("P1_DataSol_SalEspecificacoes.xml")
        
    import_file_to_sql(config_file)