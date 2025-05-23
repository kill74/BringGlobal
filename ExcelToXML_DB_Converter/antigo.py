import pandas as pd
import pyodbc
import os
import xml.etree.ElementTree as ET
import logging
import argparse
from glob import glob
from datetime import datetime  # <- Importante para a Data_Hora

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def normalize_name(name):
    if not isinstance(name, str):
        return str(name)
    replacements = {
        " ": "_", "-": "_", "(": "", ")": "", "º": "", "ç": "c", "é": "e", "á": "a",
        "ó": "o", "ã": "a", "ú": "u", "í": "i", "â": "a", "ê": "e", "ô": "o"
    }
    name = name.strip()
    for old, new in replacements.items():
        name = name.replace(old, new)
    return name.lower()

def make_columns_unique(df):
    counts = {}
    new_columns = []
    for col in df.columns:
        if col in counts:
            counts[col] += 1
            new_columns.append(f"{col}.{counts[col]}")
        else:
            counts[col] = 0
            new_columns.append(col)
    df.columns = new_columns
    return df

def normalize_column_names(df):
    seen = {}
    new_cols = []
    for col in df.columns:
        norm_col = normalize_name(str(col).strip())
        if norm_col in seen:
            seen[norm_col] += 1
            norm_col = f"{norm_col}.{seen[norm_col]}"
        else:
            seen[norm_col] = 0
        new_cols.append(norm_col)
    df.columns = new_cols
    return df

def connect_to_sql(config):
    conn_str = f"DRIVER={{SQL Server}};SERVER={config['server']},{config['port']};DATABASE={config['database']};"
    if config["trusted_connection"]:
        conn_str += "Trusted_Connection=yes;"
    return pyodbc.connect(conn_str)

def create_table_if_not_exists(config, conn):
    cursor = conn.cursor()
    col_defs = ", ".join([f"{col['name']} {col['type']}" for col in config["columns"]])
    create_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{config['table_name']}')
    BEGIN
        CREATE TABLE {config['table_name']} ({col_defs})
    END
    """
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()

def load_config(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    config = {
        "server": root.find("./database/server").text,
        "port": root.find("./database/port").text,
        "database": root.find("./database/database_name").text,
        "trusted_connection": root.find("./database/trusted_connection").text.lower() == "yes",
        "table_name": root.find("./database/table").attrib["name"],
        "columns": []
    }
    for col in root.findall("./database/table/columns/column"):
        config["columns"].append({
            "name": col.attrib["name"],
            "type": col.attrib["type"],
            "xpath": col.attrib.get("xpath"),
            "attribute": col.attrib.get("attribute"),
            "source_name": col.attrib.get("source_name"),
            "default": col.attrib.get("default", None)
        })

    if root.find("./excel") is not None:
        config["type"] = "excel"
        excel_path = root.find("./excel/file_path").text
        if not excel_path or excel_path.lower() == "ask":
            excel_path = input("Insere o caminho do ficheiro Excel: ").strip()
        config["excel_file"] = excel_path
        config["sheet_name"] = root.find("./excel/sheet_name").text
        config["skip_rows"] = int(root.find("./excel/skip_rows").text) if root.find("./excel/skip_rows") is not None else None

    elif root.find("./xml") is not None:
        config["type"] = "xml"
        config["namespace"] = root.find("./xml/namespace").attrib["uri"]
        config["root_path"] = root.find("./xml/root_path").text
        xml_path = root.find("./xml/file_path").text
        if not xml_path or xml_path.lower() == "ask":
            xml_path = input("Insere o caminho do ficheiro XML: ").strip()
        config["file_path"] = xml_path

    else:
        raise ValueError("File type not specified correctly (expected <excel> or <xml>)")

    return config

def find_column(df, source_name):
    normalized_df_cols = {normalize_name(str(col).strip()): col for col in df.columns}
    normalized_source = normalize_name(source_name)
    return normalized_df_cols.get(normalized_source)

def validate_headers(df, config):
    expected = [normalize_name(col['source_name']) for col in config['columns']]
    actual = [normalize_name(col) for col in df.columns]
    return set(expected).issubset(set(actual))

def read_excel_with_fallback(config):
    if not os.path.exists(config['excel_file']):
        raise FileNotFoundError(f"File not found: {config['excel_file']}")

    if config.get("skip_rows") is not None:
        try:
            df = pd.read_excel(config['excel_file'], sheet_name=config['sheet_name'], skiprows=config['skip_rows'], dtype=str, engine="openpyxl")
            df = make_columns_unique(df)
            df = normalize_column_names(df)
            if validate_headers(df, config):
                return df
        except Exception as e:
            logging.warning(f"Error reading with skip_rows: {e}")

    all_data = pd.read_excel(config['excel_file'], sheet_name=config['sheet_name'], header=None, dtype=str, engine="openpyxl")
    expected = [normalize_name(col['source_name']) for col in config['columns']]
    best_match = {'idx': 0, 'matches': 0}

    for idx, row in all_data.iterrows():
        matches = sum(1 for col in expected if normalize_name(str(col)) in [normalize_name(str(cell)) for cell in row])
        if matches > best_match['matches']:
            best_match = {'idx': idx, 'matches': matches}
            if matches == len(expected):
                break

    if best_match['matches'] > 0:
        df = pd.read_excel(config['excel_file'], sheet_name=config['sheet_name'], skiprows=best_match['idx'], dtype=str, engine="openpyxl")
        df = make_columns_unique(df)
        df = normalize_column_names(df)
        return df
    else:
        raise ValueError("Could not identify valid headers in Excel")

def parse_xml_to_dataframe(config):
    tree = ET.parse(config["file_path"])
    root = tree.getroot()
    namespace = {"ns": config["namespace"]}
    data = []
    for item in root.findall(config["root_path"], namespace):
        row = {}
        for col in config["columns"]:
            elem = item.find(col["xpath"], namespace)
            value = None
            if elem is not None:
                value = elem.attrib.get(col["attribute"]) if col["attribute"] else elem.text
            if value is None or value == "":
                value = col.get("default", None)
            row[col["name"]] = value.strip() if isinstance(value, str) else value
        data.append(row)
    return pd.DataFrame(data)

def clean_and_cast_dataframe(df, config):
    for col in config["columns"]:
        col_name = col["name"]
        default_value = col.get("default")

        if col_name in df.columns and df[col_name].dtype == object:
            df[col_name] = df[col_name].astype(str).apply(lambda x: ' '.join(x.split()))

        if "DECIMAL" in col["type"].upper():
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            default_value = float(default_value) if default_value is not None else 0.00
            df[col_name] = df[col_name].fillna(default_value)

        elif "INT" in col["type"].upper():
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce", downcast="integer")
            default_value = int(default_value) if default_value is not None else 0
            df[col_name] = df[col_name].fillna(default_value)

        elif "DATE" in col["type"].upper():
            df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
            df[col_name] = df[col_name].fillna(pd.Timestamp.now())

        else:
            default_value = str(default_value) if default_value is not None else "N/A"
            df[col_name] = df[col_name].fillna(default_value).astype(str)

    return df

def import_to_sql(df, config):
    conn = connect_to_sql(config)
    create_table_if_not_exists(config, conn)
    cursor = conn.cursor()
    placeholders = ', '.join(['?'] * len(df.columns))
    sql = f"INSERT INTO {config['table_name']} ({', '.join(df.columns)}) VALUES ({placeholders})"
    success = 0
    for idx, row in df.iterrows():
        try:
            cursor.execute(sql, tuple(row))
            success += 1
        except Exception as e:
            logging.warning(f"Error on row {idx + 1}: {e}")
            conn.rollback()
    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"{success}/{len(df)} rows inserted into '{config['table_name']}'")

def process_config(config):
    if config["type"] == "excel":
        logging.info(f" Processing Excel file: {os.path.basename(config['excel_file'])}")
        df = read_excel_with_fallback(config)
        selected_columns = {}
        for col in config["columns"]:
            found_col = find_column(df, col["source_name"])
            if found_col:
                selected_columns[col["name"]] = found_col
            else:
                logging.warning(f"Column '{col['source_name']}' not found. Using default.")
        for col in config["columns"]:
            col_name = col["name"]
            if col_name in selected_columns:
                df[col_name] = df[selected_columns[col_name]]
            else:
                df[col_name] = col.get("default", "")
        df = df[[col["name"] for col in config["columns"] if col["name"] in df.columns]]
        
        # ✨ ADICIONA A COLUNA DE DATA/HORA AQUI:
        df["Data_Hora"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df = clean_and_cast_dataframe(df, config)
        import_to_sql(df, config)

    elif config["type"] == "xml":
        logging.info(f"Processing XML file: {os.path.basename(config['file_path'])}")
        df = parse_xml_to_dataframe(config)

        # ✨ ADICIONA A COLUNA DE DATA/HORA AQUI TAMBÉM:
        df["Data_Hora"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df = clean_and_cast_dataframe(df, config)
        import_to_sql(df, config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Importa dados de ficheiros Excel/XML para base de dados SQL.")
    parser.add_argument("config_file", nargs="?", help="Caminho para o ficheiro de configuração XML")
    args = parser.parse_args()

    if not args.config_file:
        args.config_file = input("Insere o caminho do ficheiro de configuração XML: ").strip()

    if not os.path.exists(args.config_file):
        logging.error(f"Ficheiro de configuração não encontrado: {args.config_file}")
        exit(1)

    try:
        config = load_config(args.config_file)
        process_config(config)
    except Exception as e:
        logging.error(f"Erro ao processar '{args.config_file}': {e}")
