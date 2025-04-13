import pandas as pd
import pyodbc
import os
import xml.etree.ElementTree as ET
import logging
from glob import glob

# Setup de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('universal_excel_to_sql.log'),
        logging.StreamHandler()
    ]
)

def normalize_name(name):
    if not isinstance(name, str):
        return str(name)
    replacements = {
        " ": "_", "-": "_", "(": "", ")": "", "º": "", "ç": "c", "é": "e", "á": "a",
        "ó": "o", "ã": "a", "ú": "u", "í": "i", "â": "a", "ê": "e", "ô": "o"
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    return name.strip().lower()

def normalize_column_names(df):
    df.columns = [normalize_name(col) for col in df.columns]
    return df

def load_config(config_file):
    try:
        tree = ET.parse(config_file)
        root = tree.getroot()
        config = {
            "server": root.find("./database/server").text,
            "port": root.find("./database/port").text,
            "database": root.find("./database/database_name").text,
            "trusted_connection": root.find("./database/trusted_connection").text.lower() == "yes",
            "table_name": root.find("./database/table").attrib["name"],
            "excel_file": root.find("./excel/file_path").text,
            "sheet_name": root.find("./excel/sheet_name").text,
            "skip_rows": int(root.find("./excel/skip_rows").text) if root.find("./excel/skip_rows") is not None else None,
            "columns": []
        }
        for col in root.findall("./database/table/columns/column"):
            config["columns"].append({
                "name": col.attrib["name"],
                "type": col.attrib["type"],
                "source_name": col.attrib["source_name"],
                "default": col.attrib.get("default", None)
            })
        return config
    except Exception as e:
        logging.error(f"Erro ao carregar config {config_file}: {e}")
        raise

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

def find_column(df, source_name):
    normalized_df_cols = {normalize_name(col): col for col in df.columns}
    normalized_source = normalize_name(source_name)
    return normalized_df_cols.get(normalized_source)

def validate_headers(df, config):
    expected = [normalize_name(col['source_name']) for col in config['columns']]
    actual = [normalize_name(col) for col in df.columns]
    return set(expected).issubset(set(actual))

def read_excel_with_fallback(config):
    if not os.path.exists(config['excel_file']):
        raise FileNotFoundError(f"Ficheiro não encontrado: {config['excel_file']}")

    if config.get("skip_rows") is not None:
        try:
            df = pd.read_excel(
                config['excel_file'],
                sheet_name=config['sheet_name'],
                skiprows=config['skip_rows'],
                dtype=str,
                engine="openpyxl"
            )
            df = normalize_column_names(df)
            if validate_headers(df, config):
                return df
        except Exception as e:
            logging.warning(f"Erro leitura com skip_rows: {e}")

    all_data = pd.read_excel(
        config['excel_file'], sheet_name=config['sheet_name'],
        header=None, dtype=str, engine="openpyxl"
    )
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
        return normalize_column_names(df)
    else:
        raise ValueError("Não foi possível identificar cabeçalhos válidos no Excel")

def import_excel_to_sql(config):
    logging.info(f"📂 A processar: {os.path.basename(config['excel_file'])}")
    df = read_excel_with_fallback(config)
    
    selected_columns = {}
    for col in config["columns"]:
        found_col = find_column(df, col["source_name"])
        if found_col:
            selected_columns[col["name"]] = found_col
        else:
            logging.warning(f"⚠️ Coluna '{col['source_name']}' não encontrada. Usando default.")

    for col in config["columns"]:
        col_name = col["name"]
        if col_name in selected_columns:
            df[col_name] = df[selected_columns[col_name]]
        else:
            df[col_name] = col.get("default", "")

    df = df[[col["name"] for col in config["columns"]]]

    for col in config["columns"]:
        if "DECIMAL" in col["type"].upper() or "FLOAT" in col["type"].upper():
            df[col["name"]] = pd.to_numeric(df[col["name"]], errors="coerce").fillna(float(col.get("default", 0)))
        elif "INT" in col["type"].upper():
            df[col["name"]] = pd.to_numeric(df[col["name"]], errors="coerce").fillna(int(col.get("default", 0)))
        else:
            df[col["name"]] = df[col["name"]].fillna(col.get("default", "")).astype(str)

    conn = connect_to_sql(config)
    create_table_if_not_exists(config, conn)
    cursor = conn.cursor()

    sql = f"INSERT INTO {config['table_name']} ({', '.join(df.columns)}) VALUES ({', '.join(['?'] * len(df.columns))})"

    success_count = 0
    for idx, row in df.iterrows():
        try:
            cursor.execute(sql, tuple(row))
            success_count += 1
        except Exception as e:
            logging.warning(f"Erro linha {idx + 1}: {e}")
            conn.rollback()
    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"✅ {success_count}/{len(df)} linhas inseridas em '{config['table_name']}'")

# --- Ponto de entrada ---
if __name__ == "__main__":
    config_files = glob("devices.xml", recursive=True)
    if not config_files:
        logging.error("Nenhum ficheiro XML de configuração encontrado.")
        exit(1)

    for xml_file in config_files:
        try:
            config = load_config(xml_file)
            import_excel_to_sql(config)
        except Exception as e:
            logging.error(f"Erro ao processar '{xml_file}': {e}")
