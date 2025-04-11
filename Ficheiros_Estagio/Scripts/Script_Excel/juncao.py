import pandas as pd
import pyodbc
import os
import xml.etree.ElementTree as ET
import logging

# Setup de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('excel_to_sql_import.log'),
        logging.StreamHandler()
    ]
)

def normalize_name(name):
    """Normaliza nome removendo espaços, acentos e símbolos."""
    if not isinstance(name, str):
        return str(name)

    replacements = {
        " ": "_", "-": "_", "(": "", ")": "", "º": "", "ª": "", "ç": "c", "é": "e", "á": "a",
        "ó": "o", "ã": "a", "ú": "u", "í": "i", "â": "a", "ê": "e", "ô": "o", "ñ": "n", "ü": "u"
    }
    name = name.strip().lower()
    for old, new in replacements.items():
        name = name.replace(old, new)
    return name

def load_config(config_file="genericopensoes.xml"):
    try:
        tree = ET.parse(config_file)
        root = tree.getroot()

        excel_node = root.find("./excel")
        config = {
            "server": root.find("./database/server").text,
            "port": root.find("./database/port").text,
            "database": root.find("./database/database_name").text,
            "trusted_connection": root.find("./database/trusted_connection").text.lower() == "yes",
            "table_name": root.find("./database/table").attrib["name"],
            "excel_file": excel_node.find("file_path").text,
            "sheet_name": excel_node.find("sheet_name").text,
            "skip_rows": int(excel_node.find("skip_rows").text) if excel_node.find("skip_rows") is not None else None,
            "start_data_row": int(excel_node.find("start_data_row").text) if excel_node.find("start_data_row") is not None else None,
            "columns": []
        }

        for col in root.findall("./database/table/columns/column"):
            config["columns"].append({
                "name": col.attrib["name"],
                "type": col.attrib["type"],
                "source_name": col.attrib["source_name"],
                "default": col.attrib.get("default", "")
            })

        return config
    except Exception as e:
        logging.error(f"Erro ao carregar configuração: {str(e)}")
        raise

def read_excel_smart(config):
    """Tenta várias estratégias para ler o Excel."""
    try:
        expected_columns = [normalize_name(col["source_name"]) for col in config["columns"]]

        if config.get("start_data_row"):
            logging.info(f"Lendo com start_data_row = {config['start_data_row']}")
            header_df = pd.read_excel(
                config["excel_file"],
                sheet_name=config["sheet_name"],
                skiprows=config["start_data_row"] - 1,
                header=None,
                nrows=1,
                engine="openpyxl"
            )
            headers = [str(h) for h in header_df.iloc[0]]

            df = pd.read_excel(
                config["excel_file"],
                sheet_name=config["sheet_name"],
                skiprows=config["start_data_row"],
                header=None,
                names=headers,
                engine="openpyxl"
            )
            df.columns = [normalize_name(c) for c in df.columns]
            return df

        elif config.get("skip_rows") is not None:
            df = pd.read_excel(
                config["excel_file"],
                sheet_name=config["sheet_name"],
                skiprows=config["skip_rows"],
                dtype=str,
                engine="openpyxl"
            )
            df.columns = [normalize_name(c) for c in df.columns]
            return df

        else:
            all_data = pd.read_excel(
                config["excel_file"],
                sheet_name=config["sheet_name"],
                header=None,
                dtype=str,
                engine="openpyxl"
            )

            best_idx = -1
            best_score = 0
            for idx, row in all_data.iterrows():
                row_headers = [normalize_name(str(cell)) for cell in row]
                score = sum(1 for col in expected_columns if col in row_headers)
                if score > best_score:
                    best_idx = idx
                    best_score = score
                    if score == len(expected_columns):
                        break

            if best_idx >= 0:
                df = pd.read_excel(
                    config["excel_file"],
                    sheet_name=config["sheet_name"],
                    skiprows=best_idx,
                    dtype=str,
                    engine="openpyxl"
                )
                df.columns = [normalize_name(c) for c in df.columns]
                return df

        raise ValueError("Falha ao detectar cabeçalhos no Excel.")
    except Exception as e:
        logging.error(f"Erro ao ler Excel: {str(e)}")
        raise

def connect_sql(config):
    conn_str = f"DRIVER={{SQL Server}};SERVER={config['server']},{config['port']};DATABASE={config['database']};"
    if config["trusted_connection"]:
        conn_str += "Trusted_Connection=yes;"
    return pyodbc.connect(conn_str)

def create_table_if_needed(config, conn):
    cursor = conn.cursor()
    columns_sql = ", ".join([f"{c['name']} {c['type']}" for c in config["columns"]])
    sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{config['table_name']}')
    BEGIN
        CREATE TABLE {config['table_name']} ({columns_sql})
    END
    """
    cursor.execute(sql)
    conn.commit()

def prepare_dataframe(df, config):
    """Prepara o DataFrame com as colunas certas e tipos."""
    for col in config["columns"]:
        target = col["name"]
        source = normalize_name(col["source_name"])
        default = col["default"]

        if source in df.columns:
            df[target] = df[source]
        else:
            logging.warning(f"Coluna '{source}' não encontrada, usando valor padrão.")
            df[target] = default

        col_type = col["type"].upper()
        try:
            if "DECIMAL" in col_type or "FLOAT" in col_type:
                df[target] = pd.to_numeric(df[target], errors="coerce").fillna(float(default))
            elif "INT" in col_type:
                df[target] = pd.to_numeric(df[target], errors="coerce").fillna(int(default))
            elif "DATE" in col_type:
                df[target] = pd.to_datetime(df[target], errors="coerce")
            else:
                df[target] = df[target].fillna(default).astype(str)
        except Exception as e:
            logging.error(f"Erro ao converter coluna {target}: {str(e)}")

    return df[[col["name"] for col in config["columns"]]]

def import_excel_to_sql(config):
    try:
        df = read_excel_smart(config)
        df = prepare_dataframe(df, config)

        conn = connect_sql(config)
        create_table_if_needed(config, conn)
        cursor = conn.cursor()

        placeholders = ', '.join(['?'] * len(df.columns))
        insert_sql = f"INSERT INTO {config['table_name']} ({', '.join(df.columns)}) VALUES ({placeholders})"

        success = 0
        for _, row in df.iterrows():
            try:
                cursor.execute(insert_sql, tuple(row))
                success += 1
            except Exception as e:
                logging.warning(f"Erro ao inserir linha: {str(e)}")
                conn.rollback()

        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"Importação finalizada: {success}/{len(df)} linhas inseridas.")
        return True
    except Exception as e:
        logging.error(f"Erro na importação: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        config_file = os.getenv("CONFIG_FILE", "genericopensoes.xml")
        config = load_config(config_file)
        import_excel_to_sql(config)
    except Exception as e:
        logging.error(f"Erro fatal: {str(e)}")
