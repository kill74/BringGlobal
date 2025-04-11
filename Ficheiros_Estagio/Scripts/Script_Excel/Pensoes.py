import pandas as pd
import pyodbc
import os
import xml.etree.ElementTree as ET
import logging

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('excel_to_sql_import.log'),
        logging.StreamHandler()
    ]
)

def load_config(config_file="genericopensoes.xml"):
    """Carrega as configurações do arquivo XML."""
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

        logging.info("Configurações carregadas com sucesso.")
        return config
    except Exception as e:
        logging.error(f"Erro ao carregar configurações: {str(e)}")
        raise

def connect_to_sql(config):
    """Estabelece conexão com a base de dados."""
    try:
        connection_string = f"DRIVER={{SQL Server}};SERVER={config['server']},{config['port']};DATABASE={config['database']};"
        if config["trusted_connection"]:
            connection_string += "Trusted_Connection=yes;"
        
        logging.info("Conectando ao SQL Server...")
        return pyodbc.connect(connection_string)
    except Exception as e:
        logging.error(f"Erro ao conectar ao SQL Server: {str(e)}")
        raise

def create_table_if_not_exists(config, conn):
    """Cria a tabela na base de dados se não existir."""
    try:
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
        logging.info(f"Tabela {config['table_name']} verificada/criada com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao criar tabela: {str(e)}")
        raise

def normalize_name(name):
    """Normaliza nome removendo espaços, acentos e símbolos."""
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
    """Normaliza os nomes das colunas do DataFrame."""
    df.columns = [normalize_name(col) for col in df.columns]
    return df

def find_column(df, source_name):
    """Compara nomes normalizados para encontrar a coluna."""
    normalized_df_cols = {normalize_name(col): col for col in df.columns}
    normalized_source = normalize_name(source_name)
    return normalized_df_cols.get(normalized_source)

def validate_headers(df, config):
    """Valida se os cabeçalhos esperados estão presentes no DataFrame."""
    expected_columns = [normalize_name(col['source_name']) for col in config['columns']]
    found_columns = [normalize_name(col) for col in df.columns]
    
    missing_columns = set(expected_columns) - set(found_columns)
    if missing_columns:
        logging.warning(f"Cabeçalhos ausentes: {missing_columns}")
        return False
    return True

def read_excel_with_fallback(config):
    """Tenta ler o arquivo Excel com diferentes abordagens."""
    try:
        # Tentativa 1: Usar skip_rows do config se existir
        if config.get('skip_rows') is not None:
            logging.info(f"Tentando ler com skip_rows={config['skip_rows']}")
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

        # Tentativa 2: Varredura automática
        logging.info("Varrendo arquivo para encontrar cabeçalhos...")
        all_data = pd.read_excel(
            config['excel_file'],
            sheet_name=config['sheet_name'],
            header=None,
            dtype=str,
            engine="openpyxl"
        )
        
        best_match = {'idx': 0, 'matches': 0}
        expected_columns = [normalize_name(col['source_name']) for col in config['columns']]
        
        for idx, row in all_data.iterrows():
            row_headers = [normalize_name(str(cell)) for cell in row]
            matches = sum(1 for col in expected_columns if col in row_headers)
            
            if matches > best_match['matches']:
                best_match = {'idx': idx, 'matches': matches}
                if matches == len(expected_columns):
                    break
        
        if best_match['matches'] > 0:
            logging.info(f"Melhor correspondência encontrada na linha {best_match['idx']} com {best_match['matches']}/{len(expected_columns)} colunas")
            df = pd.read_excel(
                config['excel_file'],
                sheet_name=config['sheet_name'],
                skiprows=best_match['idx'],
                dtype=str,
                engine="openpyxl"
            )
            df = normalize_column_names(df)
            return df
        else:
            raise ValueError("Nenhum cabeçalho correspondente encontrado no arquivo Excel")
            
    except Exception as e:
        logging.error(f"Falha ao ler arquivo Excel: {str(e)}")
        raise

def import_excel_to_sql(config):
    """Importa o arquivo Excel para o SQL Server."""
    try:
        logging.info(f"Iniciando importação do arquivo {config['excel_file']}")
        
        if not os.path.exists(config['excel_file']):
            raise FileNotFoundError(f"Arquivo não encontrado: {config['excel_file']}")

        # Ler o arquivo Excel
        df = read_excel_with_fallback(config)
        
        # Mapeia colunas do Excel para o nome final no SQL
        selected_columns = {}
        for col in config["columns"]:
            found_col = find_column(df, col["source_name"])
            if found_col:
                selected_columns[col["name"]] = found_col
            else:
                logging.warning(f"Coluna '{col['source_name']}' não encontrada no Excel. Usando valor padrão.")

        # Preenche colunas faltantes com valores padrão
        for col in config["columns"]:
            col_name = col["name"]
            if col_name in selected_columns:
                df[col_name] = df[selected_columns[col_name]]
            else:
                df[col_name] = col.get("default", "")

        # Reorganiza colunas na ordem do XML
        df = df[[col["name"] for col in config["columns"]]]

        # Conversão de tipos conforme definição
        for col in config["columns"]:
            if "DECIMAL" in col["type"].upper() or "FLOAT" in col["type"].upper():
                df[col["name"]] = pd.to_numeric(df[col["name"]], errors="coerce").fillna(float(col.get("default", 0)))
            elif "INT" in col["type"].upper():
                df[col["name"]] = pd.to_numeric(df[col["name"]], errors="coerce").fillna(int(col.get("default", 0)))
            else:
                df[col["name"]] = df[col["name"]].fillna(col.get("default", "")).astype(str)

        # Conecta ao SQL e insere os dados
        conn = connect_to_sql(config)
        create_table_if_not_exists(config, conn)
        cursor = conn.cursor()

        # Prepara o SQL INSERT
        placeholders = ', '.join(['?'] * len(df.columns))
        sql = f"INSERT INTO {config['table_name']} ({', '.join(df.columns)}) VALUES ({placeholders})"

        # Insere linha por linha com tratamento de erros
        total_rows = len(df)
        success_rows = 0
        for idx, row in df.iterrows():
            try:
                cursor.execute(sql, tuple(row))
                success_rows += 1
            except Exception as e:
                logging.warning(f"Erro ao inserir linha {idx + 1}: {str(e)}")
                conn.rollback()
                continue

        # Finaliza
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Importação concluída com sucesso. {success_rows}/{total_rows} linhas inseridas na tabela '{config['table_name']}'.")
        return True
        
    except Exception as e:
        logging.error(f"Falha na importação: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        config = load_config("genericopensoes.xml")
        success = import_excel_to_sql(config)
        if not success:
            logging.error("A importação falhou. Verifique os logs para mais detalhes.")
            exit(1)
    except Exception as e:
        logging.error(f"Erro fatal: {str(e)}")
        exit(1)