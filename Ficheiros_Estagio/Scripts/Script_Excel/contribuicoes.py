import pandas as pd
import pyodbc
import os
import xml.etree.ElementTree as ET
import logging
from typing import Dict, Any, Optional

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('excel_to_sql_import.log'),
        logging.StreamHandler()
    ]
)

def load_config(config_file: str = "genericoexcelContribuicoes.xml") -> Dict[str, Any]:
    """Carrega as configurações do arquivo XML com suporte a skip_header_rows e start_data_row."""
    try:
        tree = ET.parse(config_file)
        root = tree.getroot()
        
        excel_config = root.find("./excel")
        skip_header = int(excel_config.find("skip_header_rows").text) if excel_config.find("skip_header_rows") is not None else 0
        start_data = int(excel_config.find("start_data_row").text) if excel_config.find("start_data_row") is not None else None
        
        config = {
            "server": root.find("./database/server").text,
            "port": root.find("./database/port").text,
            "database": root.find("./database/database_name").text,
            "trusted_connection": root.find("./database/trusted_connection").text.lower() == "yes",
            "table_name": root.find("./database/table").attrib["name"],
            "excel_file": excel_config.find("file_path").text,
            "sheet_name": excel_config.find("sheet_name").text,
            "skip_header_rows": skip_header,
            "start_data_row": start_data,
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

def read_excel_data(config: Dict[str, Any]) -> pd.DataFrame:
    """Lê o arquivo Excel com base nas configurações, suportando múltiplos formatos."""
    try:
        # Primeira tentativa: ler com cabeçalho personalizado
        if config.get('start_data_row') is not None:
            logging.info(f"Tentando ler com cabeçalho na linha {config['skip_header_rows']} e dados a partir da linha {config['start_data_row']}")
            
            # Lê os cabeçalhos separadamente
            header_df = pd.read_excel(
                config['excel_file'],
                sheet_name=config['sheet_name'],
                skiprows=config['skip_header_rows'],
                nrows=1,
                header=None,
                engine="openpyxl"
            )
            headers = [str(cell) for cell in header_df.iloc[0]]
            
            # Lê os dados
            df = pd.read_excel(
                config['excel_file'],
                sheet_name=config['sheet_name'],
                skiprows=config['start_data_row'] - 1,  # -1 porque skiprows é 0-based
                header=None,
                names=headers,
                engine="openpyxl"
            )
            return df

        # Segunda tentativa: abordagem padrão
        df = pd.read_excel(
            config['excel_file'],
            sheet_name=config['sheet_name'],
            skiprows=config.get('skip_header_rows', 0),
            engine="openpyxl"
        )
        return df

    except Exception as e:
        logging.error(f"Falha ao ler arquivo Excel: {str(e)}")
        raise

def normalize_string(value: str) -> str:
    """Normaliza strings removendo espaços, acentos e caracteres especiais."""
    if not isinstance(value, str):
        return str(value)
    
    replacements = {
        " ": "", "-": "", "(": "", ")": "", "º": "", "ç": "c", "é": "e", 
        "á": "a", "ó": "o", "ã": "a", "ú": "u", "í": "i", "â": "a", 
        "ê": "e", "ô": "o", "ª": "", "º": "", "ñ": "n", "ü": "u"
    }
    value = value.strip().lower()
    for old, new in replacements.items():
        value = value.replace(old, new)
    return value

def map_columns(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Mapeia as colunas do Excel para os nomes no banco de dados."""
    result_df = pd.DataFrame()
    
    for col_config in config['columns']:
        source_name = col_config['source_name']
        target_name = col_config['name']
        col_type = col_config['type']
        default = col_config['default']
        
        # Tenta encontrar a coluna no DataFrame
        found = False
        for excel_col in df.columns:
            if normalize_string(excel_col) == normalize_string(source_name):
                result_df[target_name] = df[excel_col]
                found = True
                break
        
        if not found:
            logging.warning(f"Coluna '{source_name}' não encontrada. Usando valor padrão: {default}")
            if "DECIMAL" in col_type.upper() or "FLOAT" in col_type.upper():
                result_df[target_name] = float(default)
            elif "INT" in col_type.upper():
                result_df[target_name] = int(default)
            else:
                result_df[target_name] = default
    
    return result_df

def convert_data_types(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Converte os tipos de dados conforme especificado na configuração."""
    for col_config in config['columns']:
        col_name = col_config['name']
        col_type = col_config['type'].upper()
        
        if col_name not in df.columns:
            continue
            
        try:
            if "DECIMAL" in col_type or "FLOAT" in col_type:
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(float(col_config['default']))
            elif "INT" in col_type:
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(int(col_config['default']))
            elif "DATE" in col_type or "TIME" in col_type:
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            else:  # Strings e outros tipos
                df[col_name] = df[col_name].astype(str).fillna(col_config['default'])
        except Exception as e:
            logging.error(f"Erro ao converter coluna {col_name} para {col_type}: {str(e)}")
            df[col_name] = df[col_name].fillna(col_config['default'])
    
    return df

def import_excel_to_sql(config: Dict[str, Any]) -> bool:
    """Função principal que executa toda a importação."""
    try:
        # 1. Ler o arquivo Excel
        df = read_excel_data(config)
        logging.info(f"Excel lido com sucesso. {len(df)} linhas encontradas.")
        
        # 2. Mapear colunas
        df = map_columns(df, config)
        
        # 3. Converter tipos de dados
        df = convert_data_types(df, config)
        
        # 4. Conectar ao banco de dados
        conn_str = f"DRIVER={{SQL Server}};SERVER={config['server']},{config['port']};DATABASE={config['database']};"
        if config["trusted_connection"]:
            conn_str += "Trusted_Connection=yes;"
        
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # 5. Criar tabela se não existir
            cols_def = ", ".join([f"{col['name']} {col['type']}" for col in config['columns']])
            create_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{config['table_name']}')
            BEGIN
                CREATE TABLE {config['table_name']} ({cols_def})
            END
            """
            cursor.execute(create_sql)
            conn.commit()
            
            # 6. Inserir dados
            placeholders = ", ".join(["?"] * len(df.columns))
            insert_sql = f"INSERT INTO {config['table_name']} ({', '.join(df.columns)}) VALUES ({placeholders})"
            
            total_rows = len(df)
            success_rows = 0
            
            for _, row in df.iterrows():
                try:
                    cursor.execute(insert_sql, tuple(row))
                    success_rows += 1
                except Exception as e:
                    logging.warning(f"Erro ao inserir linha: {str(e)}")
                    conn.rollback()
                    continue
            
            conn.commit()
            logging.info(f"Importação concluída. {success_rows}/{total_rows} linhas inseridas com sucesso.")
            return True
            
    except Exception as e:
        logging.error(f"Falha na importação: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        config_file = os.getenv("CONFIG_FILE", "genericoexcelContribuicoes.xml")
        config = load_config(config_file)
        
        if not import_excel_to_sql(config):
            logging.error("A importação falhou. Verifique os logs para detalhes.")
            exit(1)
            
    except Exception as e:
        logging.error(f"Erro fatal: {str(e)}")
        exit(1)