import pandas as pd 
import pyodbc        
import os            
import xml.etree.ElementTree as ET  

# Função para carregar configurações a partir do ficheiro XML
def load_config(config_file="genericoexcelcapializacao.xml"):
    """Carrega as configurações do arquivo XML."""
    tree = ET.parse(config_file)  # Lê o ficheiro XML
    root = tree.getroot()         # Pega o elemento raiz do XML

    # Lê e guarda os parâmetros de configuração 
    config = {
        "server": root.find("./database/server").text,
        "port": root.find("./database/port").text,
        "database": root.find("./database/database_name").text,
        "trusted_connection": root.find("./database/trusted_connection").text.lower() == "yes",
        "table_name": root.find("./database/table").attrib["name"],
        "excel_file": root.find("./excel/file_path").text,
        "sheet_name": root.find("./excel/sheet_name").text,
        "columns": []
    }

    # Itera por cada coluna configurada no XML e adiciona ao dicionário
    for col in root.findall("./database/table/columns/column"):
        config["columns"].append({
            "name": col.attrib["name"],               
            "type": col.attrib["type"],               
            "source_name": col.attrib["source_name"], 
            "default": col.attrib.get("default", None) 
        })

    return config  


def connect_to_sql(config):
    """Estabelece conexão com a base de dados."""
    connection_string = f"DRIVER={{SQL Server}};SERVER={config['server']},{config['port']};DATABASE={config['database']};"
    if config["trusted_connection"]:
        connection_string += "Trusted_Connection=yes;"
    return pyodbc.connect(connection_string)

# Função que cria a tabela no SQL se ela ainda não existir
def create_table_if_not_exists(config, conn):
    """Cria a tabela na base de dados se não existir."""
    cursor = conn.cursor()
    # Define o comando CREATE TABLE com base nas colunas do XML
    column_definitions = ", ".join([f"{col['name']} {col['type']}" for col in config["columns"]])
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{config['table_name']}')
    BEGIN
        CREATE TABLE {config['table_name']} ({column_definitions})
    END
    """
    cursor.execute(create_table_sql)  # Executa o SQL
    conn.commit()                     # Confirma a operação
    cursor.close()

# Função auxiliar para normalizar nomes (sem acentos e símbolos)
def normalize_name(name):
    """Normaliza nome removendo espaços, acentos e símbolos."""
    replacements = {
        " ": "_", "-": "_", "(": "", ")": "", "º": "", "ç": "c", "é": "e", "á": "a",
        "ó": "o", "ã": "a", "ú": "u", "í": "i", "â": "a", "ê": "e", "ô": "o"
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    return name.strip()

# Aplica normalização a todas as colunas 
def normalize_column_names(df):
    """Normaliza os nomes das colunas do DataFrame."""
    df.columns = [normalize_name(col) for col in df.columns]
    return df

# Procura a coluna original do Excel com base no nome normalizado
def find_column(df, source_name):
    """Compara nomes normalizados para encontrar a coluna."""
    normalized_df_cols = {normalize_name(col): col for col in df.columns}
    normalized_source = normalize_name(source_name)
    return normalized_df_cols.get(normalized_source)

# Função principal que importa os dados do Excel e insere no SQL
def import_excel_to_sql(config):
    """Importa o arquivo Excel para o SQL Server."""
    if not os.path.exists(config['excel_file']):
        raise FileNotFoundError(f"Arquivo não encontrado: {config['excel_file']}")

    # Tenta ler o Excel  até 4 linhas 
    for skip_rows in range(0, 5):
        try:
            df = pd.read_excel(config['excel_file'], sheet_name=config['sheet_name'], skiprows=skip_rows, dtype=str, engine="openpyxl")
            df = normalize_column_names(df)  # Normaliza os nomes das colunas
            if len(df.columns) > 0 and not all(col.startswith('Unnamed') for col in df.columns):
                break  # Se achar colunas válidas, sai 
        except Exception as e:
            continue

    # Mapeia colunas do Excel para o nome final no SQL
    selected_columns = {}
    for col in config["columns"]:
        found_col = find_column(df, col["source_name"])
        if found_col:
            selected_columns[col["name"]] = found_col

    # Preenche colunas faltantes com valores padrão
    for col in config["columns"]:
        col_name = col["name"]
        if col_name in selected_columns:
            df[col_name] = df[selected_columns[col_name]]
        else:
            print(f"⚠️ Coluna '{col_name}' não encontrada. Vai ser criada com valor default.")
            df[col_name] = col.get("default", "")

    # Reorganiza colunas na ordem do XML
    df = df[[col["name"] for col in config["columns"]]]

    # Conversão de tipos conforme definição
    for col in config["columns"]:
        if "DECIMAL" in col["type"].upper() or "FLOAT" in col["type"].upper():
            df[col["name"]] = pd.to_numeric(df[col["name"]], errors="coerce").fillna(float(col.get("default", 0)))
        else:
            df[col["name"]] = df[col["name"]].fillna(col.get("default", "")).astype(str)

    # Conecta ao SQL e insere os dados
    conn = connect_to_sql(config)
    create_table_if_not_exists(config, conn)  # Cria a tabela se não existir
    cursor = conn.cursor()

    # Prepara o SQL INSERT
    placeholders = ', '.join(['?'] * len(df.columns))
    sql = f"INSERT INTO {config['table_name']} ({', '.join(df.columns)}) VALUES ({placeholders})"

    # Insere linha por linha
    for _, row in df.iterrows():
        cursor.execute(sql, tuple(row))

    # Finaliza
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Dados importados com sucesso para a tabela '{config['table_name']}'.")

# Execução principal do script
if __name__ == "__main__":
    config = load_config()         # Carrega as configs do XML
    import_excel_to_sql(config)    # Roda a importação