import xml.etree.ElementTree as ET
import pyodbc
import pandas as pd
from dotenv import load_dotenv #this is needed to load the environment variables from the .env file
import os

def load_config(config_file="config.xml"):
    """Carrega as configurações do arquivo XML."""
    tree = ET.parse(config_file)
    root = tree.getroot()
    
    # Extrai as configurações da base de dados e do XML
    config = {
        "server": root.find("./database/server").text,
        "port": root.find("./database/port").text,
        "database": root.find("./database/database_name").text,
        "trusted_connection": root.find("./database/trusted_connection").text.lower() == "yes",
        "table_name": root.find("./database/table").attrib["name"],
        "columns": [],
        "namespace": root.find("./xml/namespace").attrib["uri"],
        "root_path": root.find("./xml/root_path").text,
        "file_path": root.find("./xml/file_path").text
    }
    
    # Extrai as configurações das colunas da tabela
    for col in root.findall("./database/table/columns/column"):
        config["columns"].append({
            "name": col.attrib["name"],
            "type": col.attrib["type"],
            "xpath": col.attrib["xpath"],
            "attribute": col.attrib.get("attribute"),
            "default": col.attrib.get("default", None)
        })
    
    return config

def connect_to_sql(config):
    """Estabelece conexão com a base de dados usando as configurações fornecidas."""
    # Load environment variables from Credentials.env (tens de trocar isto)
    load_dotenv("c:/Users/gnail/Documents/BookPanda/Ficheiros Estagio/Credentials.env")

    # Retrieve credentials from environment variables
    server = os.getenv("SERVER")
    port = os.getenv("PORT")
    database = os.getenv("DATABASE")
    trusted_connection = os.getenv("TRUSTED_CONNECTION")

    # Build connection string
    connection_string = f"DRIVER={{SQL Server}};SERVER={server},{port};DATABASE={database};"
    if trusted_connection.lower() == "yes":
        connection_string += "Trusted_Connection=yes;"
    return pyodbc.connect(connection_string)

def create_table_if_not_exists(config, conn):
    """Cria a tabela na base de dados se ainda nao existir."""
    cursor = conn.cursor()
    
    # Define as colunas da tabela com base na configuração
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

def parse_xml_to_dataframe(config):
    """Lê o arquivo XML e converte os dados para um DataFrame do Pandas."""
    tree = ET.parse(config["file_path"])
    root = tree.getroot()
    namespace = {"ns": config["namespace"]}
    
    data = []
    for item in root.findall(config["root_path"], namespace):
        row = {}
        for col in config["columns"]:
            elem = item.find(col["xpath"], namespace)
            
            # Obtém o valor do elemento ou atributo conforme especificado
            if elem is not None:
                value = elem.attrib.get(col["attribute"]) if col["attribute"] else elem.text
            else:
                value = col.get("default", None)
            
            # Conversão para tipos numéricos, se necessário
            if "DECIMAL" in col["type"] and value is not None:
                try:
                    value = float(value) if value not in ["", "NaN"] else float(col.get("default", 0.00))
                except (ValueError, TypeError):
                    print(f" Erro ao converter '{value}' para float na coluna {col['name']}. Usando padrão {col.get('default', 0.00)}")
                    value = float(col.get("default", 0.00))
            
            row[col["name"]] = value
        data.append(row)
    
    return pd.DataFrame(data)

def import_xml_to_sql(config):
    """Processa e insere os dados do XML na base de dados."""
    df = parse_xml_to_dataframe(config)
    conn = connect_to_sql(config)
    create_table_if_not_exists(config, conn)
    cursor = conn.cursor()
    
    # Substitui valores nulos antes da inserção no banco de dados  para evitar erros
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
    
    # Insere os dados do xml na base de dados
    for _, row in df.iterrows():
        values = tuple(row)
        placeholders = ', '.join(['?' for _ in values])
        sql = f"INSERT INTO {config['table_name']} ({', '.join(df.columns)}) VALUES ({placeholders})"
        
        try:
            print(f"Introduzindo: {values}")  # Debug
            cursor.execute(sql, values)
        except pyodbc.Error as e:
            print(f"Erro ao Introduzir linha {values}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Dados importados para {config['table_name']}")

if __name__ == "__main__":
    config = load_config("generico2.xml")
    import_xml_to_sql(config)
