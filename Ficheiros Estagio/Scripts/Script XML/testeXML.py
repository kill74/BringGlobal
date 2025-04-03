import xml.etree.ElementTree as ET
import pyodbc
import pandas as pd

def connect_to_sql():
    """Ligar a abse de dadps  """
    connection_string = 'DRIVER={SQL Server};SERVER=localhost,1433;DATABASE=master;Trusted_Connection=yes;'
    return pyodbc.connect(connection_string)

def create_table_if_not_exists(table_name, conn):
    """Cria a tabela no SQL Server se ela não existir."""
    cursor = conn.cursor()
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
    BEGIN
        CREATE TABLE {table_name} (
            Valor_moeda DECIMAL(18,2),
            Tipo_moeda NVARCHAR(50),
            Nome_pessoa NVARCHAR(255),
            Pais NVARCHAR(100),
            Numero_NIF NVARCHAR(50)
        )
    END
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()

def parse_xml_to_dataframe(xml_file):
    """Lê o XML e extrai os dados necessários considerando espaços."""
    tree = ET.parse(xml_file)
    root = tree.getroot()
    namespace = {'ns': 'urn:iso:std:iso:20022:tech:xsd:pain.001.001.03'}
    
    data = []
    for transaction in root.findall('.//ns:CdtTrfTxInf', namespace):
        valor_moeda_elem = transaction.find('.//ns:InstdAmt', namespace)
        tipo_moeda = valor_moeda_elem.attrib.get('Ccy') if valor_moeda_elem is not None else None
        valor_moeda = valor_moeda_elem.text if valor_moeda_elem is not None else None

        nome_pessoa_elem = transaction.find('.//ns:Nm', namespace)
        nome_pessoa = nome_pessoa_elem.text if nome_pessoa_elem is not None else None

        pais_elem = transaction.find('.//ns:Ctry', namespace)
        pais = pais_elem.text if pais_elem is not None else None

        nif_elem = transaction.find('.//ns:IBAN', namespace)
        numero_nif = nif_elem.text if nif_elem is not None else None
        
        data.append([valor_moeda, tipo_moeda, nome_pessoa, pais, numero_nif])
    
    df = pd.DataFrame(data, columns=["Valor_moeda", "Tipo_moeda", "Nome_pessoa", "Pais", "Numero_NIF"])
    
    # Conversão de tipos
    df["Valor_moeda"] = pd.to_numeric(df["Valor_moeda"], errors='coerce')
    
    return df

def import_xml_to_sql(xml_file, table_name):
    """Importa dados do XML para a base de dados """
    df = parse_xml_to_dataframe(xml_file)
    conn = connect_to_sql()
    create_table_if_not_exists(table_name, conn)
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        values = tuple(row)
        placeholders = ', '.join(['?' for _ in values])
        sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
        try:
            cursor.execute(sql, values)
        except pyodbc.Error as e:
            print(f"Erro ao inserir linha {values}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Dados importados para {table_name}")

# Caminho do arquivo XML
document_path = "P1_DataSol_Sal_anon.XML"
table_name = "P1_DataSol_Sal"

# Chamada da função para importar os dados do XML para o SQL Server
import_xml_to_sql(document_path, table_name)
