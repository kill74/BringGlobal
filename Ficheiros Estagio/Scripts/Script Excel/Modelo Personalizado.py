import pandas as pd
import pyodbc
import os

def connect_to_sql(server, database):
    """Conecta ao banco de dados SQL Server."""
    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    return pyodbc.connect(connection_string)

def create_table_if_not_exists(table_name, df, conn):
    """Cria a tabela no SQL Server se ela não existir."""
    cursor = conn.cursor()
    
    columns_sql = ', '.join([f'[{col}] NVARCHAR(MAX)' for col in df.columns])
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
    BEGIN
        CREATE TABLE {table_name} (
            {columns_sql}
        )
    END
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()

def normalize_column_names(df):
    """Normaliza os nomes das colunas removendo espaços e caracteres especiais."""
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^\w]", "", regex=True)
    return df

def map_columns(df, column_mapping):
    """Mapeia as colunas do DataFrame para os nomes esperados."""
    selected_columns = {}
    for key, possible_names in column_mapping.items():
        for col in df.columns:
            if any(name.lower() in col.lower() for name in possible_names):
                selected_columns[key] = col
                break
    
    if not selected_columns:
        raise ValueError(f"Nenhuma coluna correspondente encontrada. Colunas disponíveis: {df.columns.tolist()}")
    
    df = df[list(selected_columns.values())]
    df.columns = list(selected_columns.keys())
    return df

def import_xlsx_to_sql(excel_file, sheet_name, table_name, column_mapping, server, database):
    """Importa um arquivo Excel para o SQL Server."""
    if not os.path.exists(excel_file):
        raise FileNotFoundError(f"Arquivo não encontrado: {excel_file}")
    
    # Tenta carregar os dados ignorando as primeiras linhas caso necessário
    for skip_rows in range(5):
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=skip_rows)
            df = normalize_column_names(df)
            df = map_columns(df, column_mapping)
            break
        except Exception as e:
            print(f"Erro ao ler a planilha com skip_rows={skip_rows}: {e}")
    
    df = df.dropna(how='all')  # Remove linhas vazias
    
    conn = connect_to_sql(server, database)
    create_table_if_not_exists(table_name, df, conn)
    
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
    print(f"✅ Dados importados para {table_name}")

# Exemplo de configuração
document_path = "C:/Users/pmcmo/Documents/exemplo.xlsx"
sheet_name = "Planilha1"
table_name = "Tabela_Exemplo"
server = "localhost,1433"
database = "master"

column_mapping = {
    "Empresa": ["Empresa", "Company"],
    "Data_da_fatura": ["Data", "InvoiceDate"],
    "Valor": ["Valor", "Total"],
}

import_xlsx_to_sql(document_path, sheet_name, table_name, column_mapping, server, database)
