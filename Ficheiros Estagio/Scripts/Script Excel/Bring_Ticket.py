import pandas as pd
import pyodbc
import os

def connect_to_sql():
    """Conecta ao banco de dados SQL Server."""
    print("🔌 Conectando ao SQL Server...")
    connection_string = 'DRIVER={SQL Server};SERVER=localhost,1433;DATABASE=master;Trusted_Connection=yes;'
    return pyodbc.connect(connection_string)

def create_table_if_not_exists(table_name, conn):
    """Cria a tabela no SQL Server caso ela não exista."""
    print(f"📂 Verificando se a tabela '{table_name}' existe...")
    cursor = conn.cursor()
    
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
    BEGIN
        CREATE TABLE {table_name} (
            Num_Colaborador NVARCHAR(50),
            Empresa NVARCHAR(255),
            NIF_Empresa NVARCHAR(50),
            Nome_Colaborador NVARCHAR(255),
            NIF_Colaborador NVARCHAR(50),
            Num_Vales1 INT,
            Valor_Vales1 FLOAT,
            Num_Vales2 INT,
            Valor_Vales2 FLOAT,
            Num_Vales3 INT,
            Valor_Vales3 FLOAT,
            Num_Vales4 INT,
            Valor_Vales4 FLOAT,
            Num_Vales5 INT,
            Valor_Vales5 FLOAT,
            Valor_Total FLOAT,
            Email NVARCHAR(255)
        )
    END
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    print("✅ Tabela verificada/criada com sucesso!")

def normalize_column_names(df):
    """Normaliza os nomes das colunas removendo espaços e caracteres especiais."""
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^\w]", "", regex=True)
    print("📌 Colunas normalizadas:", df.columns.tolist())
    return df

def find_column(df, possible_names):
    """Encontra a coluna correta baseada em nomes alternativos."""
    for name in possible_names:
        if name in df.columns:
            return name
    return None

def clean_numeric_column(series):
    """Converte colunas numéricas corretamente."""
    return pd.to_numeric(series.astype(str).str.replace(",", "."), errors="coerce").fillna(0)

def import_xlsx_to_sql(excel_file, sheet_name, table_name):
    """Importa um arquivo Excel para o SQL Server."""
    print(f"📁 Carregando arquivo: {excel_file}")
    if not os.path.exists(excel_file):
        raise FileNotFoundError(f"❌ Arquivo não encontrado: {excel_file}")
    
    for skip_rows in range(0, 5):  
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=skip_rows, dtype=str, engine="openpyxl")
            df = normalize_column_names(df)
            print(f"Tentativa com skiprows={skip_rows} - Colunas normalizadas: {df.columns.tolist()}")
            if len(df.columns) > 0 and not all(col.startswith('Unnamed') for col in df.columns):
                break
        except Exception as e:
            print(f"Erro ao tentar ler com skiprows={skip_rows}: {e}")
            continue
    
    column_mapping = {
        "Num_Colaborador": ["Nº_colaborador", "Num_Colaborador"],
        "Empresa": ["Empresa"],
        "NIF_Empresa": ["NIF_Empresa"],
        "Nome_Colaborador": ["Nome_colaborador", "Nome_Colaborador"],
        "NIF_Colaborador": ["NIF_colaborador", "NIF_Colaborador"],
        "Num_Vales1": ["Nº_de_vales_1", "Num_Vales1"],
        "Valor_Vales1": ["Valor_de_vales_1", "Valor_Vales1"],
        "Num_Vales2": ["Nº_de_vales_2", "Num_Vales2"],
        "Valor_Vales2": ["Valor_de_vales_2", "Valor_Vales2"],
        "Num_Vales3": ["Nº_de_vales_3", "Num_Vales3"],
        "Valor_Vales3": ["Valor_de_vales_3", "Valor_Vales3"],
        "Num_Vales4": ["Nº_de_vales_4", "Num_Vales4"],
        "Valor_Vales4": ["Valor_de_vales_4", "Valor_Vales4"],
        "Num_Vales5": ["Nº_de_vales_5", "Num_Vales5"],
        "Valor_Vales5": ["Valor_de_vales_5", "Valor_Vales5"],
        "Valor_Total": ["Valor_Total__12345", "Valor_Total__(1+2+3+4+5)", "Valor_Total"],
        "Email": ["Email"]
    }
    
    selected_columns = {key: find_column(df, possible_names) for key, possible_names in column_mapping.items()}
    selected_columns = {k: v for k, v in selected_columns.items() if v is not None}
    
    df = df[list(selected_columns.values())]
    df.columns = list(selected_columns.keys())
    
    numeric_columns = ["Num_Vales1", "Valor_Vales1", "Num_Vales2", "Valor_Vales2", 
                       "Num_Vales3", "Valor_Vales3", "Num_Vales4", "Valor_Vales4", 
                       "Num_Vales5", "Valor_Vales5", "Valor_Total"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = clean_numeric_column(df[col])
    
    text_columns = ["Num_Colaborador", "Empresa", "NIF_Empresa", "Nome_Colaborador", "NIF_Colaborador", "Email"]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    
    conn = connect_to_sql()
    create_table_if_not_exists(table_name, conn)
    
    cursor = conn.cursor()
    placeholders = ', '.join(['?'] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
    
    print("📌 Inserindo os seguintes dados no banco:")
    print(df.head())
    
    cursor.executemany(sql, df.values.tolist())
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Dados importados com sucesso para {table_name}")

document_path = "C:/Users/pmcmo/Documents/Bring_Ticket_2024.12.xlsx"
sheet_name = "Ticket Infância digital"
table_name = "Ticket_Infancia_Digital"

import_xlsx_to_sql(document_path, sheet_name, table_name)