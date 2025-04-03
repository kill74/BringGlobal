import pandas as pd
import pyodbc
import os

def connect_to_sql():
    """Ligar a base de dados """
    print("Ligar ao SQL Server...")
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
            Empresa NVARCHAR(255),
            Mes_Processamento NVARCHAR(50),
            Tipo_Novo_ou_Reforco NVARCHAR(50),
            Numero_Fiscal NVARCHAR(50),
            Nome_do_aderente NVARCHAR(255),
            Num_ID NVARCHAR(50),
            Email NVARCHAR(255),
            Premio_Unique_Reforco FLOAT,
            Custo_Apolice FLOAT,
            Comissao_de_subscricao FLOAT,
            Premio_Total FLOAT
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

def import_xlsx_to_sql(excel_file, sheet_name, table_name):
    """Importa o arquivo Excel para o SQL Server."""
    print("Carregando arquivo: {excel_file}")
    if not os.path.exists(excel_file):
        raise FileNotFoundError("Arquivo não encontrado: {excel_file}")
    
    for skip_rows in range(0, 5):  
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=skip_rows, dtype=str, engine="openpyxl")
            df = normalize_column_names(df)
            print("Tentativa com skiprows={skip_rows} - Colunas normalizadas: {df.columns.tolist()}")
            if len(df.columns) > 0 and not all(col.startswith('Unnamed') for col in df.columns):
                break
        except Exception as e:
            print("Erro ao tentar ler com skiprows={skip_rows}: {e}")
            continue
    
    column_mapping = {
        "Empresa": ["Empresa"],
        "Mes_Processamento": ["Mês_Processamento"],
        "Tipo_Novo_ou_Reforco": ["Tipo - novo ou reforço(N/R)"],
        "Numero_Fiscal": ["Numero Fiscal"],
        "Nome_do_aderente": ["Nome do aderente"],
        "Num_ID": ["nºid"],
        "Email": ["email"],
        "Premio_Unique_Reforco": ["Prémio (Único/Reforço)"],
        "Custo_Apolice": ["Custo Apólice"],
        "Comissao_de_subscricao": ["Comissão de subscrição"],
        "Premio_Total": ["Prémio Total"]
    }
    
    selected_columns = {}
    for key, possible_names in column_mapping.items():
        found_col = find_column(df, possible_names)
        print(f"🔍 Procurando {key}: {possible_names} -> {found_col}")
        if found_col:
            selected_columns[key] = found_col
    
    df = df[list(selected_columns.values())]
    df.columns = list(selected_columns.keys())
    
    print("Colunas selecionadas para inserção:", selected_columns)
    
    numeric_columns = ["Premio_Unique_Reforco", "Custo_Apolice", "Comissao_de_subscricao", "Premio_Total"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    text_columns = ["Empresa", "Mes_Processamento", "Tipo_Novo_ou_Reforco", "Numero_Fiscal", "Nome_do_aderente", "Num_ID", "Email"]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    
    print("Tipos de dados ajustados:")
    print(df.dtypes)
    
    conn = connect_to_sql()
    create_table_if_not_exists(table_name, conn)
    
    cursor = conn.cursor()
    placeholders = ', '.join(['?'] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
    
    for _, row in df.iterrows():
        valores = tuple(row)
        print("Inserindo: {valores}")
        cursor.execute(sql, valores)
    
    conn.commit()
    cursor.close()
    conn.close()
    print(" Dados importados para {table_name} com sucesso!")

# Configuração
document_path = "C:/Users/pmcmo/Documents/Bring_SV_Capitalização_2024.12_Final.xlsx"
sheet_name = "SV_Capitalização"
table_name = "Tabela_Capitalizacao"

# Chamada da função
import_xlsx_to_sql(document_path, sheet_name, table_name)