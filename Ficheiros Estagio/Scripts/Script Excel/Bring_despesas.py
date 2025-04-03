import pandas as pd
import pyodbc
import os

def connect_to_sql():
    """Conecta a base de dados"""
    connection_string = 'DRIVER={SQL Server};SERVER=localhost,1433;DATABASE=master;Trusted_Connection=yes;'
    return pyodbc.connect(connection_string)

def create_table_if_not_exists(table_name, conn):
    """Cria a tabela no SQL Server se ela não existir."""
    cursor = conn.cursor()
    
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
    BEGIN
        CREATE TABLE {table_name} (
            Empresa NVARCHAR(255),
            Data_da_fatura DATE,
            Beneficio NVARCHAR(255),
            Valor_da_fatura DECIMAL(18,2),
            Valor_IVA DECIMAL(18,2),
            Mes_de_reembolso NVARCHAR(50)
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

def find_column(df, possible_names):
    """Encontra a coluna correta"""
    for name in possible_names:
        if name in df.columns:
            return name
    return None

def import_xlsx_to_sql(excel_file, sheet_name, table_name):
    """Importa o arquivo Excel para o SQL Server."""
    if not os.path.exists(excel_file):
        raise FileNotFoundError(f"Arquivo não encontrado: {excel_file}")
    
    # Primeiro, leia apenas os cabeçalhos para verificar
    df_headers = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=0)
    print("Cabeçalhos encontrados no arquivo:", df_headers.columns.tolist())
    
    # Tenta diferentes números de linhas para pular
    for skip_rows in range(0, 5):  # Tenta pular de 0 a 4 linhas
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=skip_rows)
            df = normalize_column_names(df)
            
            # Verifique se encontrou colunas válidas
            if len(df.columns) > 0 and not all(col.startswith('Unnamed') for col in df.columns):
                break
        except:
            continue
    
    print("Colunas após tentativa de leitura:", df.columns.tolist())
    
    # Mapeamento flexível de colunas
    column_mapping = {
        "Empresa": ["Empresa", "Company", "Fornecedor", "Provider"],
        "Data_da_fatura": ["Data_da_fatura", "DataFatura", "Date", "Data", "InvoiceDate"],
        "Beneficio": ["Beneficio", "Benefit", "Servico", "Service"],
        "Valor_da_fatura": ["Valor_da_fatura", "ValorFatura", "Amount", "Valor", "Total"],
        "Valor_IVA": ["Valor_IVA", "ValorIVA", "VAT", "Tax", "IVA"],
        "Mes_de_reembolso": ["Mes_de_reembolso", "MesReembolso", "Month", "Reembolso"]
    }
    
    selected_columns = {}
    for key, possible_names in column_mapping.items():
        found_col = find_column(df, possible_names)
        if found_col:
            selected_columns[key] = found_col
        else:
            # Se não encontrar, tenta verificar por conteúdo
            for col in df.columns:
                if any(name.lower() in str(col).lower() for name in possible_names):
                    selected_columns[key] = col
                    break
    
    if not selected_columns:
        raise ValueError(f"Nenhuma coluna correspondente encontrada. Colunas disponíveis: {df.columns.tolist()}")
    
    df = df[list(selected_columns.values())]
    df.columns = list(selected_columns.keys())
    df = df.dropna(how='all')  # Remove linhas totalmente vazias
    
    if 'Data_da_fatura' in df.columns:
        df['Data_da_fatura'] = pd.to_datetime(df['Data_da_fatura'], errors='coerce')
    if 'Valor_da_fatura' in df.columns:
        df['Valor_da_fatura'] = pd.to_numeric(df['Valor_da_fatura'], errors='coerce')
    if 'Valor_IVA' in df.columns:
        df['Valor_IVA'] = pd.to_numeric(df['Valor_IVA'], errors='coerce')
    
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
    print(f"✅ Dados importados para {table_name}")

document_path = "C:/Users/pmcmo/Documents/Bring_despesas_2024.12_sent_client.xlsx"
sheet_name = "Tecnologia"  # Ajuste conforme necessário
table_name = "Tabela_Despesas"

# Chamada da função para importar os dados do Excel para o SQL Server
import_xlsx_to_sql(document_path, sheet_name, table_name)
