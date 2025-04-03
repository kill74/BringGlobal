import pandas as pd  
import pyodbc
import os

def connect_to_sql():
    """Conexao A base de dados."""
    connection_string = 'DRIVER={SQL Server};SERVER=localhost,1433;DATABASE=master;Trusted_Connection=yes;'
    return pyodbc.connect(connection_string)

def create_table_if_not_exists(table_name, conn):
    """Cria a tabela se ela nao existir"""
    cursor = conn.cursor()
    
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
    BEGIN
        CREATE TABLE {table_name} (
            Empresa NVARCHAR(255),
            Nome NVARCHAR(255),
            Numero_Empregado NVARCHAR(50),
            Numero_Contribuinte NVARCHAR(50),
            Email NVARCHAR(255),
            BPI_Seguranca DECIMAL(18,2),
            BPI_Acoes DECIMAL(18,2),
            Total DECIMAL(18,2),
            Opcao_BPI_Seguranca INT,
            Opcao_BPI_Acoes INT,
            Total_Opcoes INT,
            Comentarios NVARCHAR(MAX)
        )
    END
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()

def normalize_column_names(df):
    """Normaliza os nomes das colunas removendo espaços e caracteres especiais para evitar problemas ."""
    # cCnverte todos para string e trata valores NaN e faz substituiçoes
    df.columns = [str(col).strip() if pd.notna(col) else 'Unnamed' for col in df.columns]
    df.columns = df.columns.str.replace(" ", "_").str.replace(r"[^\w]", "", regex=True)
     # Imprime os nomes 
    print("Colunas normalizadas:", df.columns.tolist())
    return df

def find_column(df, possible_names):
    """Encontra a coluna correta para a  desejada"""
    for name in possible_names:
        # Verifica tanto o nome exato quanto contém (case insensitive)
        for col in df.columns:
            if str(name).lower() in str(col).lower():
                return col
                #retorno o nome da coluna
    return None

def import_xlsx_to_sql(excel_file, sheet_name, table_name):
    """Importa o arquivo Excel para o SQL Server."""
    if not os.path.exists(excel_file):
        raise FileNotFoundError(f"Arquivo não encontrado: {excel_file}")

    """Faz um loop até encontrar os dados """
    # Tenta diferentes números de linhas para pular
    for skip_rows in range(0, 5):
        try:
            """Tenta ler o arquivo """
            df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=skip_rows, engine="xlrd")
            # Verifica se encontrou os cabeçalhos principais
            if any('empresa' in str(col).lower() for col in df.columns):
                break

                 
            # Verifique se encontrou colunas válidas
        except Exception as e:
            print(f"Tentativa com skiprows={skip_rows} falhou: {e}")
            continue
    
    df = normalize_column_names(df)
    """Maior flexibilidade no reconhecimento dos nomes  """
      # Mapeamento flexível de colunas
    column_mapping = {
        "Empresa": ["Empresa", "BRING_GLOBAL_SERVICES_SA"],
        "Nome": ["Nome", "Name"],
        "Numero_Empregado": ["NºEmpregado", "NumeroEmpregado", "BI7701"],
        "Numero_Contribuinte": ["NºdeContribuinte", "NumeroContribuinte"],
        "Email": ["Email", "E_mail", "BI7701bringglobal.com"],
        "BPI_Seguranca": ["BPISeguranca", "50"],
        "BPI_Acoes": ["BPIAcoes", "0"],
        "Total": ["Total", "501"],
        "Opcao_BPI_Seguranca": ["OpcaoBPISeguranca", "1"],
        "Opcao_BPI_Acoes": ["OpcaoBPIAcoes", "0"],
        "Total_Opcoes": ["TotalOpcoes", "1"]
    }
    
    """Seleçao das colunas corretas """
    selected_columns = {}
    for key, possible_names in column_mapping.items():
        selected_columns[key] = find_column(df, possible_names)
        print(f"🔍 {key}: {' | '.join(possible_names)} -> {selected_columns[key]}")
    
    """Remove as nao encontradas """
    selected_columns = {k: v for k, v in selected_columns.items() if v is not None}
    
    """Mantem apenas as encontradas """
    if not selected_columns:
        available_columns = "\n- ".join([str(col) for col in df.columns])
        raise ValueError(f"Nenhuma coluna correspondente encontrada. Colunas disponíveis:\n- {available_columns}")
    
    # Seleciona e renomeia colunas para os nomes padronizados no sql
    df = df[list(selected_columns.values())]
    df.columns = list(selected_columns.keys())
    
   
    if 'BPI_Seguranca' in df.columns:
        df['BPI_Seguranca'] = pd.to_numeric(df['BPI_Seguranca'], errors='coerce').fillna(0)
    if 'BPI_Acoes' in df.columns:
        df['BPI_Acoes'] = pd.to_numeric(df['BPI_Acoes'], errors='coerce').fillna(0)
    if 'Total' in df.columns:
        df['Total'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)
    
    print("\n📋 Dados preparados para importação:")
    print(df.head())
    
    """abre um cursor para a inserçao dos dados """
    conn = connect_to_sql()
    create_table_if_not_exists(table_name, conn)
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        values = tuple(row)
        placeholders = ', '.join(['?' for _ in values])
        columns = ', '.join(df.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            cursor.execute(sql, values)
        except pyodbc.Error as e:
            """Se houver algum erro """
            print(f"Erro ao inserir linha {values}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n✅ Dados importados com sucesso para a tabela {table_name}")

# Configuração
document_path = "C:/Users/pmcmo/Documents/Bring_Contribuicoes_BPI_2024.12.xls"
sheet_name = "Mapa contribuições"
table_name = "Tabela_Contribuicoes"

import_xlsx_to_sql(document_path, sheet_name, table_name)
