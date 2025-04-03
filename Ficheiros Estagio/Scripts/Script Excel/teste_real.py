import pandas as pd
import pyodbc
import os

def connect_to_sql():
    """Conecta ao banco de dados SQL Server remoto."""
    server = '10.11.30.20'
    database = ''  # Ou o nome do seu banco de dados específico
    username = 'pedro.mouro'
    password = 'DlyF94%"ec2'  # Substitua pela senha real
    
    connection_string = f"""
    DRIVER={{SQL Server}};
    SERVER={server};
    DATABASE={database};
    UID={username};
    PWD={password};
    """
    
    try:
        conn = pyodbc.connect(connection_string)
        print("✅ Conexão com o servidor estabelecida com sucesso")
        return conn
    except pyodbc.Error as e:
        print(f"❌ Falha na conexão com o servidor: {e}")
        raise

def create_table_if_not_exists(table_name, conn):
    """Cria a tabela no SQL Server se ela não existir."""
    cursor = conn.cursor()
    
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
    BEGIN
        CREATE TABLE {table_name} (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            Empresa NVARCHAR(255),
            Mês_Processamento NVARCHAR(50),
            Tipo_Novo_ou_Reforço NVARCHAR(50),
            Numero_Fiscal NVARCHAR(50),
            Nome_do_aderente NVARCHAR(255),
            Num_ID NVARCHAR(50),
            Email NVARCHAR(255),
            Premio_Unique_Reforco FLOAT,
            Custo_Apolice FLOAT,
            Comissao_de_subscricao FLOAT,
            Premio_Total FLOAT,
            Data_Importacao DATETIME DEFAULT GETDATE()
        )
    END
    """
    
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"✅ Tabela {table_name} verificada/criada com sucesso")
    except pyodbc.Error as e:
        print(f"❌ Erro ao criar tabela: {e}")
        raise
    finally:
        cursor.close()

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
    """Importa um arquivo Excel para o SQL Server remoto."""
    if not os.path.exists(excel_file):
        raise FileNotFoundError(f"Arquivo não encontrado: {excel_file}")
    
    try:
        # Leitura do Excel
        df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=2, dtype=str)
        df = normalize_column_names(df)

        # Mapeamento de colunas
        column_mapping = {
            "Empresa": ["Empresa"],
            "Mês_Processamento": ["Mês_Processamento"],
            "Tipo_Novo_ou_Reforço": ["Tipo_novo_ou_reforçoNR", "TipoNovoOuReforco"],
            "Numero_Fiscal": ["NumeroFiscal"],
            "Nome_do_aderente": ["Nome_do_aderente", "NomeAderente"],
            "Num_ID": ["nºid", "NumID"],
            "Email": ["email"],
            "Premio_Unique_Reforco": ["Prémio_ÚnicoReforço", "PremioUnicoReforco"],
            "Custo_Apolice": ["Custo_Apolice", "CustoApolice"],
            "Comissao_de_subscricao": ["Comissão_de_subscrição", "ComissaoSubscricao"],
            "Premio_Total": ["Prémio_Total", "PremioTotal"]
        }

        selected_columns = {}
        for key, possible_names in column_mapping.items():
            selected_columns[key] = find_column(df, possible_names)

        selected_columns = {k: v for k, v in selected_columns.items() if v is not None}
        
        if not selected_columns:
            raise ValueError("Nenhuma coluna correspondente encontrada")

        df = df[list(selected_columns.values())]
        df.columns = list(selected_columns.keys())

        # Conversão de tipos
        numeric_cols = ["Premio_Unique_Reforco", "Custo_Apolice", "Comissao_de_subscricao", "Premio_Total"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        text_cols = ["Empresa", "Mês_Processamento", "Tipo_Novo_ou_Reforço", 
                    "Numero_Fiscal", "Nome_do_aderente", "Num_ID", "Email"]
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

        # Conexão e importação
        conn = connect_to_sql()
        create_table_if_not_exists(table_name, conn)
        
        cursor = conn.cursor()
        placeholders = ', '.join(['?'] * len(df.columns))
        columns = ', '.join(df.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        batch_size = 100  # Insere em lotes para melhor performance
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            try:
                cursor.executemany(sql, batch.values.tolist())
                conn.commit()
                print(f"✅ Lote {i//batch_size + 1} inserido: {len(batch)} registros")
            except pyodbc.Error as e:
                conn.rollback()
                print(f"❌ Erro no lote {i//batch_size + 1}: {e}")
                # Insere linha por linha para identificar o problema
                for idx, row in batch.iterrows():
                    try:
                        cursor.execute(sql, tuple(row))
                        conn.commit()
                    except pyodbc.Error as e:
                        print(f"❌ Erro na linha {idx}: {row.to_dict()} - {e}")
                        conn.rollback()
        
    except Exception as e:
        print(f"❌ Erro durante a importação: {e}")
        raise
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

# Configuração
if __name__ == "__main__":
    document_path = "C:/Users/tiago/Documents/Bring_SV_Capitalizac╠ºa╠âo_2024.12_Final.xlsx"
    sheet_name = "SV_Capitalização"
    table_name = "Tabela_Capitalizacao"
    
    try:
        import_xlsx_to_sql(document_path, sheet_name, table_name)
    except Exception as e:
        print(f"❌ Processo terminou com erro: {e}")