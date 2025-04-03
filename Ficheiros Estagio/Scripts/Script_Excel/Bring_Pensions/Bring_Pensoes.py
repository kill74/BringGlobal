import pandas as pd
import pyodbc
import os

def connect_to_sql():
    """Conecta ao banco de dados SQL Server."""
    connection_string = 'DRIVER={SQL Server};SERVER=localhost,1433;DATABASE=master;Trusted_Connection=yes;'
    return pyodbc.connect(connection_string)

def create_table_if_not_exists(table_name, conn):
    """Cria a tabela no SQL Server se ela não existir."""
    cursor = conn.cursor()
    
    # Criação da tabela com as colunas especificadas, caso não exista
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
    BEGIN
        CREATE TABLE {table_name} (
            NIF NVARCHAR(50),
            Numero_Empregado NVARCHAR(50),
            Empresa NVARCHAR(255),
            Nome NVARCHAR(255),
            Nacionalidade NVARCHAR(100),
            Email NVARCHAR(255),
            Documentacao_Digital NVARCHAR(MAX)
        )
    END
    """
    
    # Executa o comando SQL para criar a tabela
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()

def normalize_column_names(df):
    """Normaliza os nomes das colunas removendo espaços e caracteres especiais."""
    # Remove espaços extras e caracteres especiais dos nomes das colunas
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^\w]", "", regex=True)
    print("Colunas normalizadas:", df.columns.tolist())  
    return df

def find_column(df, possible_names):
    """Encontra a coluna correta baseada em nomes alternativos."""
    # Procura a coluna correspondente de acordo com os nomes  fornecidos
    for name in possible_names:
        if name in df.columns:
            return name
    return None

def import_xlsx_to_sql(excel_file, sheet_name, table_name):
    """Importa um arquivo Excel para o SQL Server."""
    # Verifica se o arquivo Excel e valido
    if not os.path.exists(excel_file):
        raise FileNotFoundError(f"Arquivo não encontrado: {excel_file}")
    
    # Tenta carregar o arquivo Excel, pulando as primeiras 5 linhas, se necessário
    for skip_rows in range(0, 5):
        try:
            # Lê o arquivo Excel e normaliza os nomes das colunas
            df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=skip_rows, dtype=str, engine="openpyxl")
            df = normalize_column_names(df)
            
            # Mapeamento de possíveis nomes das colunas no arquivo Excel
            column_mapping = {
                "NIF": ["NIF"],
                "Numero_Empregado": ["NEmpregado", "Nempregado", "NumeroEmpregado"],
                "Empresa": ["Empresa"],
                "Nome": ["Nome"],
                "Nacionalidade": ["Nacionalidade"],
                "Email": ["email", "Email"],
                "Documentacao_Digital": ["DomuntacaoDigital", "DocumentacaoDigital"]
            }
            
            # Busca e mapeia as colunas do DataFrame para as colunas desejadas
            selected_columns = {}
            for key, possible_names in column_mapping.items():
                selected_columns[key] = find_column(df, possible_names)
                print(f"🔍 Procurando {key}: {possible_names} -> {selected_columns[key]}")  # Depuração

            # Remove colunas que não foram encontradas no DataFrame
            selected_columns = {k: v for k, v in selected_columns.items() if v is not None}

            # Verifica se alguma coluna foi encontrada
            if not selected_columns:
                raise ValueError("Nenhuma coluna correspondente encontrada.")

            # Filtra o DataFrame para manter apenas as colunas encontradas e renomeia-as
            df = df[list(selected_columns.values())]
            df.columns = list(selected_columns.keys())

            # Substitui valores NaN por strings vazias
            df = df.fillna("")  

            print("📋 Dados a serem inseridos:")
            print(df.head())  # Exibe as primeiras linhas para verificação

            # Conecta a base de dados 
            conn = connect_to_sql()
            create_table_if_not_exists(table_name, conn)

            cursor = conn.cursor()

            # Insere os dados na base de dados 
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
            break  # Sai do loop se encontrar os dados corretos

        except Exception as e:
            print(f"Tentativa com skiprows={skip_rows} falhou: {e}")
            continue
    
  
    else:
        raise ValueError("Não foi possível encontrar os dados corretos no arquivo.")

document_path = "C:/Users/pmcmo/Documents/Bring_Pensoes_RV_2024.12.xlsx"
sheet_name = "contribuicoes"
table_name = "Tabela_Pensoes"

# Chama a função para importar os dados do Excel para o SQL Server
import_xlsx_to_sql(document_path, sheet_name, table_name)
