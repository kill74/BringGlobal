import pandas as pd

# Caminho para o Excel
file_path = r"C:\Users\pmcmo\Documents\BringGlobal-1\ExcelToXML_DB_Converter\Excel\2024\Bring_Contribuicoes_BPI_2024.12.xlsx"


df_preview = pd.read_excel(file_path, sheet_name="Mapa contribuições" , header=None, nrows=100, engine="openpyxl")


print(df_preview)


# lê a folha normalmente 
try:
    df = pd.read_excel(file_path, sheet_name="Mapa contribuições" ,nrows=100, dtype=str, engine="openpyxl")
  #  print("\nColunas detectadas:")
    print(df.columns.tolist())

except Exception as e:
    print(f"Erro ao ler o ficheiro: {e}")


