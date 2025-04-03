import pandas as pd

file_path = r"C:\Users\tiago\Documents\Bring_SV_Capitalizacao.xlsx"
xl = pd.ExcelFile(file_path)

print("Abas disponíveis:", xl.sheet_names)