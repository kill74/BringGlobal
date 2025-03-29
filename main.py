import pandas as pd
import numpy as np
from sqlalchemy import create_engine

#For this to work you will need to pass your excel file to CSV format
# and change the file path to the location of your file

#This will read the csv file 
df = pd.read_csv('data.csv')

print(df)

#If the csv file is empty it will return an empty dataframe
if(df.empty):
    print("DataFrame is empty")

#This will make a conection to the database
connection_string = 'database_conection'

#Create the engine
engine = create_engine(connection_string)

#If necessary we can specify the table name
#table_name = 'table_name'



