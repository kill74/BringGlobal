import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine 

#For this to work you will need to pass your excel file to CSV format
# and change the file path to the location of your file

#Make the program read Json Files too (TO DO)

#This will read the csv file, if the file does not exist it will return an error message
#If the file is empty it will return an empty dataframe
file_path = 'data.csv'

if not os.path.exists(file_path):
    print (f"File {file_path} does not exist.")
else:
    df = pd.read_csv('data.csv')
    df.dropna(inplace=True) # Remove rows with empty values


#"to.string()" is converting the DataFrame to an string representation
# and will print the entire DataFrame 
# print(df.to_string())

print(df)

#This will only be used if you want to see the last 5 rows of the DataSet
#print(df.tail()) 

#This will only be used to see information about the DataSet
#print(df.info())

#If the csv file is empty it will return an empty dataframe
if(df.empty):
    print (f"File {file_path} is empty.")
    
#This will make a conection to the database
connection_string = 'database_conection'

#Create the engine
engine = create_engine(connection_string)

#If necessary we can specify the table name
#table_name = 'table_name'

#If necessary we can specify the schema name
#schema_name = 'schema_name'

#If necessary we can specify the database name
#database_name = 'database_name'




