import pandas as pd
import numpy as np

#For this to work you will need to pass your excel file to CSV format
# and change the file path to the location of your file

#This will read the csv file 
df = pd.read_csv('data.csv')

#If the csv file is empty it will return an empty dataframe
if(df.empty):
    print("DataFrame is empty")

