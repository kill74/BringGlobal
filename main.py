import pandas as pd
import numpy as np

def load_data(file_path):
    """
    Load data from a CSV file.

    Parameters:
    file_path (str): Path to the CSV file.

    Returns:
    pd.DataFrame: Loaded data.
    """
    try:
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        return None
    
def preprocess_data(data):
    """
    Preprocess the data by filling missing values and normalizing.

    Parameters:
    data (pd.DataFrame): Input data.

    Returns:
    pd.DataFrame: Preprocessed data.
    """
    # Fill missing values with the mean of each column
    data.fillna(data.mean(), inplace=True)
    
    # Normalize the data
    data = (data - data.min()) / (data.max() - data.min())
    
    return data
        

