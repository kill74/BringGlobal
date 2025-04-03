import pandas as pd
import os

# This will load the variables from the .env file (Its more secure)
def load_env(file_path='.env'):
    """
    Reads a .env file and sets environment variables
    Parameters:
    file_path (str): Path to .env file (default: current directory)
    """
    #I could do this with a Try Except
    if os.path.exists(file_path):
        with open(file_path) as f: # Used the abreviation becouse its more ez to type hehe
            for line in f:
                # This will skip comments and empty lines
                if line.strip() and not line.startswith('#'):
                    # Split each line into key/value pairs
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
        print("Environment variables loaded successfully!")
    else:
        print(f"No .env file found at {file_path} - using system environment variables")

def read_xml(xml_path):
    """
    Function to read and display XML data
    Parameters:
    xml_path (str): Path to XML file
    """
    # Load environment variables
    load_env()
    
    # It will read the XML file and see if everything is good 
    # And it will print the first 5 lines of the XML file using "df.head"
    if os.path.exists(xml_path):
        print(f"\nReading XML file: {xml_path}")
        df = pd.read_xml(xml_path)
        print("XML data preview:")
        print(df.head())
        print("\nTotal number of rows:", len(df))
    # If something is wrong its gonna appear this error
    else:
        print(f"\nError: XML file not found at {xml_path}")
        return

# Run the script
if __name__ == "__main__":
    # Configuration 
    XML_FILE = "data.xml"      # Path to XML file
    # Aqui esta a criar a tabela para inserir a informacao do ficheiro XML

    # If we want specific tables we can do like this
    #Idk how can i do this 
    #TABLE_CONFIG = {
        
    #}
    
    read_xml(XML_FILE)