# XML-to-SQL Converter

A Python script designed to extract data from XML files and load it into a SQL Server database using `pandas` and `pyodbc`.

## Features

- Reads data from XML files.
- Connects to SQL Server database using environment variables for credentials.
- Supports both SQL authentication and Windows authentication.
- Provides a preview of the data being processed.

## Requirements

- **Python**: 3.6 or higher
- **Libraries**:
  - `pandas`
  - `pyodbc`
  - `python-dotenv`

## Installation

1. Install the required dependencies:
   ```bash
   pip install pandas pyodbc python-dotenv
   ```

2. Create a `.env` file in the root directory with your database credentials:
   ```
   SERVER=your_server_name
   DATABASE=your_database_name
   USERNAME=your_username  # Optional for SQL authentication
   PASSWORD=your_password  # Optional for SQL authentication
   ```

## Usage

1. Prepare your XML file with the data you want to import.

2. Call the `xml_to_sql` function in `main.py`:
   ```python
   xml_to_sql("path/to/your/file.xml", "target_table_name")
   ```

3. Run the script:
   ```bash
   python main.py
   ```

### What the Script Does

1. Loads environment variables from the `.env` file.
2. Reads the XML file into a pandas DataFrame.
   - If there's an error reading the file, it displays an error message.
3. Connects to the SQL Server database using the provided credentials.
4. Uses Windows authentication if username and password are not provided.

## Customization

- **XML File Path**: Provide the path to your XML file as the first parameter to the `xml_to_sql` function.
- **Table Name**: Specify the target table name as the second parameter to the `xml_to_sql` function.
- **Database Credentials**: Update the `.env` file with your database connection details.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.