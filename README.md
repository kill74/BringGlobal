# BookPanda

BookPanda is a Python script designed to extract book data from CSV files and load it into a database using `pandas` and SQLAlchemy.

## Features

- Reads book data from a CSV file (`data.csv`).
- Handles missing or empty files gracefully.
- Connects to a database using SQLAlchemy.
- Provides flexibility to specify table, schema, or database names.

## Requirements

- **Python**: 3.6 or higher
- **Libraries**:
  - `pandas`
  - `numpy`
  - `SQLAlchemy`

## Installation

1. Install the required dependencies:
   ```bash
   pip install pandas numpy sqlalchemy
   ```

## Usage

1. Prepare a CSV file named `data.csv` with your data.

2. Update the `main.py` script if needed:
   - Change the `file_path` variable to the location of your CSV file.
   - Update the `connection_string` variable with your database connection details.

3. Run the script:
   ```bash
   python main.py
   ```

### What the Script Does

1. Checks if the specified CSV file exists.
   - If the file is missing, it displays an error message.
   - If the file is empty, it returns an empty DataFrame.
2. Reads the CSV file into a pandas DataFrame.
3. Connects to the database using the provided connection string.
4. (Optional) Allows customization of table, schema, or database names.

## Customization

- **File Path**: Modify the `file_path` variable in `main.py` to point to your CSV file.
- **Database Connection**: Update the `connection_string` variable with your database credentials.
- **Table/Schema/Database Names**: Uncomment and set the `table_name`, `schema_name`, or `database_name` variables in `main.py` as needed.

## Example CSV File

Ensure your CSV file (`data.csv`) is formatted correctly. Example:

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.