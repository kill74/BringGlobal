# BookPanda

A simple Python script that extracts book data from Excel files and loads it into a SQLite database using pandas.

## Requirements

- Python 3.6+
- pandas
- openpyxl (for Excel handling)
- sqlite3 (included in Python's standard library)

## Installation

1. Install dependencies:
   ```
   pip install pandas openpyxl
   ```

## Usage

1. Prepare your Excel file named `books.xlsx` with the following columns:
   - title (required)
   - author
   - year
   - genre
   - isbn

2. Run the script:
   ```
   python simple_book_importer.py
   ```

This will:
1. Read the books from your Excel file
2. Create a SQLite database file named `books.db`
3. Create a `books` table if it doesn't exist
4. Import all books from the Excel file

## Customizing

You can modify the script to use a different Excel file or database name by editing the source code. 