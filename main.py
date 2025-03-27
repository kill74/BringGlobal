import pandas as pd
import sqlite3
from pathlib import Path

class BookDatabase:
    def __init__(self, db_path='books.db'):
        """Initialize the database connection."""
        self.db_path = db_path
        self.conn = None
        self.create_connection()
        
    def create_connection(self):
        """Create a database connection to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            print(f"Connected to database: {self.db_path}")
            self._create_tables()
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
    
    def _create_tables(self):
        """Create tables if they don't exist."""
        create_books_table = '''
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            author TEXT,
            year INTEGER,
            genre TEXT,
            isbn TEXT,
            added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
        try:
            cursor = self.conn.cursor()
            cursor.execute(create_books_table)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")
    
    def import_from_excel(self, excel_path, sheet_name=0):
        """Import books from Excel file into the database."""
        try:
            # Validate file exists
            file_path = Path(excel_path)
            if not file_path.exists():
                print(f"File not found: {excel_path}")
                return False
            
            # Read Excel file
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            print(f"Read {len(df)} records from Excel file")
            
            # Check required columns
            required_columns = ['title']
            for col in required_columns:
                if col not in df.columns:
                    print(f"Required column '{col}' not found in Excel file")
                    return False
            
            # Clean and prepare data
            df = df.fillna('')
            
            # Insert into database
            cursor = self.conn.cursor()
            for _, row in df.iterrows():
                # Prepare data for insertion
                columns = []
                values = []
                for col in df.columns:
                    columns.append(col)
                    values.append(row[col])
                
                # Create placeholders for SQL
                placeholders = ", ".join(["?"] * len(columns))
                columns_str = ", ".join(columns)
                
                # Insert statement
                sql = f"INSERT INTO books ({columns_str}) VALUES ({placeholders})"
                cursor.execute(sql, values)
            
            self.conn.commit()
            print(f"Successfully imported {len(df)} books into the database")
            return True
        except Exception as e:
            print(f"Error importing from Excel: {e}")
            return False
    
    def get_all_books(self):
        """Retrieve all books from the database."""
        try:
            df = pd.read_sql_query("SELECT * FROM books", self.conn)
            return df
        except sqlite3.Error as e:
            print(f"Error retrieving books: {e}")
            return pd.DataFrame()
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed")

# Example usage
if __name__ == "__main__":
    db = BookDatabase()
    
    # Example: Import books from Excel file
    excel_file = "books.xlsx"  
    db.import_from_excel(excel_file)
    
    # Example: Retrieve all books
    books_df = db.get_all_books()
    print("\nBooks in database:")
    print(books_df)
    
    # Close connection
    db.close()

