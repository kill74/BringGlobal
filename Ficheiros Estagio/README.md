# Data Import Tools

A Python toolkit for importing data from XML and Excel files into SQL Server databases.

## Features

- Supports both XML and Excel file formats
- Configurable via XML configuration files
- Automated table creation with defined schemas
- Flexible column mapping and data type handling
- Windows authentication support for SQL Server
- Built-in data normalization and validation

## Requirements

- **Python**: 3.6 or higher
- **Libraries**:
  - `pandas`
  - `pyodbc`
  - `openpyxl` (for Excel support)

## Installation

1. Install required dependencies:
   ```bash
   pip install pandas pyodbc openpyxl