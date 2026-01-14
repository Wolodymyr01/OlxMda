"""
CSV to SQL Server importer
Reads CSV with proper handling of embedded commas and imports to olx_house_price table.
Handles locale-independent number parsing and robust error handling.
"""

import csv
import pyodbc
import sys
from pathlib import Path
from decimal import InvalidOperation

# Configuration
CSV_FILE = "olx_house_price_Q122.csv"
SERVER = "."
DATABASE = "OlxQa"
TABLE_NAME = "[dbo].[olx_house_price]"

# Column mappings and type conversions
COLUMN_DEFS = {
    "price": {"sql_type": "float", "nullable": False, "converter": lambda x: _parse_float(x)},
    "price_per_meter": {"sql_type": "float", "nullable": False, "converter": lambda x: _parse_float(x)},
    "offer_type": {"sql_type": "nvarchar(50)", "nullable": False, "converter": lambda x: x[:50] if x else None},
    "floor": {"sql_type": "tinyint", "nullable": True, "converter": lambda x: _parse_tinyint(x)},
    "area": {"sql_type": "float", "nullable": True, "converter": lambda x: _parse_float(x)},
    "rooms": {"sql_type": "tinyint", "nullable": False, "converter": lambda x: _parse_tinyint(x)},
    "offer_type_of_building": {"sql_type": "nvarchar(50)", "nullable": True, "converter": lambda x: x[:50] if x else None},
    "market": {"sql_type": "nvarchar(50)", "nullable": False, "converter": lambda x: x[:50] if x else None},
    "city_name": {"sql_type": "nvarchar(50)", "nullable": False, "converter": lambda x: x[:50] if x else None},
    "voivodeship": {"sql_type": "nvarchar(50)", "nullable": False, "converter": lambda x: x[:50] if x else None},
    "month": {"sql_type": "nvarchar(50)", "nullable": False, "converter": lambda x: x[:50] if x else None},
    "year": {"sql_type": "smallint", "nullable": False, "converter": lambda x: _parse_int(x)},
    "population": {"sql_type": "int", "nullable": False, "converter": lambda x: _parse_int(x)},
    "longitude": {"sql_type": "float", "nullable": False, "converter": lambda x: _parse_float(x)},
    "latitude": {"sql_type": "float", "nullable": False, "converter": lambda x: _parse_float(x)},
}

REQUIRED_FIELDS = ["price", "price_per_meter", "rooms", "year", "population", "longitude", "latitude", "offer_type", "market", "city_name", "voivodeship"]


def _parse_float(value):
    """Parse float with locale-independent handling of commas and dots."""
    if not value or not isinstance(value, str):
        return None
    
    value = value.strip()
    if not value:
        return None
    
    try:
        # Replace comma with period (handle European decimal separator)
        normalized = value.replace(",", ".")
        return float(normalized)
    except (ValueError, InvalidOperation):
        return None


def _parse_int(value):
    """Parse integer with robust error handling."""
    if not value or not isinstance(value, str):
        return None
    
    value = value.strip()
    if not value:
        return None
    
    try:
        # Remove any decimal part if present
        return int(float(value.replace(",", ".")))
    except (ValueError, InvalidOperation):
        return None


def _parse_tinyint(value):
    """Parse tinyint (0-255) with validation."""
    if not value or not isinstance(value, str):
        return None
    
    value = value.strip()
    if not value:
        return None
    
    try:
        int_val = int(float(value.replace(",", ".")))
        if 0 <= int_val <= 255:
            return int_val
        return None
    except (ValueError, InvalidOperation):
        return None


def read_csv(filepath):
    """Read CSV file with proper handling of embedded commas."""
    print(f"Reading CSV file: {filepath}")
    
    if not Path(filepath).exists():
        raise FileNotFoundError(f"CSV file not found: {filepath}")
    
    rows = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):  # start=2 because row 1 is header
            rows.append((i, row))
    
    print(f"✓ Read {len(rows)} rows")
    return rows


def validate_and_convert_row(row_num, row):
    """Validate and convert row data. Returns (converted_row, is_valid, error_msg, raw_values)."""
    converted = {}
    errors = []
    raw_values = {}
    
    for col_name, col_def in COLUMN_DEFS.items():
        raw_value = row.get(col_name, "").strip() if col_name in row else ""
        raw_values[col_name] = raw_value
        
        # Apply converter
        try:
            converted_value = col_def["converter"](raw_value)
        except Exception as e:
            converted_value = None
            errors.append(f"  {col_name}: {str(e)}")
        
        # Check if required field has valid value
        if col_name in REQUIRED_FIELDS:
            if converted_value is None:
                errors.append(f"  {col_name}: Missing or invalid (raw value: '{raw_value}')")
        
        converted[col_name] = converted_value
    
    is_valid = len(errors) == 0
    error_msg = "\n".join(errors) if errors else None
    
    return converted, is_valid, error_msg, raw_values


def insert_rows(cursor, rows):
    """Insert converted rows into database."""
    print("\nValidating and inserting data...")
    
    inserted = 0
    rejected = 0
    rejected_samples = []
    
    for row_num, row in rows:
        converted, is_valid, error_msg, raw_values = validate_and_convert_row(row_num, row)
        
        if not is_valid:
            rejected += 1
            if len(rejected_samples) < 20:
                rejected_samples.append({
                    "row": row_num,
                    "errors": error_msg,
                    "raw_values": raw_values
                })
            continue
        
        # Insert row
        insert_sql = f"""
        INSERT INTO {TABLE_NAME} (
            [price], [price_per_meter], [offer_type], [floor], [area], [rooms],
            [offer_type_of_building], [market], [city_name], [voivodeship], [month], [year],
            [population], [longitude], [latitude]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        values = (
            converted["price"],
            converted["price_per_meter"],
            converted["offer_type"],
            converted["floor"],
            converted["area"],
            converted["rooms"],
            converted["offer_type_of_building"],
            converted["market"],
            converted["city_name"],
            converted["voivodeship"],
            converted["month"],
            converted["year"],
            converted["population"],
            converted["longitude"],
            converted["latitude"]
        )
        
        try:
            cursor.execute(insert_sql, values)
            inserted += 1
        except Exception as e:
            rejected += 1
            print(f"✗ Row {row_num}: {str(e)}")
    
    cursor.commit()
    
    print(f"\n✓ Import Summary:")
    print(f"  Rows inserted: {inserted}")
    print(f"  Rows rejected: {rejected}")
    print(f"  Total rows: {inserted + rejected}")
    
    if rejected_samples:
        print(f"\nSample of rejected rows (first {len(rejected_samples)}):")
        for sample in rejected_samples:
            print(f"\n  Row {sample['row']}")
            print(f"  Validation errors:")
            print(sample['errors'])
            print(f"  Raw values:")
            for field in REQUIRED_FIELDS:
                raw_val = sample['raw_values'].get(field, "")
                if not raw_val:
                    print(f"    {field}: [EMPTY]")
                elif len(raw_val) > 100:
                    print(f"    {field}: {raw_val[:100]}...")
                else:
                    print(f"    {field}: {raw_val}")


def execute_sql_file(cursor, filepath, description):
    """Execute SQL script from file."""
    print(f"\nExecuting {description}...")
    
    if not Path(filepath).exists():
        raise FileNotFoundError(f"SQL file not found: {filepath}")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Split by GO statements (SQL Server batch separator, case-insensitive)
        # GO is not valid T-SQL, it's a command tool directive, so we need to remove it
        import re
        batches = re.split(r'\ngo\s*$', sql_content, flags=re.MULTILINE | re.IGNORECASE)
        
        for batch in batches:
            batch = batch.strip()
            if batch:
                cursor.execute(batch)
        
        cursor.commit()
        print(f"✓ {description} completed")
    except Exception as e:
        print(f"✗ Error executing {description}: {e}")
        raise


def main():
    """Main execution."""
    try:
        print("=" * 70)
        print("CSV to SQL Server Importer")
        print("=" * 70)
        
        # Connect to SQL Server
        print(f"\nConnecting to SQL Server...")
        print(f"  Server: {SERVER}")
        print(f"  Database: {DATABASE}")
        
        conn_string = f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};Database={DATABASE};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        
        # Set locale-independent settings
        cursor.execute("SET LANGUAGE us_english")
        cursor.execute("SET DATEFORMAT mdy")
        cursor.execute("SET NOCOUNT ON")
        conn.commit()
        print("✓ Connected")
        
        # Execute table setup script
        execute_sql_file(cursor, "OlxImportTable.sql", "OlxImportTable.sql")
        
        # Read CSV
        rows = read_csv(CSV_FILE)
        
        # Insert data
        insert_rows(cursor, rows)
        
        # Execute schema script
        execute_sql_file(cursor, "OlxSchema.sql", "OlxSchema.sql")
        
        # Execute data script
        execute_sql_file(cursor, "OlxData.sql", "OlxData.sql")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 70)
        print("Import completed successfully!")
        print("=" * 70)
        
    except pyodbc.Error as e:
        print(f"\n✗ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
