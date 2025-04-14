import duckdb
import os

# Connect to the existing DuckDB database
conn = duckdb.connect('claims.duckdb')

# Check if the state table exists
table_exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='gender'").fetchone()

if not table_exists:
    print("Error: The gender table does not exist. Please run create_claims_db.py first.")
    conn.close()
    exit(1)

# Import state CSV file using duckdb.read_csv()
try:
    # Clear existing data
    conn.execute("DELETE FROM gender")
    print("Existing data cleared from gender table")

    conn.execute(f"""
        INSERT INTO gender VALUES (1,'Male'),(2,'Female');
    """)
    print(f"Data imported successfully for gender")

    # Add count(*) check
    row_count = conn.execute("SELECT COUNT(*) FROM gender").fetchone()[0]
    print(f"Total number of rows in gender table: {row_count}")

except Exception as e:
    print(f"Error importing data: {str(e)}")

# Close the database connection
conn.close()
print("Database connection closed.")
