import duckdb
import os

# Connect to the existing DuckDB database
conn = duckdb.connect('claims.duckdb')

# Check if the state table exists
table_exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='state'").fetchone()

if not table_exists:
    print("Error: The state table does not exist. Please run create_claims_db.py first.")
    conn.close()
    exit(1)

# Import state CSV file using duckdb.read_csv()
try:
    # Clear existing data
    conn.execute("DELETE FROM state")
    print("Existing data cleared from state table")

    # Get the CSV file in the state folder
    state_folder = './data/state'
    csv_file = 'state.csv'
    file_path = os.path.join(state_folder, csv_file)

    conn.execute(f"""
        INSERT INTO state
        SELECT 
            CAST(state_name AS VARCHAR(50)),
            CAST(sp_state_code AS VARCHAR(2)),
            CAST(state_abbr AS VARCHAR(2))
        FROM read_csv('{file_path}', auto_detect=true)
    """)
    print(f"Data imported successfully from {csv_file}")

    # Add count(*) check
    row_count = conn.execute("SELECT COUNT(*) FROM state").fetchone()[0]
    print(f"Total number of rows in state table: {row_count}")

except Exception as e:
    print(f"Error importing data: {str(e)}")

# Close the database connection
conn.close()
print("Database connection closed.")