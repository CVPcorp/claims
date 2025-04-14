import duckdb
import os

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def execute_sql(conn, sql, table_name):
    try:
        conn.execute(sql)
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = result.fetchone()[0]
        print(f"{table_name} table created successfully. Current row count: {row_count}")
    except Exception as e:
        print(f"Error creating {table_name} table: {str(e)}")

# Create a connection to a new or existing DuckDB database file
conn = duckdb.connect('claims.duckdb')

# SQL file paths
sql_files = {
    'beneficiary_summary': 'sql/beneficiary_summary.sql',
    'inpatient_claims': 'sql/inpatient_claims.sql',
    'readmission_rate': 'sql/readmission_rate.sql',
    'state':'sql/state.sql',
    'gender':'sql/gender.sql'
}

# Execute SQL from files
for table_name, file_path in sql_files.items():
    sql = read_sql_file(file_path)
    execute_sql(conn, sql, table_name)

# Close the connection
conn.close()

print("Database creation completed.")
