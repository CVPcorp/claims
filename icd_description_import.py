import duckdb
import pandas as pd

file_path = "data/icd/icd10cm-codes-April-2025.txt"

df = pd.read_fwf(file_path, widths=[7, 180], header=None, names=["icd10_cm_code", "description"],dtype=str)

duckdb_file = "claims.duckdb"
conn = duckdb.connect(duckdb_file)

conn.register("temp_df",df)

conn.execute("DROP TABLE IF EXISTS icd10_diag_desc")
conn.execute("CREATE TABLE icd10_diag_desc AS SELECT * FROM temp_df")

conn.close()
print("Database connection closed.")

