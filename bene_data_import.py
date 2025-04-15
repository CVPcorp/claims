import duckdb
import os
import zipfile
import shutil

# Connect to the existing DuckDB database
conn = duckdb.connect('claims.duckdb')

# Check if the beneficiary_summary table exists
table_exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='beneficiary_summary'").fetchone()

if not table_exists:
    print("Error: The beneficiary_summary table does not exist. Please run create_claims_db.py first.")
    conn.close()
    exit(1)

# Define paths
bene_folder = './data/bene'
zip_file_path = os.path.join(bene_folder, 'bene.zip')

# Unzip the bene.zip file if it exists
if os.path.exists(zip_file_path):
    print(f"Found zip file: {zip_file_path}")
    print("Extracting CSV files...")
    
    # Create a temporary extraction directory
    temp_extract_dir = os.path.join(bene_folder, 'temp_extract')
    if os.path.exists(temp_extract_dir):
        shutil.rmtree(temp_extract_dir)
    os.makedirs(temp_extract_dir, exist_ok=True)
    
    # Extract all files from the zip
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(temp_extract_dir)
    
    # Move CSV files to the bene folder
    for file in os.listdir(temp_extract_dir):
        if file.endswith('.csv'):
            source_path = os.path.join(temp_extract_dir, file)
            dest_path = os.path.join(bene_folder, file)
            shutil.move(source_path, dest_path)
            print(f"Extracted: {file}")
    
    # Clean up the temporary directory
    shutil.rmtree(temp_extract_dir)
    print("Extraction complete")
else:
    print(f"Warning: Zip file not found at {zip_file_path}")
    print("Proceeding with existing CSV files in the directory")

# Import all beneficiary CSV files using duckdb.read_csv()
try:
    # Clear existing data
    conn.execute("DELETE FROM beneficiary_summary")
    print("Existing data cleared from beneficiary_summary table")

    # Get all CSV files in the bene folder
    csv_files = [f for f in os.listdir(bene_folder) if f.endswith('.csv')]
    
    if not csv_files:
        print("No CSV files found in the bene folder")
    else:
        print(f"Found {len(csv_files)} CSV files to import")

    for csv_file in csv_files:
        file_path = os.path.join(bene_folder, csv_file)
        conn.execute(f"""
            INSERT INTO beneficiary_summary
            SELECT 
                DESYNPUF_ID,
                CAST(STRPTIME(CAST(BENE_BIRTH_DT AS VARCHAR), '%Y%m%d') AS DATE) AS BENE_BIRTH_DT,
                CAST(STRPTIME(CAST(BENE_DEATH_DT AS VARCHAR), '%Y%m%d') AS DATE) AS BENE_DEATH_DT,
                CAST(BENE_SEX_IDENT_CD AS CHAR(1)),
                CAST(BENE_RACE_CD AS CHAR(1)),
                CAST(BENE_ESRD_IND AS CHAR(1)),
                CAST(SP_STATE_CODE AS CHAR(2)),
                CAST(BENE_COUNTY_CD AS CHAR(3)),
                CAST(BENE_HI_CVRAGE_TOT_MONS AS INTEGER),
                CAST(BENE_SMI_CVRAGE_TOT_MONS AS INTEGER),
                CAST(BENE_HMO_CVRAGE_TOT_MONS AS INTEGER),
                CAST(PLAN_CVRG_MOS_NUM AS INTEGER),
                CAST(SP_ALZHDMTA AS INTEGER),
                CAST(SP_CHF AS INTEGER),
                CAST(SP_CHRNKIDN AS INTEGER),
                CAST(SP_CNCR AS INTEGER),
                CAST(SP_COPD AS INTEGER),
                CAST(SP_DEPRESSN AS INTEGER),
                CAST(SP_DIABETES AS INTEGER),
                CAST(SP_ISCHMCHT AS INTEGER),
                CAST(SP_OSTEOPRS AS INTEGER),
                CAST(SP_RA_OA AS INTEGER),
                CAST(SP_STRKETIA AS INTEGER),
                CAST(MEDREIMB_IP AS DECIMAL(13,2)),
                CAST(BENRES_IP AS DECIMAL(13,2)),
                CAST(PPPYMT_IP AS DECIMAL(13,2)),
                CAST(MEDREIMB_OP AS DECIMAL(13,2)),
                CAST(BENRES_OP AS DECIMAL(13,2)),
                CAST(PPPYMT_OP AS DECIMAL(13,2)),
                CAST(MEDREIMB_CAR AS DECIMAL(13,2)),
                CAST(BENRES_CAR AS DECIMAL(13,2))
            FROM read_csv('{file_path}', auto_detect=true)
            ORDER BY DESYNPUF_ID
        """)
        print(f"Data imported successfully from {csv_file}")

    # Add count(*) check
    row_count = conn.execute("SELECT COUNT(*) FROM beneficiary_summary").fetchone()[0]
    print(f"Total number of rows in beneficiary_summary table: {row_count}")

except Exception as e:
    print(f"Error importing data: {str(e)}")

# Close the database connection
conn.close()
print("Database connection closed.")
