import duckdb
import os
import zipfile
import shutil

# Connect to the existing DuckDB database
conn = duckdb.connect('claims.duckdb')

# Check if the inpatient_claims table exists
table_exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='inpatient_claims'").fetchone()

if not table_exists:
    print("Error: The inpatient_claims table does not exist. Please run create_claims_db.py first.")
    conn.close()
    exit(1)

# Define paths
inpatient_folder = './data/inpatient'
zip_file_path = os.path.join(inpatient_folder, 'inpatient.zip')

# Unzip the inpatient.zip file if it exists
if os.path.exists(zip_file_path):
    print(f"Found zip file: {zip_file_path}")
    print("Extracting CSV files...")
    
    # Create a temporary extraction directory
    temp_extract_dir = os.path.join(inpatient_folder, 'temp_extract')
    if os.path.exists(temp_extract_dir):
        shutil.rmtree(temp_extract_dir)
    os.makedirs(temp_extract_dir, exist_ok=True)
    
    # Extract all files from the zip
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(temp_extract_dir)
    
    # Move CSV files to the inpatient folder
    for file in os.listdir(temp_extract_dir):
        if file.endswith('.csv'):
            source_path = os.path.join(temp_extract_dir, file)
            dest_path = os.path.join(inpatient_folder, file)
            shutil.move(source_path, dest_path)
            print(f"Extracted: {file}")
    
    # Clean up the temporary directory
    shutil.rmtree(temp_extract_dir)
    print("Extraction complete")
else:
    print(f"Warning: Zip file not found at {zip_file_path}")
    print("Proceeding with existing CSV files in the directory")

# Import all inpatient CSV files using duckdb.read_csv()
try:
    # Clear existing data
    conn.execute("DELETE FROM inpatient_claims")
    print("Existing data cleared from inpatient_claims table")

    # Get all CSV files in the inpatient folder
    csv_files = [f for f in os.listdir(inpatient_folder) if f.endswith('.csv')]
    
    if not csv_files:
        print("No CSV files found in the inpatient folder")
    else:
        print(f"Found {len(csv_files)} CSV files to import")

    for csv_file in csv_files:
        file_path = os.path.join(inpatient_folder, csv_file)
        conn.execute(f"""
            INSERT INTO inpatient_claims
            SELECT 
                DESYNPUF_ID,
                CLM_ID,
                CAST(SEGMENT AS INTEGER),
                CAST(STRPTIME(CAST(CLM_FROM_DT AS VARCHAR), '%Y%m%d') AS DATE) AS CLM_FROM_DT,
                CAST(STRPTIME(CAST(CLM_THRU_DT AS VARCHAR), '%Y%m%d') AS DATE) AS CLM_THRU_DT,
                PRVDR_NUM,
                CAST(CLM_PMT_AMT AS DECIMAL(10, 2)),
                CAST(NCH_PRMRY_PYR_CLM_PD_AMT AS DECIMAL(10, 2)),
                AT_PHYSN_NPI,
                OP_PHYSN_NPI,
                OT_PHYSN_NPI,
                CAST(STRPTIME(CAST(CLM_ADMSN_DT AS VARCHAR), '%Y%m%d') AS DATE) AS CLM_ADMSN_DT,
                ADMTNG_ICD9_DGNS_CD,
                CAST(CLM_PASS_THRU_PER_DIEM_AMT AS DECIMAL(10, 2)),
                CAST(NCH_BENE_IP_DDCTBL_AMT AS DECIMAL(10, 2)),
                CAST(NCH_BENE_PTA_COINSRNC_LBLTY_AM AS DECIMAL(10, 2)),
                CAST(NCH_BENE_BLOOD_DDCTBL_LBLTY_AM AS DECIMAL(10, 2)),
                CAST(CLM_UTLZTN_DAY_CNT AS INTEGER),
                CAST(STRPTIME(CAST(NCH_BENE_DSCHRG_DT AS VARCHAR), '%Y%m%d') AS DATE) AS NCH_BENE_DSCHRG_DT,
                CLM_DRG_CD,
                ICD9_DGNS_CD_1,
                ICD9_DGNS_CD_2,
                ICD9_DGNS_CD_3,
                ICD9_DGNS_CD_4,
                ICD9_DGNS_CD_5,
                ICD9_DGNS_CD_6,
                ICD9_DGNS_CD_7,
                ICD9_DGNS_CD_8,
                ICD9_DGNS_CD_9,
                ICD9_DGNS_CD_10,
                ICD9_PRCDR_CD_1,
                ICD9_PRCDR_CD_2,
                ICD9_PRCDR_CD_3,
                ICD9_PRCDR_CD_4,
                ICD9_PRCDR_CD_5,
                ICD9_PRCDR_CD_6,
                HCPCS_CD_1,
                HCPCS_CD_2,
                HCPCS_CD_3,
                HCPCS_CD_4,
                HCPCS_CD_5,
                HCPCS_CD_6,
                HCPCS_CD_7,
                HCPCS_CD_8,
                HCPCS_CD_9,
                HCPCS_CD_10,
                HCPCS_CD_11,
                HCPCS_CD_12,
                HCPCS_CD_13,
                HCPCS_CD_14,
                HCPCS_CD_15,
                HCPCS_CD_16,
                HCPCS_CD_17,
                HCPCS_CD_18,
                HCPCS_CD_19,
                HCPCS_CD_20,
                HCPCS_CD_21,
                HCPCS_CD_22,
                HCPCS_CD_23,
                HCPCS_CD_24,
                HCPCS_CD_25,
                HCPCS_CD_26,
                HCPCS_CD_27,
                HCPCS_CD_28,
                HCPCS_CD_29,
                HCPCS_CD_30,
                HCPCS_CD_31,
                HCPCS_CD_32,
                HCPCS_CD_33,
                HCPCS_CD_34,
                HCPCS_CD_35,
                HCPCS_CD_36,
                HCPCS_CD_37,
                HCPCS_CD_38,
                HCPCS_CD_39,
                HCPCS_CD_40,
                HCPCS_CD_41,
                HCPCS_CD_42,
                HCPCS_CD_43,
                HCPCS_CD_44,
                HCPCS_CD_45
            FROM read_csv('{file_path}', auto_detect=true)
            order by DESYNPUF_ID, CLM_ADMSN_DT
        """)
        print(f"Data imported successfully from {csv_file}")

    # Add count(*) check
    row_count = conn.execute("SELECT COUNT(*) FROM inpatient_claims").fetchone()[0]
    print(f"Total number of rows in inpatient_claims table: {row_count}")

except Exception as e:
    print(f"Error importing data: {str(e)}")

# Close the database connection
conn.close()
print("Database connection closed.")
