import duckdb

try:
    # Connect to the existing DuckDB database
    conn = duckdb.connect('claims.duckdb')
    print("Successfully connected to the database.")

    # Check if the inpatient_claims table exists
    table_check = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='inpatient_claims'").fetchone()
    if not table_check:
        print("Error: The inpatient_claims table does not exist.")
        conn.close()
        exit(1)
    else:
        print("The inpatient_claims table exists.")

    # Get the count of rows in the inpatient_claims table
    row_count = conn.execute("SELECT COUNT(*) FROM inpatient_claims").fetchone()[0]
    print(f"Total number of rows in inpatient_claims table: {row_count}")

    # SQL query to create the all_cause_readmission view
    create_view_query = """
    CREATE OR REPLACE VIEW all_cause_readmission AS
    WITH ordered_claims AS (
  SELECT 
    DESYNPUF_ID,
    CLM_ID,
    CLM_ADMSN_DT,
    NCH_BENE_DSCHRG_DT,
    ICD9_DGNS_CD_1,
    ICD9_DGNS_CD_2,
    ICD9_DGNS_CD_3,
    ICD9_DGNS_CD_4,
    ICD9_DGNS_CD_5,
    ICD9_DGNS_CD_6,
    ICD9_DGNS_CD_7,
    ICD9_DGNS_CD_8,
    ICD9_DGNS_CD_9,
    ICD9_PRCDR_CD_1,
    LAG(NCH_BENE_DSCHRG_DT) OVER (PARTITION BY DESYNPUF_ID ORDER BY CLM_ADMSN_DT) AS prev_discharge_date,
    LAG(ICD9_DGNS_CD_1) OVER (PARTITION BY DESYNPUF_ID ORDER BY CLM_ADMSN_DT) AS prev_diagnosis
  FROM main.inpatient_claims
  WHERE NCH_BENE_DSCHRG_DT IS NOT NULL
    AND CLM_ADMSN_DT IS NOT NULL
),
readmissions AS (
  SELECT 
    DESYNPUF_ID,
    CLM_ID,
    CLM_ADMSN_DT,
    NCH_BENE_DSCHRG_DT,
    ICD9_DGNS_CD_1,
    ICD9_PRCDR_CD_1,    
    prev_discharge_date,
    prev_diagnosis,
    CASE 
      WHEN CLM_ADMSN_DT - prev_discharge_date <= 30 
      AND CLM_ADMSN_DT <> prev_discharge_date
      AND ICD9_DGNS_CD_1 != 'V57' -- Exclude rehabilitation admissions
      AND ICD9_DGNS_CD_1 NOT LIKE '29%' -- Exclude psychiatric admissions
      AND ICD9_DGNS_CD_1 NOT LIKE '30%'
      AND ICD9_DGNS_CD_1 NOT LIKE '31%'
      AND 'V642' NOT IN (ICD9_DGNS_CD_1, ICD9_DGNS_CD_2, ICD9_DGNS_CD_3, ICD9_DGNS_CD_4, ICD9_DGNS_CD_5, ICD9_DGNS_CD_6, ICD9_DGNS_CD_7, ICD9_DGNS_CD_8, ICD9_DGNS_CD_9) -- Exclude left against medical advice
      THEN 1
      ELSE 0
    END AS is_readmission
  FROM ordered_claims
)
SELECT 
  DESYNPUF_ID,
  CLM_ID,
  CLM_ADMSN_DT,
  NCH_BENE_DSCHRG_DT,
  ICD9_DGNS_CD_1,
  ICD9_PRCDR_CD_1,  
  prev_discharge_date,
  prev_diagnosis,
  is_readmission
FROM readmissions
WHERE is_readmission = 1;
    """

    print("Creating the all_cause_readmission view...")
    conn.execute(create_view_query)
    print("View created successfully.")

    # Query to select from the view
    select_query = "SELECT * FROM all_cause_readmission LIMIT 10"

    print("Executing the readmissions query...")
    # Execute the query and fetch results
    results = conn.execute(select_query).fetchall()

    # Print the results
    print("\nReadmissions within 30 days (showing up to 10 results):")
    print("DESYNPUF_ID | CLM_ID | CLM_ADMSN_DT | NCH_BENE_DSCHRG_DT | ICD9_DGNS_CD_1 | prev_discharge_date | prev_diagnosis | is_readmission")
    print("-" * 140)
    for row in results:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} | {row[7]}")

    # Print the total number of readmissions
    total_readmissions = conn.execute("SELECT COUNT(*) FROM all_cause_readmission").fetchone()[0]
    print(f"\nTotal number of readmissions: {total_readmissions}")

except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Close the database connection
    if 'conn' in locals():
        conn.close()
        print("Database connection closed.")
