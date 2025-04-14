import duckdb

def calculate_and_insert_readmission_rate():
    query = """
    INSERT OR REPLACE INTO readmission_rate (year, SP_STATE_CODE, BENE_SEX_IDENT_CD, readmissions, total_admissions, readmission_rate)
    WITH valid_claims AS (
        SELECT 
            ic.CLM_ID,
            ic.DESYNPUF_ID,
            ic.CLM_ADMSN_DT,
            EXTRACT(YEAR FROM ic.CLM_ADMSN_DT) AS year,
            LAG(ic.NCH_BENE_DSCHRG_DT) OVER (PARTITION BY ic.DESYNPUF_ID ORDER BY ic.CLM_ADMSN_DT) AS prev_discharge_date
        FROM 
            inpatient_claims ic
        WHERE 
            EXTRACT(YEAR FROM ic.CLM_ADMSN_DT) IN (2008, 2009, 2010)
    )
    SELECT
        vc.year,
        bs.SP_STATE_CODE,
        bs.BENE_SEX_IDENT_CD,
        COUNT(DISTINCT acr.CLM_ID) AS readmissions,
        COUNT(DISTINCT vc.CLM_ID) AS total_admissions,
        CAST(COUNT(DISTINCT acr.CLM_ID) AS FLOAT) / COUNT(DISTINCT vc.CLM_ID) AS readmission_rate
    FROM
        valid_claims vc
    JOIN
        beneficiary_summary bs ON vc.DESYNPUF_ID = bs.DESYNPUF_ID
    LEFT JOIN
        all_cause_readmission acr ON vc.CLM_ID = acr.CLM_ID
    WHERE
        (vc.CLM_ADMSN_DT - vc.prev_discharge_date > 30 OR vc.prev_discharge_date IS NULL)
        AND NOT (
            -- Exclude planned readmissions (transplants)
            vc.CLM_ID IN (
                SELECT CLM_ID
                FROM inpatient_claims
                WHERE ICD9_DGNS_CD_1 LIKE 'V42%'  -- Organ replacements
                OR ICD9_DGNS_CD_1 IN ('5280', '5281', '5282')  -- Pancreas transplant
                OR ICD9_DGNS_CD_1 = '3751'  -- Heart transplant
                OR ICD9_DGNS_CD_1 IN ('3350', '3351', '3352')  -- Lung transplant
                OR ICD9_DGNS_CD_1 IN ('4697')  -- Intestine transplant
                OR ICD9_DGNS_CD_1 IN ('4100', '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', '4109')  -- Bone marrow transplant
                OR ICD9_DGNS_CD_1 IN ('5561', '5569')  -- Kidney transplant
                OR ICD9_DGNS_CD_1 IN ('5051', '5059')  -- Liver transplant
            )
        )
    GROUP BY
        vc.year, bs.SP_STATE_CODE, bs.BENE_SEX_IDENT_CD
    ORDER BY
        vc.year, bs.SP_STATE_CODE, bs.BENE_SEX_IDENT_CD
    """
    conn.execute(query)
    conn.commit()

# Connect to the existing DuckDB database
conn = duckdb.connect('claims.duckdb')
print("Successfully connected to the database.")

try:
    # Calculate and insert readmission rates for all years at once
    print("Calculating and inserting readmission rates for all years...")
    calculate_and_insert_readmission_rate()
    print("Readmission rates for all years have been calculated and inserted into the readmission_rate table.")

    # Verify the inserted data
    print("\nVerifying inserted data:")
    verification_query = """
    SELECT year, SP_STATE_CODE, BENE_SEX_IDENT_CD, readmissions, total_admissions, readmission_rate
    FROM readmission_rate
    ORDER BY year, SP_STATE_CODE, BENE_SEX_IDENT_CD
    LIMIT 10
    """
    verification_results = conn.execute(verification_query).fetchall()
    print("Year | State | Gender | Readmissions | Total Admissions | Readmission Rate")
    print("-" * 75)
    for row in verification_results:
        print(f"{row[0]:4} | {row[1]:5} | {row[2]:6} | {row[3]:12} | {row[4]:16} | {row[5]:.2%}")

except Exception as e:
    print(f"An error occurred: {str(e)}")

# Close the database connection
conn.close()
print("\nDatabase connection closed.")
