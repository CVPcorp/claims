import duckdb
import pandas as pd

file_path = "data/icd/gem_i9diag.txt"
colspecs = [(0, 5), (5, 14)]
col_names = ["icd9", "icd10"]

df = pd.read_fwf(file_path, colspecs=colspecs, names=col_names, dtype=str)

duckdb_file = "claims.duckdb"
conn = duckdb.connect(duckdb_file)

conn.register("temp_df",df)

conn.execute("DROP TABLE IF EXISTS icd_diag_xwalk")
conn.execute("DROP TABLE IF EXISTS inpatient_claims_icd10")

conn.execute("CREATE TABLE icd_diag_xwalk AS SELECT * FROM temp_df")
conn.execute("CREATE TABLE inpatient_claims_icd10  as select c.DESYNPUF_ID, c.CLM_ID ,c.CLM_FROM_DT, c.CLM_THRU_DT, c.CLM_PMT_AMT, c.CLM_ADMSN_DT,c.CLM_UTLZTN_DAY_CNT,c.NCH_BENE_DSCHRG_DT ,c.CLM_DRG_CD ,c.ICD9_DGNS_CD_1,i.icd10 as ICD10_DGNS_CODE FROM INPATIENT_CLAIMS c left join (select icd9, min(icd10) as icd10 from icd_diag_xwalk group by icd9) i ON c.ICD9_DGNS_CD_1 = i.icd9")

conn.close()
print("Database connection closed.")