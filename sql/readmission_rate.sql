CREATE TABLE if not exists readmission_rate (
    year INTEGER,
    SP_STATE_CODE VARCHAR(2),
    BENE_SEX_IDENT_CD VARCHAR(1),
    readmissions INTEGER,
    total_admissions INTEGER,
    age_grp_id INTEGER DEFAULT 0,
    condition_type_id INTEGER DEFAULT 0,    
    readmission_rate FLOAT,
    PRIMARY KEY (year, SP_STATE_CODE, BENE_SEX_IDENT_CD,age_grp_id,condition_type_id)
);
