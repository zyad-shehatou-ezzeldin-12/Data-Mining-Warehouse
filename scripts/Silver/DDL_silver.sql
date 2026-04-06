/*
===============================================================================
DDL Script: Create Silver Tables — Sleep Apnea Data Warehouse
===============================================================================
Script Purpose:
    Creates all Silver layer tables for the Sleep Apnea DW domain, dropping
    existing tables if they already exist. Tables are built from 10 Bronze
    source datasets that cover:
        - Sleep health & lifestyle (patient-level)
        - Sleep apnea clinical detail (PSG/sensor signals)
        - Diabetes comorbidity
        - Hypertension risk
        - Population-level aggregates (2016 epidemiology)

Data Sources (Bronze → Silver mapping):
    bronze.sleep_health_lifestyle        → silver.sleep_patient_profile
    bronze.sleep_data_sampled            → silver.sleep_patient_profile  (union / deduplicate)
    bronze.enhanced_sleep_sdb            → silver.sleep_apnea_clinical
    bronze.sleep_apnea_dataset           → silver.sleep_apnea_risk
    bronze.diabetes_data                 → silver.diabetes_patient
    bronze.diabetes_dataset_10k          → silver.diabetes_risk_factors
    bronze.diabetes_prediction           → silver.diabetes_prediction
    bronze.hypertension_risk             → silver.hypertension_risk
    bronze.diabetes_all_2016             → silver.epidemiology_diabetes_2016
    bronze.diabetes_hypertension_2016    → silver.epidemiology_diab_hyp_2016

===============================================================================
*/

-- ============================================================
-- 1. DIMENSION: Patient Profile (Sleep Health + Sampled Data)
--    Source: bronze.sleep_health_lifestyle + bronze.sleep_data_sampled
--    Grain : One row per unique Person ID
-- ============================================================
IF OBJECT_ID('silver.sleep_patient_profile', 'U') IS NOT NULL
    DROP TABLE silver.sleep_patient_profile;
GO

CREATE TABLE silver.sleep_patient_profile (
    person_id               INT,            -- Natural key from source
    gender                  NVARCHAR(10),   -- 'Male' / 'Female' — normalized
    age                     INT,
    occupation              NVARCHAR(100),
    sleep_duration_hrs      DECIMAL(5,2),   -- Hours; validated 3–12 range
    quality_of_sleep        INT,            -- 1–10 scale
    physical_activity_lvl   INT,            -- Minutes/day
    stress_level            INT,            -- 1–10 scale
    bmi_category            NVARCHAR(30),   -- Normalized: 'Normal','Overweight','Obese'
    systolic_bp             INT,            -- Parsed from 'xxx/yyy' string
    diastolic_bp            INT,
    heart_rate              INT,
    daily_steps             INT,
    sleep_disorder          NVARCHAR(50),   -- 'Healthy','Sleep Apnea','Insomnia','Unknown'
    data_source             NVARCHAR(50),   -- 'sleep_health' or 'sleep_sampled'
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 2. FACT: Sleep Apnea Clinical / PSG Signals
--    Source: bronze.enhanced_sleep_sdb
--    Grain : One row per Patient ID + sleep study session
-- ============================================================
IF OBJECT_ID('silver.sleep_apnea_clinical', 'U') IS NOT NULL
    DROP TABLE silver.sleep_apnea_clinical;
GO

CREATE TABLE silver.sleep_apnea_clinical (
    patient_id                  INT,
    age                         INT,
    gender                      NVARCHAR(10),
    bmi                         DECIMAL(5,2),
    snoring                     NVARCHAR(5),    -- 'Yes' / 'No'
    oxygen_saturation_pct       DECIMAL(5,2),   -- SpO2 from sensor
    ahi_score                   INT,            -- Apnea-Hypopnea Index (key severity marker)
    nasal_airflow               DECIMAL(6,4),
    chest_movement_effort       DECIMAL(6,4),
    ecg_heart_rate              INT,
    spo2_pct                    DECIMAL(5,2),
    sleep_position              NVARCHAR(20),   -- 'Supine','Lateral','Prone'
    eeg_sleep_stage             NVARCHAR(20),   -- 'REM','NREM','Deep'
    diagnosis_sdb               NVARCHAR(5),    -- 'Yes' / 'No'
    severity                    NVARCHAR(20),   -- 'Mild','Moderate','Severe','None','Unknown'
    treatment                   NVARCHAR(50),   -- 'CPAP','Surgery','None','Unknown'
    physician_notes             NVARCHAR(500),
    patient_symptoms            NVARCHAR(500),
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 3. FACT: Sleep Apnea Risk Factors (Binary/Numeric)
--    Source: bronze.sleep_apnea_dataset
--    Grain : One row per anonymised patient observation
-- ============================================================
IF OBJECT_ID('silver.sleep_apnea_risk', 'U') IS NOT NULL
    DROP TABLE silver.sleep_apnea_risk;
GO

CREATE TABLE silver.sleep_apnea_risk (
    record_id               INT IDENTITY(1,1),  -- Surrogate — source has no PK
    age                     DECIMAL(5,1),
    bmi                     DECIMAL(5,2),
    diabetes_flag           TINYINT,            -- 0/1
    hypertension_flag       TINYINT,
    gender_male_flag        TINYINT,            -- 1=Male, 0=Female
    neck_circumference_cm   DECIMAL(5,2),
    smoking_flag            TINYINT,
    alcohol_use_flag        TINYINT,
    snoring_flag            TINYINT,
    fatigue_flag            TINYINT,
    sleep_apnea_flag        TINYINT,            -- Target / outcome
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 4. FACT: Diabetes Patient Detail (Rich clinical features)
--    Source: bronze.diabetes_data  (1,879 records, 46 columns)
--    Grain : One row per PatientID
-- ============================================================
IF OBJECT_ID('silver.diabetes_patient', 'U') IS NOT NULL
    DROP TABLE silver.diabetes_patient;
GO

CREATE TABLE silver.diabetes_patient (
    patient_id                      INT,
    age                             INT,
    gender                          NVARCHAR(10),   -- Decoded: 0=Female,1=Male
    ethnicity                       INT,
    socioeconomic_status            INT,
    education_level                 INT,
    bmi                             DECIMAL(6,4),
    smoking_flag                    TINYINT,
    alcohol_consumption             DECIMAL(6,4),
    physical_activity               DECIMAL(6,4),
    diet_quality                    DECIMAL(6,4),
    sleep_quality                   DECIMAL(6,4),
    family_history_diabetes         TINYINT,
    gestational_diabetes            TINYINT,
    polycystic_ovary_syndrome       TINYINT,
    previous_pre_diabetes           TINYINT,
    hypertension_flag               TINYINT,
    systolic_bp                     INT,
    diastolic_bp                    INT,
    fasting_blood_sugar             DECIMAL(8,4),
    hba1c                           DECIMAL(6,4),
    serum_creatinine                DECIMAL(6,4),
    bun_levels                      DECIMAL(6,4),
    cholesterol_total               DECIMAL(8,4),
    cholesterol_ldl                 DECIMAL(8,4),
    cholesterol_hdl                 DECIMAL(8,4),
    cholesterol_triglycerides       DECIMAL(8,4),
    antihypertensive_meds           TINYINT,
    statins                         TINYINT,
    antidiabetic_meds               TINYINT,
    frequent_urination              TINYINT,
    excessive_thirst                TINYINT,
    unexplained_weight_loss         TINYINT,
    fatigue_level                   DECIMAL(6,4),
    blurred_vision                  TINYINT,
    slow_healing_sores              TINYINT,
    tingling_hands_feet             TINYINT,
    quality_of_life_score           DECIMAL(8,4),
    heavy_metals_exposure           TINYINT,
    occupational_chemical_exposure  TINYINT,
    water_quality                   TINYINT,
    medical_checkups_freq           DECIMAL(6,4),
    medication_adherence            DECIMAL(6,4),
    health_literacy                 DECIMAL(6,4),
    diagnosis_diabetes              TINYINT,        -- 0=No, 1=Yes
    dwh_create_date                 DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 5. FACT: Diabetes Risk Factors — Numeric/Binary Snapshot
--    Source: bronze.diabetes_dataset_10k
--    Grain : One row per anonymised observation
-- ============================================================
IF OBJECT_ID('silver.diabetes_risk_factors', 'U') IS NOT NULL
    DROP TABLE silver.diabetes_risk_factors;
GO

CREATE TABLE silver.diabetes_risk_factors (
    record_id               INT IDENTITY(1,1),
    age                     DECIMAL(5,1),
    bmi                     DECIMAL(5,2),
    glucose                 DECIMAL(7,2),
    insulin                 DECIMAL(7,2),
    blood_pressure          DECIMAL(7,2),
    family_history_flag     TINYINT,
    hypertension_flag       TINYINT,
    sleep_apnea_flag        TINYINT,            -- KEY comorbidity link
    smoking_flag            TINYINT,
    gender_male_flag        TINYINT,
    diabetes_flag           TINYINT,
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 6. FACT: Diabetes Prediction Dataset (Large ML-grade set)
--    Source: bronze.diabetes_prediction  (100,000 rows)
--    Grain : One row per deduplicated record
-- ============================================================
IF OBJECT_ID('silver.diabetes_prediction', 'U') IS NOT NULL
    DROP TABLE silver.diabetes_prediction;
GO

CREATE TABLE silver.diabetes_prediction (
    record_id               INT IDENTITY(1,1),
    gender                  NVARCHAR(10),       -- Normalized; 'Other' → 'Unknown'
    age                     INT,
    hypertension_flag       TINYINT,
    heart_disease_flag      TINYINT,
    smoking_history         NVARCHAR(30),       -- Normalized: 'No Info' → 'Unknown'
    bmi                     DECIMAL(5,2),
    hba1c_level             DECIMAL(5,2),
    blood_glucose_level     INT,
    diabetes_flag           TINYINT,
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 7. FACT: Hypertension Risk (Cardiovascular)
--    Source: bronze.hypertension_risk  (4,240 rows)
--    Grain : One row per patient observation
-- ============================================================
IF OBJECT_ID('silver.hypertension_risk', 'U') IS NOT NULL
    DROP TABLE silver.hypertension_risk;
GO

CREATE TABLE silver.hypertension_risk (
    record_id           INT IDENTITY(1,1),
    gender              NVARCHAR(10),       -- Decoded: 1=Male, 0=Female
    age                 INT,
    current_smoker      TINYINT,
    cigs_per_day        DECIMAL(5,1),       -- NULL-safe: missing → NULL
    bp_meds_flag        TINYINT,            -- NULL-safe: missing → NULL
    diabetes_flag       TINYINT,
    total_cholesterol   INT,                -- NULL-safe
    systolic_bp         DECIMAL(6,1),
    diastolic_bp        INT,
    bmi                 DECIMAL(5,2),       -- NULL-safe
    heart_rate          INT,                -- NULL-safe
    glucose             INT,                -- NULL-safe
    hypertension_risk   TINYINT,            -- Target: 0=Low, 1=High
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 8. AGGREGATE: Epidemiology — Diabetes 2016
--    Source: bronze.diabetes_all_2016
--    Grain : One row per Census Tract (CT)
--    Note  : CT codes are geographic identifiers (US FIPS)
-- ============================================================
IF OBJECT_ID('silver.epidemiology_diabetes_2016', 'U') IS NOT NULL
    DROP TABLE silver.epidemiology_diabetes_2016;
GO

CREATE TABLE silver.epidemiology_diabetes_2016 (
    census_tract        BIGINT,         -- CT = geographic identifier
    bp_adult_diab       INT,            -- BPAD: BP Adult Diabetic count
    bp_adult_nondiab    INT,            -- BPAN: BP Adult Non-Diabetic count
    bp_adult_nondiab2   INT,            -- BPAN2: alternate non-diab measure
    bw_adult_diab       INT,            -- BWAD: Body Weight Adult Diabetic
    bw_adult_nondiab    INT,
    bw_adult_nondiab2   INT,
    bm_adult_diab       INT,            -- BMAD: Biomarker Adult Diabetic
    bm_adult_nondiab    INT,
    bm_adult_nondiab2   INT,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 9. AGGREGATE: Epidemiology — Diabetes + Hypertension 2016
--    Source: bronze.diabetes_hypertension_2016
--    Grain : One row per Census Tract (same CT key — joinable to table 8)
-- ============================================================
IF OBJECT_ID('silver.epidemiology_diab_hyp_2016', 'U') IS NOT NULL
    DROP TABLE silver.epidemiology_diab_hyp_2016;
GO

CREATE TABLE silver.epidemiology_diab_hyp_2016 (
    census_tract            BIGINT,
    total_bp_adult_diab     INT,        -- BTPAD: Total BP Adult Diabetic
    total_bp_adult_nondiab  INT,        -- BTPAN
    total_bw_adult_diab     INT,        -- BTWAD
    total_bw_adult_nondiab  INT,        -- BTWAN
    total_bm_adult_diab     INT,        -- BTMAD
    total_bm_adult_nondiab  INT,        -- BTMAN
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO
