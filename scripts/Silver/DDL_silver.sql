/*
===============================================================================
DDL Script: Create Silver Tables — Sleep Apnea Data Warehouse
===============================================================================
Script Purpose:
    Creates cleansed, normalized Silver tables from Bronze sources.
    Covers: Sleep Apnea, Sleep Lifestyle, Sleep Disordered Breathing,
            Diabetes (detailed + prediction + 10k link), Hypertension Risk,
            Diabetes/Hypertension Aggregates (2016).
===============================================================================
*/

-- ============================================================
-- 1. silver.sleep_apnea_core
--    Source: bronze.sleep_apnea_dataset
--    Clinical flags: Diabetes, Hypertension, Smoking, Alcohol,
--                    Snoring, Fatigue, Sleep Apnea outcome
-- ============================================================
IF OBJECT_ID('silver.sleep_apnea_core', 'U') IS NOT NULL
    DROP TABLE silver.sleep_apnea_core;
GO

CREATE TABLE silver.sleep_apnea_core (
    record_id             INT IDENTITY(1,1),
    age                   DECIMAL(5,1),
    gender                NVARCHAR(10),        -- 'Male' / 'Female' (decoded from Gender_Male)
    bmi                   DECIMAL(5,1),
    neck_circumference    DECIMAL(5,1),
    diabetes_flag         TINYINT,             -- 0/1
    hypertension_flag     TINYINT,             -- 0/1
    smoking_flag          TINYINT,             -- 0/1
    alcohol_flag          TINYINT,             -- 0/1
    snoring_flag          TINYINT,             -- 0/1
    fatigue_flag          TINYINT,             -- 0/1
    sleep_apnea_flag      TINYINT,             -- 0/1  (outcome)
    patient_key           NVARCHAR(16),        -- MD5 hash of age|gender|bmi (surrogate)
    dwh_create_date       DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 2. silver.sleep_lifestyle
--    Source: bronze.Sleep_Data_Sampled  (15,000 rows)
--    Lifestyle: occupation, sleep duration, quality, stress,
--               physical activity, BMI category, BP, heart rate
-- ============================================================
IF OBJECT_ID('silver.sleep_lifestyle', 'U') IS NOT NULL
    DROP TABLE silver.sleep_lifestyle;
GO

CREATE TABLE silver.sleep_lifestyle (
    record_id               INT IDENTITY(1,1),
    gender                  NVARCHAR(10),
    age                     INT,
    occupation              NVARCHAR(100),
    sleep_duration_hrs      DECIMAL(4,2),
    quality_of_sleep        TINYINT,            -- 1-10 scale
    physical_activity_level INT,
    stress_level            TINYINT,            -- 1-10 scale
    bmi_category            NVARCHAR(20),       -- Normalized: 'Normal'/'Overweight'/'Obese'
    systolic_bp             SMALLINT,           -- Parsed from 'xxx/yyy'
    diastolic_bp            SMALLINT,
    heart_rate              INT,
    daily_steps             INT,
    sleep_disorder          NVARCHAR(20),       -- 'None'/'Sleep Apnea'/'Insomnia'
    patient_key             NVARCHAR(16),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 3. silver.sleep_health_lifestyle
--    Source: bronze.Sleep_health_and_lifestyle_dataset  (374 rows)
--    Smaller validated clinical dataset with same structure
-- ============================================================
IF OBJECT_ID('silver.sleep_health_lifestyle', 'U') IS NOT NULL
    DROP TABLE silver.sleep_health_lifestyle;
GO

CREATE TABLE silver.sleep_health_lifestyle (
    record_id               INT IDENTITY(1,1),
    gender                  NVARCHAR(10),
    age                     INT,
    occupation              NVARCHAR(100),
    sleep_duration_hrs      DECIMAL(4,2),
    quality_of_sleep        TINYINT,
    physical_activity_level INT,
    stress_level            TINYINT,
    bmi_category            NVARCHAR(20),
    systolic_bp             SMALLINT,
    diastolic_bp            SMALLINT,
    heart_rate              INT,
    daily_steps             INT,
    sleep_disorder          NVARCHAR(20),       -- NULL → 'None'
    patient_key             NVARCHAR(16),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 4. silver.sleep_disordered_breathing
--    Source: bronze.enhanced_sleep_disordered_breathing_dataset (10 rows)
--    Clinical signals: AHI, SpO2, ECG, EEG stage, diagnosis, severity
--    NOTE: Only 10 rows — kept as reference/lookup enrichment
-- ============================================================
IF OBJECT_ID('silver.sleep_disordered_breathing', 'U') IS NOT NULL
    DROP TABLE silver.sleep_disordered_breathing;
GO

CREATE TABLE silver.sleep_disordered_breathing (
    record_id                   INT IDENTITY(1,1),
    age                         INT,
    gender                      NVARCHAR(10),
    bmi                         DECIMAL(5,1),
    snoring                     NVARCHAR(5),        -- 'Yes'/'No'
    oxygen_saturation           DECIMAL(5,1),       -- SpO2 %
    apnea_hypopnea_index        DECIMAL(6,2),       -- AHI score
    nasal_airflow_rate          DECIMAL(6,2),
    chest_movement_effort       DECIMAL(6,2),
    ecg_heart_rate              INT,
    spo2_ratio                  DECIMAL(6,1),
    sleep_position              NVARCHAR(20),
    eeg_sleep_stage             NVARCHAR(20),
    diagnosis_sdb               NVARCHAR(5),        -- 'Yes'/'No'
    severity                    NVARCHAR(20),       -- 'None'/'Mild'/'Moderate'/'Severe'
    treatment                   NVARCHAR(100),      -- NULL → 'Not Assigned'
    patient_key                 NVARCHAR(16),
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 5. silver.diabetes_detailed
--    Source: bronze.diabetes_data  (1,879 rows)
--    Rich clinical + lifestyle diabetes dataset
-- ============================================================
IF OBJECT_ID('silver.diabetes_detailed', 'U') IS NOT NULL
    DROP TABLE silver.diabetes_detailed;
GO

CREATE TABLE silver.diabetes_detailed (
    record_id                       INT IDENTITY(1,1),
    patient_source_id               INT,
    age                             INT,
    gender                          NVARCHAR(10),       -- Decoded: 0→Female, 1→Male
    bmi                             DECIMAL(6,2),
    smoking_flag                    TINYINT,
    alcohol_consumption             DECIMAL(5,2),
    physical_activity               DECIMAL(5,2),
    diet_quality                    DECIMAL(5,2),
    sleep_quality                   DECIMAL(5,2),
    hypertension_flag               TINYINT,
    systolic_bp                     INT,
    diastolic_bp                    INT,
    fasting_blood_sugar             DECIMAL(7,2),
    hba1c                           DECIMAL(5,2),
    cholesterol_total               DECIMAL(7,2),
    cholesterol_ldl                 DECIMAL(7,2),
    cholesterol_hdl                 DECIMAL(7,2),
    cholesterol_triglycerides       DECIMAL(7,2),
    family_history_diabetes         TINYINT,
    previous_pre_diabetes           TINYINT,
    fatigue_level                   DECIMAL(5,2),
    quality_of_life_score           DECIMAL(5,2),
    diabetes_diagnosis              TINYINT,
    patient_key                     NVARCHAR(16),
    dwh_create_date                 DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 6. silver.diabetes_sleep_link
--    Source: bronze.diabetes_dataset_10k  (10,000 rows)
--    KEY BRIDGE TABLE: links Sleep Apnea ↔ Diabetes ↔ Hypertension
-- ============================================================
IF OBJECT_ID('silver.diabetes_sleep_link', 'U') IS NOT NULL
    DROP TABLE silver.diabetes_sleep_link;
GO

CREATE TABLE silver.diabetes_sleep_link (
    record_id           INT IDENTITY(1,1),
    patient_source_id   INT,
    age                 DECIMAL(5,1),
    gender              NVARCHAR(10),       -- Decoded from Gender_Male
    bmi                 DECIMAL(5,1),
    glucose             DECIMAL(7,2),
    insulin             DECIMAL(7,2),
    blood_pressure      INT,
    family_history_flag TINYINT,
    hypertension_flag   TINYINT,
    sleep_apnea_flag    TINYINT,
    smoking_flag        TINYINT,
    diabetes_flag       TINYINT,
    patient_key         NVARCHAR(16),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 7. silver.diabetes_prediction
--    Source: bronze.diabetes_prediction_dataset11  (100,000 rows)
--    Large population-level diabetes risk dataset
-- ============================================================
IF OBJECT_ID('silver.diabetes_prediction', 'U') IS NOT NULL
    DROP TABLE silver.diabetes_prediction;
GO

CREATE TABLE silver.diabetes_prediction (
    record_id               INT IDENTITY(1,1),
    gender                  NVARCHAR(10),
    age                     DECIMAL(5,1),
    bmi                     DECIMAL(5,1),
    hypertension_flag       TINYINT,
    heart_disease_flag      TINYINT,
    smoking_history         NVARCHAR(30),   -- Normalized smoking categories
    hba1c_level             DECIMAL(5,2),
    blood_glucose_level     INT,
    diabetes_flag           TINYINT,
    patient_key             NVARCHAR(16),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 8. silver.hypertension_risk
--    Source: bronze.Hypertension_risk_model_main  (4,240 rows)
--    Cardiovascular / BP risk model data
-- ============================================================
IF OBJECT_ID('silver.hypertension_risk', 'U') IS NOT NULL
    DROP TABLE silver.hypertension_risk;
GO

CREATE TABLE silver.hypertension_risk (
    record_id           INT IDENTITY(1,1),
    gender              NVARCHAR(10),       -- Decoded from male=1/0
    age                 INT,
    bmi                 DECIMAL(5,2),
    current_smoker      TINYINT,
    cigs_per_day        DECIMAL(5,1),       -- NULL preserved → 0 for non-smokers
    bp_medication       TINYINT,            -- NULL preserved → 0
    diabetes_flag       TINYINT,
    total_cholesterol   DECIMAL(7,1),
    systolic_bp         DECIMAL(6,1),
    diastolic_bp        DECIMAL(6,1),
    heart_rate          INT,
    glucose             DECIMAL(6,1),
    hypertension_risk   TINYINT,            -- 0/1 outcome
    patient_key         NVARCHAR(16),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 9. silver.diabetes_aggregate_2016
--    Source: bronze.diabetes_all_2016  (390 rows)
--    Aggregate stats: no individual Age/Gender/BMI — kept as-is
--    CT = region/area code
-- ============================================================
IF OBJECT_ID('silver.diabetes_aggregate_2016', 'U') IS NOT NULL
    DROP TABLE silver.diabetes_aggregate_2016;
GO

CREATE TABLE silver.diabetes_aggregate_2016 (
    record_id               INT IDENTITY(1,1),
    area_code               NVARCHAR(20),       -- CT column
    bp_affected_diabetic    INT,                -- BPAD
    bp_affected_non_diab    INT,                -- BPAN
    bp_affected_non_diab2   INT,                -- BPAN2
    bw_affected_diabetic    INT,                -- BWAD
    bw_affected_non_diab    INT,                -- BWAN
    bw_affected_non_diab2   INT,                -- BWAN2
    bm_affected_diabetic    INT,                -- BMAD
    bm_affected_non_diab    INT,                -- BMAN
    bm_affected_non_diab2   INT,                -- BMAN2
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- 10. silver.diabetes_hypertension_aggregate_2016
--     Source: bronze.diabetes_hypertension_all_2016  (390 rows)
--     Combined aggregate: diabetes + hypertension by area code
-- ============================================================
IF OBJECT_ID('silver.diabetes_hypertension_aggregate_2016', 'U') IS NOT NULL
    DROP TABLE silver.diabetes_hypertension_aggregate_2016;
GO

CREATE TABLE silver.diabetes_hypertension_aggregate_2016 (
    record_id                   INT IDENTITY(1,1),
    area_code                   NVARCHAR(20),
    both_affected_diabetic      INT,            -- BTPAD
    both_affected_non_diab      INT,            -- BTPAN
    both_bw_affected_diabetic   INT,            -- BTWAD
    both_bw_affected_non_diab   INT,            -- BTWAN
    both_bm_affected_diabetic   INT,            -- BTMAD
    both_bm_affected_non_diab   INT,            -- BTMAN
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO
