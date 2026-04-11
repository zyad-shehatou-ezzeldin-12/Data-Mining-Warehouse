/*
===============================================================================
DDL Script: Create Gold Layer Tables - Sleep Apnea Data Warehouse
===============================================================================
Script Purpose:
    Creates all Gold layer tables (dimensions, central fact, analytical marts).
    The Gold layer is the final, analytics-ready layer consumed by:
        - BI tools (Power BI, Tableau, Looker)
        - Direct SQL reporting
        - ML feature stores
        - CSV/Excel exports for clinical researchers

Change Log:
    [Updated] fact_patient_health_snapshot - refined Tier 1 & Tier 2
              sleep apnea predictor columns.
    [Updated] mart_clinical_severity   - expanded with full PSG signal columns
    [Updated] mart_bmi_sleep_disorder  - added neck circumference & snoring averages
    [Updated] mart_comorbidity_by_age  - added avg snoring rate & avg neck circumference
    [Updated] mart_occupation_risk     - added avg sleep quality & avg fatigue rate
    [Updated] mart_diabetes_risk_profile - added avg hba1c & cholesterol metrics

Usage:
    Run this script in SSMS to create/recreate all Gold layer tables.
    Then run DQL_gold.sql to populate them.
===============================================================================
*/

-- ============================================================
-- PRE-STEP: DROP fact_patient_health_snapshot
-- Must be dropped first to remove FK constraints on dimensions
-- ============================================================
IF OBJECT_ID('gold.fact_patient_health_snapshot', 'U') IS NOT NULL
    DROP TABLE gold.fact_patient_health_snapshot;
GO

-- ============================================================
-- DIMENSION 1: dim_age_band
-- Static lookup - age buckets for grouping analytics
-- ============================================================
IF OBJECT_ID('gold.dim_age_band', 'U') IS NOT NULL
    DROP TABLE gold.dim_age_band;
GO

CREATE TABLE gold.dim_age_band (
    age_band_id     INT PRIMARY KEY,
    label           NVARCHAR(20),
    age_min         INT,
    age_max         INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- DIMENSION 2: dim_bmi_category
-- Normalized BMI classification (WHO standard)
-- ============================================================
IF OBJECT_ID('gold.dim_bmi_category', 'U') IS NOT NULL
    DROP TABLE gold.dim_bmi_category;
GO

CREATE TABLE gold.dim_bmi_category (
    bmi_cat_id      INT PRIMARY KEY,
    label           NVARCHAR(20),
    bmi_min         DECIMAL(5,2),
    bmi_max         DECIMAL(5,2),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- DIMENSION 3: dim_disorder_type
-- Sleep disorder classification lookup
-- ============================================================
IF OBJECT_ID('gold.dim_disorder_type', 'U') IS NOT NULL
    DROP TABLE gold.dim_disorder_type;
GO

CREATE TABLE gold.dim_disorder_type (
    disorder_id       INT PRIMARY KEY,
    label             NVARCHAR(30),
    is_sleep_disorder TINYINT,
    dwh_create_date   DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- DIMENSION 4: dim_gender
-- ============================================================
IF OBJECT_ID('gold.dim_gender', 'U') IS NOT NULL
    DROP TABLE gold.dim_gender;
GO

CREATE TABLE gold.dim_gender (
    gender_id       INT PRIMARY KEY,
    label           NVARCHAR(10),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- DIMENSION 5: dim_patient
-- One row per unique patient; links to fact via person_id
-- ============================================================
IF OBJECT_ID('gold.dim_patient', 'U') IS NOT NULL
    DROP TABLE gold.dim_patient;
GO

CREATE TABLE gold.dim_patient (
    person_id       INT PRIMARY KEY,
    gender          NVARCHAR(10),
    age             INT,
    age_band        NVARCHAR(20),
    occupation      NVARCHAR(100),
    bmi_category    NVARCHAR(20),
    sleep_disorder  NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- CENTRAL FACT TABLE: fact_patient_health_snapshot
-- Grain: One row per anonymised patient health observation.
-- Combines sleep + diabetes + hypertension risk signals.
-- FKs -> all five dimensions.
-- ============================================================
IF OBJECT_ID('gold.fact_patient_health_snapshot', 'U') IS NOT NULL
    DROP TABLE gold.fact_patient_health_snapshot;
GO

CREATE TABLE gold.fact_patient_health_snapshot (
    -- Keys
    patient_primarykey      NVARCHAR(50),
    age_band_id             INT REFERENCES gold.dim_age_band(age_band_id),
    bmi_cat_id              INT REFERENCES gold.dim_bmi_category(bmi_cat_id),
    -- Demographics
    age                     DECIMAL(5,1),
    gender                  NVARCHAR(10),
    occupation              NVARCHAR(100),
    -- Core biometrics
    bmi                     DECIMAL(5,2),
    glucose                 DECIMAL(7,2),
    insulin                 DECIMAL(7,2),
    blood_pressure          DECIMAL(7,2),
    systolic_bp             DECIMAL(6,2),
    diastolic_bp            DECIMAL(6,2),
    heart_rate              DECIMAL(6,2),
    daily_steps             INT,
    -- Glycaemic and lipid panel
    hba1c                   DECIMAL(5,2),
    cholesterol_total       DECIMAL(7,2),
    cholesterol_ldl         DECIMAL(7,2),
    cholesterol_hdl         DECIMAL(7,2),
    -- Comorbidity flags
    family_history_flag     TINYINT,
    hypertension_flag       TINYINT,
    sleep_apnea_flag        TINYINT,
    smoking_flag            TINYINT,
    gender_male_flag        TINYINT,
    diabetes_flag           TINYINT,
    alcohol_flag            TINYINT,
    -- Tier 1: Primary sleep apnea predictors
    neck_circumference      DECIMAL(5,2),
    snoring_flag            TINYINT,
    fatigue_flag            TINYINT,
    oxygen_saturation       DECIMAL(5,2),
    ahi_score               DECIMAL(6,2),
    spo2_ratio              DECIMAL(7,4),
    eeg_sleep_stage         NVARCHAR(20),
    nasal_airflow_rate      DECIMAL(7,4),
    chest_movement_effort   DECIMAL(7,4),
    sleep_position          NVARCHAR(20),
    sleep_duration_hrs      DECIMAL(4,2),
    quality_of_sleep        INT,
    -- Tier 2: Supporting lifestyle and symptom features
    stress_level            INT,
    physical_activity_level INT,
    alcohol_consumption     NVARCHAR(20),
    fatigue_level           INT,
    diet_quality            NVARCHAR(20),
    quality_of_life_score   DECIMAL(5,2),
    -- Derived grouping columns
    age_band                NVARCHAR(20),
    bmi_category            NVARCHAR(20),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 1: mart_comorbidity_by_age
-- ============================================================
IF OBJECT_ID('gold.mart_comorbidity_by_age', 'U') IS NOT NULL
    DROP TABLE gold.mart_comorbidity_by_age;
GO

CREATE TABLE gold.mart_comorbidity_by_age (
    age_band                NVARCHAR(20),
    total_patients          INT,
    diabetes_count          INT,
    sleep_apnea_count       INT,
    hypertension_count      INT,
    all_three_count         INT,
    avg_bmi                 DECIMAL(6,2),
    avg_glucose             DECIMAL(8,2),
    avg_blood_pressure      DECIMAL(7,2),
    avg_neck_circumference  DECIMAL(6,2),
    snoring_count           INT,
    fatigue_count           INT,
    avg_ahi_score           DECIMAL(6,2),
    avg_spo2                DECIMAL(5,2),
    diabetes_rate_pct       DECIMAL(6,1),
    sleep_apnea_rate_pct    DECIMAL(6,1),
    hypertension_rate_pct   DECIMAL(6,1),
    all_three_rate_pct      DECIMAL(6,1),
    snoring_rate_pct        DECIMAL(6,1),
    fatigue_rate_pct        DECIMAL(6,1),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 2: mart_bmi_sleep_disorder
-- ============================================================
IF OBJECT_ID('gold.mart_bmi_sleep_disorder', 'U') IS NOT NULL
    DROP TABLE gold.mart_bmi_sleep_disorder;
GO

CREATE TABLE gold.mart_bmi_sleep_disorder (
    bmi_category            NVARCHAR(20),
    sleep_disorder          NVARCHAR(30),
    patient_count           INT,
    avg_sleep_hrs           DECIMAL(5,2),
    avg_stress              DECIMAL(5,2),
    avg_systolic            DECIMAL(6,2),
    avg_heart_rate          DECIMAL(6,2),
    avg_neck_circumference  DECIMAL(6,2),
    snoring_rate_pct        DECIMAL(6,1),
    avg_ahi_score           DECIMAL(6,2),
    avg_oxygen_saturation   DECIMAL(5,2),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 3: mart_occupation_risk
-- ============================================================
IF OBJECT_ID('gold.mart_occupation_risk', 'U') IS NOT NULL
    DROP TABLE gold.mart_occupation_risk;
GO

CREATE TABLE gold.mart_occupation_risk (
    occupation              NVARCHAR(100),
    total_patients          INT,
    sleep_apnea_count       INT,
    insomnia_count          INT,
    avg_sleep_hrs           DECIMAL(5,2),
    avg_stress              DECIMAL(5,2),
    avg_systolic            DECIMAL(6,2),
    avg_daily_steps         DECIMAL(8,2),
    avg_quality_of_sleep    DECIMAL(5,2),
    avg_neck_circumference  DECIMAL(6,2),
    fatigue_rate_pct        DECIMAL(6,1),
    sleep_apnea_rate_pct    DECIMAL(6,1),
    insomnia_rate_pct       DECIMAL(6,1),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 4: mart_clinical_severity
-- ============================================================
IF OBJECT_ID('gold.mart_clinical_severity', 'U') IS NOT NULL
    DROP TABLE gold.mart_clinical_severity;
GO

CREATE TABLE gold.mart_clinical_severity (
    patient_id              INT,
    age                     INT,
    gender                  NVARCHAR(10),
    bmi                     DECIMAL(5,2),
    bmi_category            NVARCHAR(20),
    neck_circumference      DECIMAL(5,2),
    snoring                 NVARCHAR(10),
    ahi_score               INT,
    spo2_pct                DECIMAL(5,2),
    spo2_ratio              DECIMAL(7,4),
    oxygen_saturation       DECIMAL(5,2),
    nasal_airflow_rate      DECIMAL(7,4),
    chest_movement_effort   DECIMAL(7,4),
    eeg_sleep_stage         NVARCHAR(20),
    sleep_position          NVARCHAR(20),
    ecg_heart_rate          DECIMAL(6,2),
    fatigue_flag            TINYINT,
    severity                NVARCHAR(20),
    treatment               NVARCHAR(50),
    diagnosed_sdb           NVARCHAR(5),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 5: mart_epidemiology_combined
-- ============================================================
IF OBJECT_ID('gold.mart_epidemiology_combined', 'U') IS NOT NULL
    DROP TABLE gold.mart_epidemiology_combined;
GO

CREATE TABLE gold.mart_epidemiology_combined (
    census_tract            BIGINT PRIMARY KEY,
    bp_adult_diab           INT,
    bp_adult_nondiab        INT,
    bw_adult_diab           INT,
    bw_adult_nondiab        INT,
    bm_adult_diab           INT,
    bm_adult_nondiab        INT,
    total_bp_diab_hyp       INT,
    total_bp_nondiab_hyp    INT,
    total_bw_diab_hyp       INT,
    total_bw_nondiab_hyp    INT,
    total_bm_diab_hyp       INT,
    total_bm_nondiab_hyp    INT,
    diab_hyp_overlap_rate   DECIMAL(8,4),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 6: mart_diabetes_risk_profile
-- ============================================================
IF OBJECT_ID('gold.mart_diabetes_risk_profile', 'U') IS NOT NULL
    DROP TABLE gold.mart_diabetes_risk_profile;
GO

CREATE TABLE gold.mart_diabetes_risk_profile (
    age_band                NVARCHAR(20),
    gender                  NVARCHAR(10),
    bmi_category            NVARCHAR(20),
    total                   INT,
    diabetes_count          INT,
    hypertension_count      INT,
    sleep_apnea_count       INT,
    avg_hba1c               DECIMAL(8,6),
    avg_glucose             DECIMAL(10,6),
    avg_cholesterol_total   DECIMAL(7,2),
    avg_cholesterol_ldl     DECIMAL(7,2),
    avg_cholesterol_hdl     DECIMAL(7,2),
    avg_neck_circumference  DECIMAL(6,2),
    diabetes_rate_pct       DECIMAL(6,1),
    sleep_apnea_rate_pct    DECIMAL(6,1),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO
