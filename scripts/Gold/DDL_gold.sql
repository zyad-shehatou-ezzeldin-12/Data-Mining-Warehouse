/*
===============================================================================
DDL Script: Create Gold Layer Tables — Sleep Apnea Data Warehouse
===============================================================================
Script Purpose:
    Creates all Gold layer tables (dimensions, central fact, analytical marts).
    The Gold layer is the final, analytics-ready layer

Layer Architecture:
    ┌──────────────────────────────────────────────────────────┐
    │                     GOLD LAYER                           │
    │  DIMENSIONS      FACT TABLE        ANALYTICAL MARTS      │
    │  dim_age_band  ─►                 mart_comorbidity_by_age│
    │  dim_bmi_cat   ─►  fact_patient ─►mart_bmi_sleep_disorder│
    │  dim_disorder  ─►  _health_snap   mart_occupation_risk   │
    │  dim_gender    ─►  shot           mart_clinical_severity │
    │  dim_patient   ─►                 mart_epidemiology      │
    │                                   mart_diabetes_risk     │
    └──────────────────────────────────────────────────────────┘

===============================================================================
*/

-- ============================================================
-- DIMENSION 1: dim_age_band
-- Static lookup — age buckets for grouping analytics
-- ============================================================
IF OBJECT_ID('gold.dim_age_band', 'U') IS NOT NULL DROP TABLE gold.dim_age_band;
GO
CREATE TABLE gold.dim_age_band (
    age_band_id     INT PRIMARY KEY,
    label           NVARCHAR(20),       -- 'Under 18', '18-30', '31-45', '46-60', '61+'
    age_min         INT,
    age_max         INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- DIMENSION 2: dim_bmi_category
-- Normalized BMI classification (WHO standard)
-- ============================================================
IF OBJECT_ID('gold.dim_bmi_category', 'U') IS NOT NULL DROP TABLE gold.dim_bmi_category;
GO
CREATE TABLE gold.dim_bmi_category (
    bmi_cat_id      INT PRIMARY KEY,
    label           NVARCHAR(20),       -- 'Normal', 'Overweight', 'Obese', 'Unknown'
    bmi_min         DECIMAL(5,2),
    bmi_max         DECIMAL(5,2),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- DIMENSION 3: dim_disorder_type
-- Sleep disorder classification lookup
-- ============================================================
IF OBJECT_ID('gold.dim_disorder_type', 'U') IS NOT NULL DROP TABLE gold.dim_disorder_type;
GO
CREATE TABLE gold.dim_disorder_type (
    disorder_id       INT PRIMARY KEY,
    label             NVARCHAR(30),     -- 'Healthy', 'Sleep Apnea', 'Insomnia', 'Unknown'
    is_sleep_disorder TINYINT,          -- 0=No, 1=Yes
    dwh_create_date   DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- DIMENSION 4: dim_gender
-- ============================================================
IF OBJECT_ID('gold.dim_gender', 'U') IS NOT NULL DROP TABLE gold.dim_gender;
GO
CREATE TABLE gold.dim_gender (
    gender_id       INT PRIMARY KEY,
    label           NVARCHAR(10),       -- 'Male', 'Female', 'Unknown'
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- DIMENSION 5: dim_patient
-- One row per unique patient; links to fact via person_id
-- ============================================================
IF OBJECT_ID('gold.dim_patient', 'U') IS NOT NULL DROP TABLE gold.dim_patient;
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
-- FKs → all five dimensions.
-- This is the primary query target for all cross-domain analysis.
-- ============================================================
IF OBJECT_ID('gold.fact_patient_health_snapshot', 'U') IS NOT NULL
    DROP TABLE gold.fact_patient_health_snapshot;
GO
CREATE TABLE gold.fact_patient_health_snapshot (
    -- Keys
    record_id           INT PRIMARY KEY,
    age_band_id         INT REFERENCES gold.dim_age_band(age_band_id),
    bmi_cat_id          INT REFERENCES gold.dim_bmi_category(bmi_cat_id),
    -- Raw measures (denormalized for query performance)
    age                 DECIMAL(5,1),
    bmi                 DECIMAL(5,2),
    glucose             DECIMAL(7,2),
    insulin             DECIMAL(7,2),
    blood_pressure      DECIMAL(7,2),
    -- Comorbidity flags (the analytical core)
    family_history_flag TINYINT,
    hypertension_flag   TINYINT,        -- ★ comorbidity
    sleep_apnea_flag    TINYINT,        -- ★ comorbidity
    smoking_flag        TINYINT,
    gender_male_flag    TINYINT,
    diabetes_flag       TINYINT,        -- ★ comorbidity / outcome
    -- Derived grouping columns (for fast GROUP BY without JOIN)
    age_band            NVARCHAR(20),
    bmi_category        NVARCHAR(20),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 1: mart_comorbidity_by_age
-- Pre-aggregated rates: diabetes + sleep apnea + hypertension
-- by age band. Key analytical output for clinical reports.
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
    all_three_count         INT,            -- All comorbidities together
    avg_bmi                 DECIMAL(6,2),
    avg_glucose             DECIMAL(8,2),
    avg_blood_pressure      DECIMAL(7,2),
    diabetes_rate_pct       DECIMAL(6,1),
    sleep_apnea_rate_pct    DECIMAL(6,1),
    hypertension_rate_pct   DECIMAL(6,1),
    all_three_rate_pct      DECIMAL(6,1),   -- Triple comorbidity rate
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 2: mart_bmi_sleep_disorder
-- BMI category × sleep disorder cross-tab with clinical means.
-- ============================================================
IF OBJECT_ID('gold.mart_bmi_sleep_disorder', 'U') IS NOT NULL
    DROP TABLE gold.mart_bmi_sleep_disorder;
GO
CREATE TABLE gold.mart_bmi_sleep_disorder (
    bmi_category        NVARCHAR(20),
    sleep_disorder      NVARCHAR(30),
    patient_count       INT,
    avg_sleep_hrs       DECIMAL(5,2),
    avg_stress          DECIMAL(5,2),
    avg_systolic        DECIMAL(6,2),
    avg_heart_rate      DECIMAL(6,2),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 3: mart_occupation_risk
-- Sleep disorder rates and lifestyle metrics by occupation.
-- Reveals high-risk professional profiles.
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
    sleep_apnea_rate_pct    DECIMAL(6,1),
    insomnia_rate_pct       DECIMAL(6,1),
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 4: mart_clinical_severity
-- AHI-level PSG detail joined with treatment decisions.
-- Small table (10 rows) but critical for clinical reporting.
-- ============================================================
IF OBJECT_ID('gold.mart_clinical_severity', 'U') IS NOT NULL
    DROP TABLE gold.mart_clinical_severity;
GO
CREATE TABLE gold.mart_clinical_severity (
    patient_id      INT,
    age             INT,
    gender          NVARCHAR(10),
    bmi             DECIMAL(5,2),
    bmi_category    NVARCHAR(20),
    ahi_score       INT,            -- Apnea-Hypopnea Index (0=none, 5-14=mild, 15-29=mod, 30+=severe)
    spo2_pct        DECIMAL(5,2),   -- Blood oxygen saturation
    severity        NVARCHAR(20),
    treatment       NVARCHAR(50),
    diagnosed_sdb   NVARCHAR(5),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 5: mart_epidemiology_combined
-- Census-tract level diabetes + hypertension overlap rates.
-- Joinable to geographic mapping for spatial analysis.
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
    total_bp_diab_hyp       INT,    -- Diabetic patients who also have hypertension
    total_bp_nondiab_hyp    INT,
    total_bw_diab_hyp       INT,
    total_bw_nondiab_hyp    INT,
    total_bm_diab_hyp       INT,
    total_bm_nondiab_hyp    INT,
    diab_hyp_overlap_rate   DECIMAL(8,4),   -- Fraction of diabetics who also have hypertension
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================
-- MART 6: mart_diabetes_risk_profile
-- Diabetes risk rates by age × gender × BMI segment.
-- Optimized for ML feature selection and risk scoring.
-- ============================================================
IF OBJECT_ID('gold.mart_diabetes_risk_profile', 'U') IS NOT NULL
    DROP TABLE gold.mart_diabetes_risk_profile;
GO
CREATE TABLE gold.mart_diabetes_risk_profile (
    age_band            NVARCHAR(20),
    gender              NVARCHAR(10),
    bmi_category        NVARCHAR(20),
    total               INT,
    diabetes_count      INT,
    hypertension_count  INT,
    avg_hba1c           DECIMAL(8,6),
    avg_glucose         DECIMAL(10,6),
    diabetes_rate_pct   DECIMAL(6,1),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO
