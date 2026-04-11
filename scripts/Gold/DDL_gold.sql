/*
 ===============================================================================
 Stored Procedure: Load Gold Layer — Sleep Apnea Data Warehouse
 ===============================================================================
 Script Purpose:
 ETL from Silver → Gold for all dimensions, the central fact table,
 and all 6 analytical marts.
 
 FIXED: Handles foreign key constraints by using DELETE instead of TRUNCATE
 for referenced tables, or drops/recreates constraints temporarily.
 
 Change Log:
 [Updated] STEP 6  — fact_patient_health_snapshot: added LEFT JOINs to
 silver.sleep_apnea_core, silver.sleep_disordered_breathing,
 silver.sleep_health_lifestyle, and silver.diabetes_detailed
 to populate all Tier 1 & Tier 2 sleep apnea predictor columns.
 
 [Updated] STEP 7  — mart_comorbidity_by_age: added avg_neck_circumference,
 snoring_count, fatigue_count, avg_ahi_score, avg_spo2,
 snoring_rate_pct, fatigue_rate_pct.
 
 [Updated] STEP 8  — mart_bmi_sleep_disorder: added avg_neck_circumference,
 snoring_rate_pct, avg_ahi_score, avg_oxygen_saturation.
 Source extended with LEFT JOIN to silver.sleep_apnea_core
 and silver.sleep_disordered_breathing.
 
 [Updated] STEP 9  — mart_occupation_risk: added avg_quality_of_sleep,
 avg_neck_circumference, fatigue_rate_pct.
 Source extended with LEFT JOIN to silver.sleep_apnea_core.
 
 [Updated] STEP 10 — mart_clinical_severity: added neck_circumference,
 snoring, spo2_ratio, oxygen_saturation, nasal_airflow_rate,
 chest_movement_effort, eeg_sleep_stage, sleep_position,
 ecg_heart_rate, fatigue_flag. All sourced from
 silver.sleep_disordered_breathing + silver.sleep_apnea_core.
 
 [Updated] STEP 12 — mart_diabetes_risk_profile: added sleep_apnea_count,
 avg_cholesterol_total, avg_cholesterol_ldl, avg_cholesterol_hdl,
 avg_neck_circumference, sleep_apnea_rate_pct. Source extended
 with LEFT JOIN to silver.diabetes_detailed and
 silver.sleep_apnea_core.
 
 Usage : EXEC gold.load_gold;
 ===============================================================================
 */
CREATE
OR ALTER PROCEDURE gold.load_gold AS BEGIN
DECLARE @start_time DATETIME,
    @end_time DATETIME,
    @batch_start DATETIME,
    @batch_end DATETIME;
BEGIN TRY
SET @batch_start = GETDATE();
PRINT '================================================';
PRINT 'Loading Gold Layer — Sleep Apnea DW';
PRINT '================================================';
-- PRE-STEP: Clear fact table first (has FK references to dimensions)
PRINT '>> PRE-STEP: Clearing fact table (FK dependencies)';
TRUNCATE TABLE gold.fact_patient_health_snapshot;
-- ────────────────────────────────────────────────────
-- STEP 1: dim_age_band (static seed)
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 1: Loading gold.dim_age_band';
DELETE FROM gold.dim_age_band;
INSERT INTO gold.dim_age_band (age_band_id, label, age_min, age_max)
VALUES (1, 'Under 18', 0, 17),
    (2, '18-30', 18, 30),
    (3, '31-45', 31, 45),
    (4, '46-60', 46, 60),
    (5, '61+', 61, 99);
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's | Rows: 5';
PRINT '>> -------------';
-- ────────────────────────────────────────────────────
-- STEP 2: dim_bmi_category (WHO standard)
-- FIX: Use DELETE instead of TRUNCATE (referenced by FK)
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 2: Loading gold.dim_bmi_category';
DELETE FROM gold.dim_bmi_category;
INSERT INTO gold.dim_bmi_category (bmi_cat_id, label, bmi_min, bmi_max)
VALUES (1, 'Normal', 0, 24.9),
    (2, 'Overweight', 25, 29.9),
    (3, 'Obese', 30, 99.0),
    (4, 'Unknown', NULL, NULL);
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's | Rows: 4';
PRINT '>> -------------';
-- ────────────────────────────────────────────────────
-- STEP 3: dim_disorder_type
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 3: Loading gold.dim_disorder_type';
TRUNCATE TABLE gold.dim_disorder_type;
INSERT INTO gold.dim_disorder_type (disorder_id, label, is_sleep_disorder)
VALUES (1, 'Healthy', 0),
    (2, 'Sleep Apnea', 1),
    (3, 'Insomnia', 1),
    (4, 'Unknown', 0);
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's | Rows: 4';
PRINT '>> -------------';
-- ────────────────────────────────────────────────────
-- STEP 4: dim_gender
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 4: Loading gold.dim_gender';
TRUNCATE TABLE gold.dim_gender;
INSERT INTO gold.dim_gender (gender_id, label)
VALUES (1, 'Male'),
    (2, 'Female'),
    (3, 'Unknown');
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's | Rows: 3';
PRINT '>> -------------';
-- ────────────────────────────────────────────────────
-- STEP 5: dim_patient
-- Source: silver.sleep_health_lifestyle
-- Derive age_band inline using CASE
-- Use record_id as person_id (no person_id in silver)
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 5: Loading gold.dim_patient';
TRUNCATE TABLE gold.dim_patient;
INSERT INTO gold.dim_patient (
        person_id,
        gender,
        age,
        age_band,
        occupation,
        bmi_category,
        sleep_disorder
    )
SELECT record_id AS person_id,
    gender,
    age,
    CASE
        WHEN age < 18 THEN 'Under 18'
        WHEN age <= 30 THEN '18-30'
        WHEN age <= 45 THEN '31-45'
        WHEN age <= 60 THEN '46-60'
        ELSE '61+'
    END AS age_band,
    occupation,
    CASE
        WHEN TRIM(bmi_category) IN ('Normal', 'Normal Weight') THEN 'Normal'
        WHEN TRIM(bmi_category) = 'Overweight' THEN 'Overweight'
        WHEN TRIM(bmi_category) = 'Obese' THEN 'Obese'
        ELSE 'Unknown'
    END AS bmi_category,
    ISNULL(NULLIF(TRIM(sleep_disorder), ''), 'Unknown') AS sleep_disorder
FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY record_id
                ORDER BY dwh_create_date DESC
            ) AS rn
        FROM silver.sleep_health_lifestyle
    ) t
WHERE rn = 1;
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's';
PRINT '>> -------------';
-- ────────────────────────────────────────────────────
-- STEP 6: fact_patient_health_snapshot
-- Primary source : silver.diabetes_sleep_link
-- [NEW] LEFT JOINs added to pull all Tier 1 & Tier 2
--       sleep apnea predictor columns:
--         sac  → silver.sleep_apnea_core
--         sdb  → silver.sleep_disordered_breathing
--         shl  → silver.sleep_health_lifestyle
--         dd   → silver.diabetes_detailed
-- Join key: patient_key (present on all four silver tables)
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 6: Loading gold.fact_patient_health_snapshot';

-- ── Pre-compute imputation defaults (mean / mode) from each source ──
DECLARE
    -- silver.sleep_health_lifestyle means
    @avg_systolic_bp     DECIMAL(6,2),
    @avg_diastolic_bp    DECIMAL(6,2),
    @avg_heart_rate_shl  DECIMAL(6,2),
    @avg_daily_steps     INT,
    @avg_sleep_dur       DECIMAL(4,2),
    @avg_quality_sleep   INT,
    @avg_stress          INT,
    @avg_phys_activity   INT,
    @mode_occupation     NVARCHAR(100),
    -- silver.diabetes_detailed means
    @avg_hba1c           DECIMAL(5,2),
    @avg_chol_total      DECIMAL(7,2),
    @avg_chol_ldl        DECIMAL(7,2),
    @avg_chol_hdl        DECIMAL(7,2),
    @avg_alcohol_cons    DECIMAL(5,2),
    @avg_fatigue_level   INT,
    @avg_diet_quality    DECIMAL(5,2),
    @avg_qol_score       DECIMAL(5,2),
    -- silver.sleep_apnea_core means
    @avg_neck_circ       DECIMAL(5,2),
    @mode_alcohol_flag   TINYINT,
    @mode_snoring_flag   TINYINT,
    @mode_fatigue_flag   TINYINT,
    -- silver.sleep_disordered_breathing means
    @avg_oxy_sat         DECIMAL(5,2),
    @avg_ahi             DECIMAL(6,2),
    @avg_spo2_ratio      DECIMAL(7,4),
    @avg_nasal_flow      DECIMAL(7,4),
    @avg_chest_effort    DECIMAL(7,4),
    @mode_eeg_stage      NVARCHAR(20),
    @mode_sleep_pos      NVARCHAR(20);

-- Compute from silver.sleep_health_lifestyle
SELECT
    @avg_systolic_bp   = ROUND(AVG(CAST(systolic_bp AS FLOAT)), 2),
    @avg_diastolic_bp  = ROUND(AVG(CAST(diastolic_bp AS FLOAT)), 2),
    @avg_heart_rate_shl= ROUND(AVG(CAST(heart_rate AS FLOAT)), 2),
    @avg_daily_steps   = ROUND(AVG(CAST(daily_steps AS FLOAT)), 0),
    @avg_sleep_dur     = ROUND(AVG(CAST(sleep_duration_hrs AS FLOAT)), 2),
    @avg_quality_sleep = ROUND(AVG(CAST(quality_of_sleep AS FLOAT)), 0),
    @avg_stress        = ROUND(AVG(CAST(stress_level AS FLOAT)), 0),
    @avg_phys_activity = ROUND(AVG(CAST(physical_activity_level AS FLOAT)), 0)
FROM silver.sleep_health_lifestyle;

SELECT TOP 1 @mode_occupation = occupation
FROM silver.sleep_health_lifestyle
GROUP BY occupation ORDER BY COUNT(*) DESC;

-- Compute from silver.diabetes_detailed
SELECT
    @avg_hba1c        = ROUND(AVG(CAST(hba1c AS FLOAT)), 2),
    @avg_chol_total   = ROUND(AVG(CAST(cholesterol_total AS FLOAT)), 2),
    @avg_chol_ldl     = ROUND(AVG(CAST(cholesterol_ldl AS FLOAT)), 2),
    @avg_chol_hdl     = ROUND(AVG(CAST(cholesterol_hdl AS FLOAT)), 2),
    @avg_alcohol_cons = ROUND(AVG(CAST(alcohol_consumption AS FLOAT)), 2),
    @avg_fatigue_level= ROUND(AVG(CAST(fatigue_level AS FLOAT)), 0),
    @avg_diet_quality = ROUND(AVG(CAST(diet_quality AS FLOAT)), 2),
    @avg_qol_score    = ROUND(AVG(CAST(quality_of_life_score AS FLOAT)), 2)
FROM silver.diabetes_detailed;

-- Compute from silver.sleep_apnea_core
SELECT
    @avg_neck_circ    = ROUND(AVG(CAST(neck_circumference AS FLOAT)), 2),
    @mode_snoring_flag= ROUND(AVG(CAST(snoring_flag AS FLOAT)), 0),
    @mode_fatigue_flag= ROUND(AVG(CAST(fatigue_flag AS FLOAT)), 0),
    @mode_alcohol_flag= ROUND(AVG(CAST(alcohol_flag AS FLOAT)), 0)
FROM silver.sleep_apnea_core;

-- Compute from silver.sleep_disordered_breathing
SELECT
    @avg_oxy_sat      = ROUND(AVG(CAST(oxygen_saturation AS FLOAT)), 2),
    @avg_ahi          = ROUND(AVG(CAST(apnea_hypopnea_index AS FLOAT)), 2),
    @avg_spo2_ratio   = ROUND(AVG(CAST(spo2_ratio AS FLOAT)), 4),
    @avg_nasal_flow   = ROUND(AVG(CAST(nasal_airflow_rate AS FLOAT)), 4),
    @avg_chest_effort = ROUND(AVG(CAST(chest_movement_effort AS FLOAT)), 4)
FROM silver.sleep_disordered_breathing;

SELECT TOP 1 @mode_eeg_stage = eeg_sleep_stage
FROM silver.sleep_disordered_breathing
GROUP BY eeg_sleep_stage ORDER BY COUNT(*) DESC;

SELECT TOP 1 @mode_sleep_pos = sleep_position
FROM silver.sleep_disordered_breathing
GROUP BY sleep_position ORDER BY COUNT(*) DESC;

-- ── Insert with imputed defaults ──
TRUNCATE TABLE gold.fact_patient_health_snapshot;
INSERT INTO gold.fact_patient_health_snapshot (
        -- Keys
        record_id,
        patient_key,
        age_band_id,
        bmi_cat_id,
        -- Demographics
        age,
        gender,
        occupation,
        -- Core biometrics
        bmi,
        glucose,
        insulin,
        blood_pressure,
        systolic_bp,
        diastolic_bp,
        heart_rate,
        daily_steps,
        -- Glycaemic & lipid panel
        hba1c,
        cholesterol_total,
        cholesterol_ldl,
        cholesterol_hdl,
        -- Comorbidity flags
        family_history_flag,
        hypertension_flag,
        sleep_apnea_flag,
        smoking_flag,
        gender_male_flag,
        diabetes_flag,
        alcohol_flag,
        -- Tier 1: Primary sleep apnea predictors
        neck_circumference,
        snoring_flag,
        fatigue_flag,
        oxygen_saturation,
        ahi_score,
        spo2_ratio,
        eeg_sleep_stage,
        nasal_airflow_rate,
        chest_movement_effort,
        sleep_position,
        sleep_duration_hrs,
        quality_of_sleep,
        -- Tier 2: Supporting lifestyle & symptom features
        stress_level,
        physical_activity_level,
        alcohol_consumption,
        fatigue_level,
        diet_quality,
        quality_of_life_score,
        -- Grouping helpers
        age_band,
        bmi_category
    )
SELECT -- Keys
    r.record_id,
    r.patient_key,
    ab.age_band_id,
    bc.bmi_cat_id,
    -- Demographics
    r.age,
    r.gender,
    ISNULL(shl.occupation, @mode_occupation) AS occupation,
    -- Core biometrics
    r.bmi,
    r.glucose,
    r.insulin,
    r.blood_pressure,
    ISNULL(shl.systolic_bp, @avg_systolic_bp) AS systolic_bp,
    ISNULL(shl.diastolic_bp, @avg_diastolic_bp) AS diastolic_bp,
    ISNULL(shl.heart_rate, @avg_heart_rate_shl) AS heart_rate,
    ISNULL(shl.daily_steps, @avg_daily_steps) AS daily_steps,
    -- Glycaemic & lipid panel (imputed with dataset mean)
    ISNULL(dd.hba1c, @avg_hba1c) AS hba1c,
    ISNULL(dd.cholesterol_total, @avg_chol_total) AS cholesterol_total,
    ISNULL(dd.cholesterol_ldl, @avg_chol_ldl) AS cholesterol_ldl,
    ISNULL(dd.cholesterol_hdl, @avg_chol_hdl) AS cholesterol_hdl,
    -- Comorbidity flags
    r.family_history_flag,
    r.hypertension_flag,
    r.sleep_apnea_flag,
    r.smoking_flag,
    CASE WHEN r.gender = 'Male' THEN 1 ELSE 0 END AS gender_male_flag,
    r.diabetes_flag,
    ISNULL(sac.alcohol_flag, @mode_alcohol_flag) AS alcohol_flag,
    -- Tier 1: Primary sleep apnea predictors (mean imputation)
    ISNULL(sac.neck_circumference, @avg_neck_circ) AS neck_circumference,
    ISNULL(sac.snoring_flag, @mode_snoring_flag) AS snoring_flag,
    ISNULL(sac.fatigue_flag, @mode_fatigue_flag) AS fatigue_flag,
    -- Tier 1: PSG signals (mean imputation from 10-row source)
    ISNULL(sdb.oxygen_saturation, @avg_oxy_sat) AS oxygen_saturation,
    ISNULL(sdb.apnea_hypopnea_index, @avg_ahi) AS ahi_score,
    ISNULL(sdb.spo2_ratio, @avg_spo2_ratio) AS spo2_ratio,
    ISNULL(sdb.eeg_sleep_stage, @mode_eeg_stage) AS eeg_sleep_stage,
    ISNULL(sdb.nasal_airflow_rate, @avg_nasal_flow) AS nasal_airflow_rate,
    ISNULL(sdb.chest_movement_effort, @avg_chest_effort) AS chest_movement_effort,
    ISNULL(sdb.sleep_position, @mode_sleep_pos) AS sleep_position,
    -- Tier 1: Sleep quality (mean imputation)
    ISNULL(shl.sleep_duration_hrs, @avg_sleep_dur) AS sleep_duration_hrs,
    ISNULL(shl.quality_of_sleep, @avg_quality_sleep) AS quality_of_sleep,
    -- Tier 2: Lifestyle (mean imputation)
    ISNULL(shl.stress_level, @avg_stress) AS stress_level,
    ISNULL(shl.physical_activity_level, @avg_phys_activity) AS physical_activity_level,
    -- Tier 2: Clinical detail (mean imputation)
    ISNULL(dd.alcohol_consumption, @avg_alcohol_cons) AS alcohol_consumption,
    ISNULL(dd.fatigue_level, @avg_fatigue_level) AS fatigue_level,
    ISNULL(dd.diet_quality, @avg_diet_quality) AS diet_quality,
    ISNULL(dd.quality_of_life_score, @avg_qol_score) AS quality_of_life_score,
    -- Grouping helpers
    ISNULL(ab.label, 'Unknown') AS age_band,
    ISNULL(bc.label, 'Unknown') AS bmi_category
FROM silver.diabetes_sleep_link r
    LEFT JOIN gold.dim_age_band ab ON r.age >= ab.age_min
    AND r.age <= ab.age_max
    LEFT JOIN gold.dim_bmi_category bc ON r.bmi >= bc.bmi_min
    AND r.bmi <= bc.bmi_max
    AND bc.label != 'Unknown'
    -- Tier 1: physical OSA markers (patient_key match works well: ~828 rows)
    LEFT JOIN silver.sleep_apnea_core sac ON r.patient_key = sac.patient_key
    -- Tier 1: PSG signals (patient_key match: ~7 rows, only 10 in source)
    LEFT JOIN silver.sleep_disordered_breathing sdb ON r.patient_key = sdb.patient_key
    -- Tier 1+2: sleep lifestyle (join on age+gender because shl uses BMI midpoint hashes)
    LEFT JOIN (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY age, gender
            ORDER BY record_id
        ) AS rn
        FROM silver.sleep_health_lifestyle
    ) shl ON CAST(FLOOR(r.age) AS INT) = shl.age
         AND r.gender = shl.gender
         AND shl.rn = 1
    -- Tier 2: glycaemic/lipid detail (join on age+gender+rounded BMI for better overlap)
    LEFT JOIN (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY age, gender, CAST(ROUND(bmi,0) AS INT)
            ORDER BY record_id
        ) AS rn
        FROM silver.diabetes_detailed
    ) dd ON CAST(FLOOR(r.age) AS INT) = dd.age
         AND r.gender = dd.gender
         AND CAST(ROUND(r.bmi,0) AS INT) = CAST(ROUND(dd.bmi,0) AS INT)
         AND dd.rn = 1;
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's';
PRINT '>> -------------';
PRINT '';
PRINT '--- Loading Analytical Marts ---';
PRINT '------------------------------------------------';
-- ────────────────────────────────────────────────────
-- STEP 7: mart_comorbidity_by_age
-- Source: gold.fact_patient_health_snapshot (post step 6)
-- [NEW] avg_neck_circumference, snoring_count, fatigue_count,
--       avg_ahi_score, avg_spo2, snoring_rate_pct, fatigue_rate_pct
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 7: Loading gold.mart_comorbidity_by_age';
TRUNCATE TABLE gold.mart_comorbidity_by_age;
INSERT INTO gold.mart_comorbidity_by_age (
        age_band,
        total_patients,
        diabetes_count,
        sleep_apnea_count,
        hypertension_count,
        all_three_count,
        avg_bmi,
        avg_glucose,
        avg_blood_pressure,
        avg_neck_circumference,
        snoring_count,
        fatigue_count,
        avg_ahi_score,
        avg_spo2,
        diabetes_rate_pct,
        sleep_apnea_rate_pct,
        hypertension_rate_pct,
        all_three_rate_pct,
        snoring_rate_pct,
        fatigue_rate_pct
    )
SELECT age_band,
    COUNT(*) AS total_patients,
    SUM(diabetes_flag) AS diabetes_count,
    SUM(sleep_apnea_flag) AS sleep_apnea_count,
    SUM(hypertension_flag) AS hypertension_count,
    SUM(
        CASE
            WHEN diabetes_flag = 1
            AND sleep_apnea_flag = 1
            AND hypertension_flag = 1 THEN 1
            ELSE 0
        END
    ) AS all_three_count,
    ROUND(AVG(CAST(bmi AS FLOAT)), 2) AS avg_bmi,
    ROUND(AVG(CAST(glucose AS FLOAT)), 2) AS avg_glucose,
    ROUND(AVG(CAST(blood_pressure AS FLOAT)), 2) AS avg_blood_pressure,
    -- [NEW] Tier 1 aggregates
    ROUND(AVG(CAST(neck_circumference AS FLOAT)), 2) AS avg_neck_circumference,
    SUM(ISNULL(snoring_flag, 0)) AS snoring_count,
    SUM(ISNULL(fatigue_flag, 0)) AS fatigue_count,
    ROUND(AVG(CAST(ahi_score AS FLOAT)), 2) AS avg_ahi_score,
    ROUND(AVG(CAST(oxygen_saturation AS FLOAT)), 2) AS avg_spo2,
    ROUND(100.0 * SUM(diabetes_flag) / COUNT(*), 1) AS diabetes_rate_pct,
    ROUND(100.0 * SUM(sleep_apnea_flag) / COUNT(*), 1) AS sleep_apnea_rate_pct,
    ROUND(100.0 * SUM(hypertension_flag) / COUNT(*), 1) AS hypertension_rate_pct,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN diabetes_flag = 1
                AND sleep_apnea_flag = 1
                AND hypertension_flag = 1 THEN 1
                ELSE 0
            END
        ) / COUNT(*),
        1
    ) AS all_three_rate_pct,
    -- [NEW] Tier 1 rate aggregates
    ROUND(
        100.0 * SUM(ISNULL(snoring_flag, 0)) / COUNT(*),
        1
    ) AS snoring_rate_pct,
    ROUND(
        100.0 * SUM(ISNULL(fatigue_flag, 0)) / COUNT(*),
        1
    ) AS fatigue_rate_pct
FROM gold.fact_patient_health_snapshot
WHERE age_band IS NOT NULL
GROUP BY age_band
ORDER BY MIN(age);
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's';
PRINT '>> -------------';
-- ────────────────────────────────────────────────────
-- STEP 8: mart_bmi_sleep_disorder
-- Source: silver.sleep_patient_profile
-- [NEW] LEFT JOINs to silver.sleep_apnea_core and
--       silver.sleep_disordered_breathing to add:
--       avg_neck_circumference, snoring_rate_pct,
--       avg_ahi_score, avg_oxygen_saturation
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 8: Loading gold.mart_bmi_sleep_disorder';
TRUNCATE TABLE gold.mart_bmi_sleep_disorder;
INSERT INTO gold.mart_bmi_sleep_disorder (
        bmi_category,
        sleep_disorder,
        patient_count,
        avg_sleep_hrs,
        avg_stress,
        avg_systolic,
        avg_heart_rate,
        avg_neck_circumference,
        snoring_rate_pct,
        avg_ahi_score,
        avg_oxygen_saturation
    )
SELECT CASE
        WHEN shl.bmi_category IN ('Normal', 'Normal Weight') THEN 'Normal'
        WHEN shl.bmi_category = 'Overweight' THEN 'Overweight'
        WHEN shl.bmi_category = 'Obese' THEN 'Obese'
        ELSE 'Unknown'
    END AS bmi_category,
    ISNULL(NULLIF(TRIM(shl.sleep_disorder), ''), 'Unknown') AS sleep_disorder,
    COUNT(*) AS patient_count,
    ROUND(AVG(CAST(shl.sleep_duration_hrs AS FLOAT)), 2) AS avg_sleep_hrs,
    ROUND(AVG(CAST(shl.stress_level AS FLOAT)), 2) AS avg_stress,
    ROUND(AVG(CAST(shl.systolic_bp AS FLOAT)), 2) AS avg_systolic,
    ROUND(AVG(CAST(shl.heart_rate AS FLOAT)), 2) AS avg_heart_rate,
    -- [NEW] Tier 1 aggregates
    ROUND(AVG(CAST(sac.neck_circumference AS FLOAT)), 2) AS avg_neck_circumference,
    ROUND(
        100.0 * SUM(ISNULL(sac.snoring_flag, 0)) / COUNT(*),
        1
    ) AS snoring_rate_pct,
    ROUND(AVG(CAST(sdb.apnea_hypopnea_index AS FLOAT)), 2) AS avg_ahi_score,
    ROUND(AVG(CAST(sdb.oxygen_saturation AS FLOAT)), 2) AS avg_oxygen_saturation
FROM silver.sleep_health_lifestyle shl
    LEFT JOIN silver.sleep_apnea_core sac ON shl.patient_key = sac.patient_key
    LEFT JOIN silver.sleep_disordered_breathing sdb ON shl.patient_key = sdb.patient_key
WHERE shl.sleep_disorder != 'Unknown'
    OR shl.sleep_disorder IS NULL
GROUP BY CASE
        WHEN shl.bmi_category IN ('Normal', 'Normal Weight') THEN 'Normal'
        WHEN shl.bmi_category = 'Overweight' THEN 'Overweight'
        WHEN shl.bmi_category = 'Obese' THEN 'Obese'
        ELSE 'Unknown'
    END,
    ISNULL(NULLIF(TRIM(shl.sleep_disorder), ''), 'Unknown');
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's';
PRINT '>> -------------';
-- ────────────────────────────────────────────────────
-- STEP 9: mart_occupation_risk
-- Source: silver.sleep_patient_profile
-- [NEW] LEFT JOIN to silver.sleep_apnea_core to add:
--       avg_quality_of_sleep, avg_neck_circumference,
--       fatigue_rate_pct
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 9: Loading gold.mart_occupation_risk';
TRUNCATE TABLE gold.mart_occupation_risk;
INSERT INTO gold.mart_occupation_risk (
        occupation,
        total_patients,
        sleep_apnea_count,
        insomnia_count,
        avg_sleep_hrs,
        avg_stress,
        avg_systolic,
        avg_daily_steps,
        avg_quality_of_sleep,
        avg_neck_circumference,
        fatigue_rate_pct,
        sleep_apnea_rate_pct,
        insomnia_rate_pct
    )
SELECT shl.occupation,
    COUNT(*) AS total_patients,
    SUM(
        CASE
            WHEN shl.sleep_disorder = 'Sleep Apnea' THEN 1
            ELSE 0
        END
    ) AS sleep_apnea_count,
    SUM(
        CASE
            WHEN shl.sleep_disorder = 'Insomnia' THEN 1
            ELSE 0
        END
    ) AS insomnia_count,
    ROUND(AVG(CAST(shl.sleep_duration_hrs AS FLOAT)), 2) AS avg_sleep_hrs,
    ROUND(AVG(CAST(shl.stress_level AS FLOAT)), 2) AS avg_stress,
    ROUND(AVG(CAST(shl.systolic_bp AS FLOAT)), 2) AS avg_systolic,
    ROUND(AVG(CAST(shl.daily_steps AS FLOAT)), 2) AS avg_daily_steps,
    -- [NEW] Tier 1 aggregates
    ROUND(AVG(CAST(shl.quality_of_sleep AS FLOAT)), 2) AS avg_quality_of_sleep,
    ROUND(AVG(CAST(sac.neck_circumference AS FLOAT)), 2) AS avg_neck_circumference,
    ROUND(
        100.0 * SUM(ISNULL(sac.fatigue_flag, 0)) / COUNT(*),
        1
    ) AS fatigue_rate_pct,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN shl.sleep_disorder = 'Sleep Apnea' THEN 1
                ELSE 0
            END
        ) / COUNT(*),
        1
    ) AS sleep_apnea_rate_pct,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN shl.sleep_disorder = 'Insomnia' THEN 1
                ELSE 0
            END
        ) / COUNT(*),
        1
    ) AS insomnia_rate_pct
FROM silver.sleep_health_lifestyle shl
    LEFT JOIN silver.sleep_apnea_core sac ON shl.patient_key = sac.patient_key
GROUP BY shl.occupation;
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's';
PRINT '>> -------------';
-- ────────────────────────────────────────────────────
-- STEP 10: mart_clinical_severity
-- Primary source: silver.sleep_apnea_clinical (PSG data)
-- [NEW] LEFT JOIN to silver.sleep_apnea_core for:
--       neck_circumference, snoring, fatigue_flag
-- [NEW] All PSG signal columns from
--       silver.sleep_disordered_breathing:
--       spo2_ratio, oxygen_saturation, nasal_airflow_rate,
--       chest_movement_effort, eeg_sleep_stage,
--       sleep_position, ecg_heart_rate
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 10: Loading gold.mart_clinical_severity';
TRUNCATE TABLE gold.mart_clinical_severity;
INSERT INTO gold.mart_clinical_severity (
        patient_id,
        age,
        gender,
        bmi,
        bmi_category,
        neck_circumference,
        snoring,
        ahi_score,
        spo2_pct,
        spo2_ratio,
        oxygen_saturation,
        nasal_airflow_rate,
        chest_movement_effort,
        eeg_sleep_stage,
        sleep_position,
        ecg_heart_rate,
        fatigue_flag,
        severity,
        treatment,
        diagnosed_sdb
    )
SELECT sdb.record_id AS patient_id,
    sdb.age,
    sdb.gender,
    sdb.bmi,
    CASE
        WHEN sdb.bmi < 25 THEN 'Normal'
        WHEN sdb.bmi < 30 THEN 'Overweight'
        WHEN sdb.bmi >= 30 THEN 'Obese'
        ELSE 'Unknown'
    END AS bmi_category,
    -- Physical OSA markers (silver.sleep_apnea_core)
    sac.neck_circumference,
    sac.snoring_flag AS snoring,
    -- PSG measures (silver.sleep_disordered_breathing)
    sdb.apnea_hypopnea_index AS ahi_score,
    sdb.oxygen_saturation AS spo2_pct,
    sdb.spo2_ratio,
    sdb.oxygen_saturation,
    sdb.nasal_airflow_rate,
    sdb.chest_movement_effort,
    sdb.eeg_sleep_stage,
    sdb.sleep_position,
    sdb.ecg_heart_rate,
    -- Daytime symptom (silver.sleep_apnea_core)
    sac.fatigue_flag,
    sdb.severity,
    sdb.treatment,
    sdb.diagnosis_sdb AS diagnosed_sdb
FROM silver.sleep_disordered_breathing sdb
    LEFT JOIN silver.sleep_apnea_core sac ON sdb.patient_key = sac.patient_key;
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's';
PRINT '>> -------------';
-- ────────────────────────────────────────────────────
-- STEP 11: mart_epidemiology_combined
-- Source: JOIN silver.epidemiology_diabetes_2016
--           + silver.epidemiology_diab_hyp_2016 on census_tract
-- Unchanged — no sleep apnea columns applicable here
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 11: Loading gold.mart_epidemiology_combined';
TRUNCATE TABLE gold.mart_epidemiology_combined;
INSERT INTO gold.mart_epidemiology_combined (
        census_tract,
        bp_adult_diab,
        bp_adult_nondiab,
        bw_adult_diab,
        bw_adult_nondiab,
        bm_adult_diab,
        bm_adult_nondiab,
        total_bp_diab_hyp,
        total_bp_nondiab_hyp,
        total_bw_diab_hyp,
        total_bw_nondiab_hyp,
        total_bm_diab_hyp,
        total_bm_nondiab_hyp,
        diab_hyp_overlap_rate
    )
SELECT CAST(d.area_code AS BIGINT) AS census_tract,
    d.bp_affected_diabetic AS bp_adult_diab,
    d.bp_affected_non_diab AS bp_adult_nondiab,
    d.bw_affected_diabetic AS bw_adult_diab,
    d.bw_affected_non_diab AS bw_adult_nondiab,
    d.bm_affected_diabetic AS bm_adult_diab,
    d.bm_affected_non_diab AS bm_adult_nondiab,
    h.both_affected_diabetic AS total_bp_diab_hyp,
    h.both_affected_non_diab AS total_bp_nondiab_hyp,
    h.both_bw_affected_diabetic AS total_bw_diab_hyp,
    h.both_bw_affected_non_diab AS total_bw_nondiab_hyp,
    h.both_bm_affected_diabetic AS total_bm_diab_hyp,
    h.both_bm_affected_non_diab AS total_bm_nondiab_hyp,
    CASE
        WHEN d.bp_affected_diabetic = 0 THEN NULL
        ELSE ROUND(
            CAST(h.both_affected_diabetic AS FLOAT) / d.bp_affected_diabetic,
            4
        )
    END AS diab_hyp_overlap_rate
FROM silver.diabetes_aggregate_2016 d
    INNER JOIN silver.diabetes_hypertension_aggregate_2016 h ON d.area_code = h.area_code;
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's';
PRINT '>> -------------';
-- ────────────────────────────────────────────────────
-- STEP 12: mart_diabetes_risk_profile
-- Primary source: silver.diabetes_prediction (96k rows)
-- [NEW] LEFT JOIN to silver.diabetes_detailed for:
--       avg_cholesterol_total, avg_cholesterol_ldl,
--       avg_cholesterol_hdl
-- [NEW] LEFT JOIN to silver.sleep_apnea_core for:
--       sleep_apnea_count, avg_neck_circumference,
--       sleep_apnea_rate_pct
-- ────────────────────────────────────────────────────
SET @start_time = GETDATE();
PRINT '>> STEP 12: Loading gold.mart_diabetes_risk_profile';
TRUNCATE TABLE gold.mart_diabetes_risk_profile;
INSERT INTO gold.mart_diabetes_risk_profile (
        age_band,
        gender,
        bmi_category,
        total,
        diabetes_count,
        hypertension_count,
        sleep_apnea_count,
        avg_hba1c,
        avg_glucose,
        avg_cholesterol_total,
        avg_cholesterol_ldl,
        avg_cholesterol_hdl,
        avg_neck_circumference,
        diabetes_rate_pct,
        sleep_apnea_rate_pct
    )
SELECT CASE
        WHEN p.age < 18 THEN 'Under 18'
        WHEN p.age <= 30 THEN '18-30'
        WHEN p.age <= 45 THEN '31-45'
        WHEN p.age <= 60 THEN '46-60'
        ELSE '61+'
    END AS age_band,
    p.gender,
    CASE
        WHEN p.bmi < 25 THEN 'Normal'
        WHEN p.bmi < 30 THEN 'Overweight'
        WHEN p.bmi >= 30 THEN 'Obese'
        ELSE 'Unknown'
    END AS bmi_category,
    COUNT(*) AS total,
    SUM(p.diabetes_flag) AS diabetes_count,
    SUM(p.hypertension_flag) AS hypertension_count,
    -- [NEW] sleep apnea co-occurrence in diabetic segments
    SUM(ISNULL(sac.sleep_apnea_flag, 0)) AS sleep_apnea_count,
    ROUND(AVG(CAST(p.hba1c_level AS FLOAT)), 6) AS avg_hba1c,
    ROUND(AVG(CAST(p.blood_glucose_level AS FLOAT)), 6) AS avg_glucose,
    -- [NEW] lipid panel (silver.diabetes_detailed)
    ROUND(AVG(CAST(dd.cholesterol_total AS FLOAT)), 2) AS avg_cholesterol_total,
    ROUND(AVG(CAST(dd.cholesterol_ldl AS FLOAT)), 2) AS avg_cholesterol_ldl,
    ROUND(AVG(CAST(dd.cholesterol_hdl AS FLOAT)), 2) AS avg_cholesterol_hdl,
    -- [NEW] OSA physical marker (silver.sleep_apnea_core)
    ROUND(AVG(CAST(sac.neck_circumference AS FLOAT)), 2) AS avg_neck_circumference,
    ROUND(100.0 * SUM(p.diabetes_flag) / COUNT(*), 1) AS diabetes_rate_pct,
    -- [NEW] OSA prevalence within each diabetes segment
    ROUND(
        100.0 * SUM(ISNULL(sac.sleep_apnea_flag, 0)) / COUNT(*),
        1
    ) AS sleep_apnea_rate_pct
FROM silver.diabetes_prediction p
    LEFT JOIN silver.diabetes_detailed dd ON p.patient_key = dd.patient_key
    LEFT JOIN silver.sleep_apnea_core sac ON p.patient_key = sac.patient_key
WHERE p.gender != 'Unknown'
GROUP BY CASE
        WHEN p.age < 18 THEN 'Under 18'
        WHEN p.age <= 30 THEN '18-30'
        WHEN p.age <= 45 THEN '31-45'
        WHEN p.age <= 60 THEN '46-60'
        ELSE '61+'
    END,
    p.gender,
    CASE
        WHEN p.bmi < 25 THEN 'Normal'
        WHEN p.bmi < 30 THEN 'Overweight'
        WHEN p.bmi >= 30 THEN 'Obese'
        ELSE 'Unknown'
    END;
SET @end_time = GETDATE();
PRINT '>> Duration: ' + CAST(
    DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR
) + 's';
PRINT '>> -------------';
SET @batch_end = GETDATE();
PRINT '';
PRINT '==========================================';
PRINT 'Gold Layer Load Complete';
PRINT '   Total Duration: ' + CAST(
    DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR
) + ' seconds';
PRINT '==========================================';
END TRY BEGIN CATCH PRINT '==========================================';
PRINT 'ERROR: ' + ERROR_MESSAGE();
PRINT 'Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
PRINT 'State : ' + CAST(ERROR_STATE() AS NVARCHAR);
PRINT '==========================================';
END CATCH
END
GO
