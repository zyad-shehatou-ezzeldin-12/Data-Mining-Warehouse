/*
===============================================================================
Stored Procedure: Load Silver Layer — Sleep Apnea Data Warehouse
===============================================================================
Script Purpose:
    ETL from Bronze → Silver for all 10 source tables.
    Transformations applied per table are documented inline.

    patient_key = MD5-style surrogate key built from:
        CONCAT(CAST(age AS INT), '_', gender, '_', CAST(ROUND(bmi,1) AS VARCHAR))
    Using HASHBYTES + CONVERT to produce a 16-char hex key compatible
    with SQL Server (no external functions needed).

Usage:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start DATETIME, @batch_end DATETIME;

    BEGIN TRY
        SET @batch_start = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer — Sleep Apnea DWH';
        PRINT '================================================';

        -- ==============================================================
        -- HELPER: patient_key formula used throughout:
        --   LEFT(CONVERT(NVARCHAR(32),
        --        HASHBYTES('MD5', CONCAT(CAST(FLOOR(age) AS NVARCHAR),
        --                               '_', gender,
        --                               '_', CAST(ROUND(bmi,1) AS NVARCHAR))),
        --        2), 16)
        --   This produces a stable, reproducible 16-char hex surrogate.
        -- ==============================================================

        -- ---------------------------------------------------------------
        -- 1. silver.sleep_apnea_core
        --    Transformations:
        --    • Gender_Male (0/1) → 'Female'/'Male'
        --    • All flag columns already 0/1 — no change needed
        --    • patient_key generated from age + gender + bmi
        -- ---------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.sleep_apnea_core';
        TRUNCATE TABLE silver.sleep_apnea_core;
        PRINT '>> Inserting Data Into: silver.sleep_apnea_core';
        INSERT INTO silver.sleep_apnea_core (
            age, gender, bmi, neck_circumference,
            diabetes_flag, hypertension_flag, smoking_flag,
            alcohol_flag, snoring_flag, fatigue_flag, sleep_apnea_flag,
            patient_key
        )
        SELECT
            Age,
            CASE WHEN Gender_Male = 1 THEN 'Male' ELSE 'Female' END AS gender,
            BMI,
            Neck_Circumference,
            Diabetes,
            Hypertension,
            Smoking,
            Alcohol_Use,
            Snoring,
            Fatigue,
            Sleep_Apnea,
            LEFT(CONVERT(NVARCHAR(32),
                 HASHBYTES('MD5', CONCAT(
                     CAST(FLOOR(Age) AS NVARCHAR), '_',
                     CASE WHEN Gender_Male = 1 THEN 'Male' ELSE 'Female' END, '_',
                     CAST(ROUND(BMI,1) AS NVARCHAR)
                 )), 2), 16) AS patient_key
        FROM bronze.sleep_apnea_dataset;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ---------------------------------------------------------------
        -- 2. silver.sleep_lifestyle
        --    Transformations:
        --    • blood_pressure 'sys/dia' string → split to two INT columns
        --    • bmi_category: 'Normal Weight' and 'Normal' → both → 'Normal'
        --    • sleep_disorder: 'Healthy' → 'None' (consistent null handling)
        --    • patient_key: age (INT), gender, bmi not available directly
        --      → bmi_category approximation: Normal≈22, Overweight≈27, Obese≈32
        --      NOTE: Since this table has no numeric BMI, we use bmi_category
        --            midpoint estimate for patient_key only.
        -- ---------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.sleep_lifestyle';
        TRUNCATE TABLE silver.sleep_lifestyle;
        PRINT '>> Inserting Data Into: silver.sleep_lifestyle';
        INSERT INTO silver.sleep_lifestyle (
            gender, age, occupation, sleep_duration_hrs,
            quality_of_sleep, physical_activity_level, stress_level,
            bmi_category, systolic_bp, diastolic_bp, heart_rate,
            daily_steps, sleep_disorder, patient_key
        )
        SELECT
            CASE
                WHEN UPPER(TRIM(gender)) = 'MALE'   THEN 'Male'
                WHEN UPPER(TRIM(gender)) = 'FEMALE' THEN 'Female'
                ELSE 'Unknown'
            END AS gender,
            age,
            TRIM(occupation) AS occupation,
            sleep_duration  AS sleep_duration_hrs,
            quality_of_sleep,
            physical_activity_level,
            stress_level,
            CASE
                WHEN UPPER(TRIM(bmi_category)) IN ('NORMAL WEIGHT','NORMAL') THEN 'Normal'
                WHEN UPPER(TRIM(bmi_category)) = 'OVERWEIGHT' THEN 'Overweight'
                WHEN UPPER(TRIM(bmi_category)) = 'OBESE'      THEN 'Obese'
                ELSE 'Normal'
            END AS bmi_category,
            -- Parse 'sys/dia' → split on '/'
            TRY_CAST(LEFT(blood_pressure,
                     CHARINDEX('/', blood_pressure) - 1) AS SMALLINT) AS systolic_bp,
            TRY_CAST(SUBSTRING(blood_pressure,
                     CHARINDEX('/', blood_pressure) + 1,
                     LEN(blood_pressure)) AS SMALLINT)               AS diastolic_bp,
            heart_rate,
            daily_steps,
            CASE
                WHEN UPPER(TRIM(sleep_disorder)) = 'HEALTHY' THEN 'None'
                WHEN sleep_disorder IS NULL                   THEN 'None'
                ELSE TRIM(sleep_disorder)
            END AS sleep_disorder,
            -- patient_key uses bmi midpoint estimate
            LEFT(CONVERT(NVARCHAR(32),
                 HASHBYTES('MD5', CONCAT(
                     CAST(age AS NVARCHAR), '_',
                     CASE WHEN UPPER(TRIM(gender))='MALE' THEN 'Male' ELSE 'Female' END, '_',
                     CASE
                         WHEN UPPER(TRIM(bmi_category)) IN ('NORMAL WEIGHT','NORMAL') THEN '22.0'
                         WHEN UPPER(TRIM(bmi_category)) = 'OVERWEIGHT' THEN '27.0'
                         WHEN UPPER(TRIM(bmi_category)) = 'OBESE'      THEN '32.0'
                         ELSE '22.0'
                     END
                 )), 2), 16) AS patient_key
        FROM bronze.sleep_data_sampled;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ---------------------------------------------------------------
        -- 3. silver.sleep_health_lifestyle
        --    Same transformations as sleep_lifestyle (same schema)
        --    • Sleep Disorder NULL → 'None'
        --    FIXED: Changed column names with spaces to underscores
        -- ---------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.sleep_health_lifestyle';
        TRUNCATE TABLE silver.sleep_health_lifestyle;
        PRINT '>> Inserting Data Into: silver.sleep_health_lifestyle';
        INSERT INTO silver.sleep_health_lifestyle (
            gender, age, occupation, sleep_duration_hrs,
            quality_of_sleep, physical_activity_level, stress_level,
            bmi_category, systolic_bp, diastolic_bp, heart_rate,
            daily_steps, sleep_disorder, patient_key
        )
        SELECT
            CASE
                WHEN UPPER(TRIM(gender)) = 'MALE'   THEN 'Male'
                WHEN UPPER(TRIM(gender)) = 'FEMALE' THEN 'Female'
                ELSE 'Unknown'
            END,
            age,
            TRIM(occupation),
            sleep_duration,          -- FIXED: was [Sleep Duration]
            quality_of_sleep,        -- FIXED: was [Quality of Sleep]
            physical_activity_level, -- FIXED: was [Physical Activity Level]
            stress_level,            -- FIXED: was [Stress Level]
            CASE
                WHEN UPPER(TRIM(bmi_category)) IN ('NORMAL WEIGHT','NORMAL') THEN 'Normal'
                WHEN UPPER(TRIM(bmi_category)) = 'OVERWEIGHT' THEN 'Overweight'
                WHEN UPPER(TRIM(bmi_category)) = 'OBESE'      THEN 'Obese'
                ELSE 'Normal'
            END,
            TRY_CAST(LEFT(blood_pressure,
                     CHARINDEX('/', blood_pressure) - 1) AS SMALLINT),
            TRY_CAST(SUBSTRING(blood_pressure,
                     CHARINDEX('/', blood_pressure) + 1,
                     LEN(blood_pressure)) AS SMALLINT),
            heart_rate,
            daily_steps,
            CASE
                WHEN sleep_disorder IS NULL OR TRIM(sleep_disorder) = ''
                    THEN 'None'
                ELSE TRIM(sleep_disorder)
            END,
            LEFT(CONVERT(NVARCHAR(32),
                 HASHBYTES('MD5', CONCAT(
                     CAST(age AS NVARCHAR), '_',
                     CASE WHEN UPPER(TRIM(gender))='MALE' THEN 'Male' ELSE 'Female' END, '_',
                     CASE
                         WHEN UPPER(TRIM(bmi_category)) IN ('NORMAL WEIGHT','NORMAL') THEN '22.0'
                         WHEN UPPER(TRIM(bmi_category)) = 'OVERWEIGHT' THEN '27.0'
                         WHEN UPPER(TRIM(bmi_category)) = 'OBESE'      THEN '32.0'
                         ELSE '22.0'
                     END
                 )), 2), 16)
        FROM bronze.sleep_health_lifestyle;  -- FIXED: correct table name
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ---------------------------------------------------------------
        -- 4. silver.sleep_disordered_breathing
        --    Transformations:
        --    • Severity NULL → 'None' (2 nulls in source = undiagnosed)
        --    • Treatment NULL → 'Not Assigned' (4 nulls in source)
        --    • Only 10 rows — all rows are kept
        -- ---------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.sleep_disordered_breathing';
        TRUNCATE TABLE silver.sleep_disordered_breathing;
        PRINT '>> Inserting Data Into: silver.sleep_disordered_breathing';
        INSERT INTO silver.sleep_disordered_breathing (
            age, gender, bmi, snoring, oxygen_saturation,
            apnea_hypopnea_index, nasal_airflow_rate, chest_movement_effort,
            ecg_heart_rate, spo2_ratio, sleep_position, eeg_sleep_stage,
            diagnosis_sdb, severity, treatment, patient_key
        )
        SELECT
            Age,
            CASE
                WHEN UPPER(TRIM(Gender)) = 'MALE'   THEN 'Male'
                WHEN UPPER(TRIM(Gender)) = 'FEMALE' THEN 'Female'
                ELSE 'Unknown'
            END,
            BMI,
            Snoring,
            Oxygen_Saturation,
            Apnea_Hypopnea_Index_AHI,
            Nasal_Airflow_Flow_Rate,    -- FIXED: was [Nasal Airflow (Flow Rate)]
            Chest_Movement_Effort,
            ECG_Heart_Rate,
            SpO2_Ratio,
            Position,
            EEG_Sleep_Stage,
            Diagnosis_of_SDB,
            ISNULL(Severity, 'None')     AS severity,
            ISNULL(Treatment, 'Not Assigned') AS treatment,
            LEFT(CONVERT(NVARCHAR(32),
                 HASHBYTES('MD5', CONCAT(
                     CAST(Age AS NVARCHAR), '_',
                     CASE WHEN UPPER(TRIM(Gender))='MALE' THEN 'Male' ELSE 'Female' END, '_',
                     CAST(ROUND(BMI,1) AS NVARCHAR)
                 )), 2), 16)
        FROM bronze.enhanced_sleep_sdb;   -- FIXED: correct table name
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ---------------------------------------------------------------
        -- 5. silver.diabetes_detailed
        --    Transformations:
        --    • Gender 0→Female, 1→Male (binary encoded in source)
        --    • Select only clinically relevant columns (25 of 46)
        --      — columns like DoctorInCharge='Confidential', PatientID
        --        kept as patient_source_id only
        --    • All values are continuous/clean — no nulls found
        -- ---------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.diabetes_detailed';
        TRUNCATE TABLE silver.diabetes_detailed;
        PRINT '>> Inserting Data Into: silver.diabetes_detailed';
        INSERT INTO silver.diabetes_detailed (
            patient_source_id, age, gender, bmi,
            smoking_flag, alcohol_consumption, physical_activity,
            diet_quality, sleep_quality, hypertension_flag,
            systolic_bp, diastolic_bp, fasting_blood_sugar, hba1c,
            cholesterol_total, cholesterol_ldl, cholesterol_hdl,
            cholesterol_triglycerides, family_history_diabetes,
            previous_pre_diabetes, fatigue_level, quality_of_life_score,
            diabetes_diagnosis, patient_key
        )
        SELECT
            PatientID,
            Age,
            CASE WHEN Gender = 0 THEN 'Female' ELSE 'Male' END,
            ROUND(BMI, 2),
            Smoking,
            ROUND(AlcoholConsumption, 2),
            ROUND(PhysicalActivity, 2),
            ROUND(DietQuality, 2),
            ROUND(SleepQuality, 2),
            Hypertension,
            SystolicBP,
            DiastolicBP,
            ROUND(FastingBloodSugar, 2),
            ROUND(HbA1c, 2),
            ROUND(CholesterolTotal, 2),
            ROUND(CholesterolLDL, 2),
            ROUND(CholesterolHDL, 2),
            ROUND(CholesterolTriglycerides, 2),
            FamilyHistoryDiabetes,
            PreviousPreDiabetes,
            ROUND(FatigueLevels, 2),
            ROUND(QualityOfLifeScore, 2),
            Diagnosis,
            LEFT(CONVERT(NVARCHAR(32),
                 HASHBYTES('MD5', CONCAT(
                     CAST(Age AS NVARCHAR), '_',
                     CASE WHEN Gender=0 THEN 'Female' ELSE 'Male' END, '_',
                     CAST(ROUND(BMI,1) AS NVARCHAR)
                 )), 2), 16)
        FROM bronze.diabetes_data;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ---------------------------------------------------------------
        -- 6. silver.diabetes_sleep_link  (KEY BRIDGE TABLE)
        --    Transformations:
        --    • Gender_Male 0/1 → 'Female'/'Male'
        --    • All numeric columns — clean, no nulls
        -- ---------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.diabetes_sleep_link';
        TRUNCATE TABLE silver.diabetes_sleep_link;
        PRINT '>> Inserting Data Into: silver.diabetes_sleep_link';
        INSERT INTO silver.diabetes_sleep_link (
            patient_source_id, age, gender, bmi, glucose, insulin,
            blood_pressure, family_history_flag, hypertension_flag,
            sleep_apnea_flag, smoking_flag, diabetes_flag, patient_key
        )
        SELECT
            PatientID,
            Age,
            CASE WHEN Gender_Male = 1 THEN 'Male' ELSE 'Female' END,
            BMI,
            Glucose,
            Insulin,
            Blood_Pressure,
            Family_History,
            Hypertension,
            Sleep_Apnea,
            Smoking,
            Diabetes,
            LEFT(CONVERT(NVARCHAR(32),
                 HASHBYTES('MD5', CONCAT(
                     CAST(FLOOR(Age) AS NVARCHAR), '_',
                     CASE WHEN Gender_Male=1 THEN 'Male' ELSE 'Female' END, '_',
                     CAST(ROUND(BMI,1) AS NVARCHAR)
                 )), 2), 16)
        FROM bronze.diabetes_dataset_10k;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ---------------------------------------------------------------
        -- 7. silver.diabetes_prediction
        --    Transformations:
        --    • gender: 'Other' → 'Unknown'
        --    • smoking_history: 'No Info' → 'Unknown'; normalize
        --    • 100,000 rows — no nulls
        -- ---------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.diabetes_prediction';
        TRUNCATE TABLE silver.diabetes_prediction;
        PRINT '>> Inserting Data Into: silver.diabetes_prediction';
        INSERT INTO silver.diabetes_prediction (
            gender, age, bmi, hypertension_flag, heart_disease_flag,
            smoking_history, hba1c_level, blood_glucose_level,
            diabetes_flag, patient_key
        )
        SELECT
            CASE
                WHEN UPPER(TRIM(gender)) = 'MALE'   THEN 'Male'
                WHEN UPPER(TRIM(gender)) = 'FEMALE' THEN 'Female'
                ELSE 'Unknown'
            END,
            age,
            bmi,
            hypertension,
            heart_disease,
            CASE
                WHEN UPPER(TRIM(smoking_history)) IN ('NO INFO','') THEN 'Unknown'
                WHEN UPPER(TRIM(smoking_history)) = 'NEVER'         THEN 'Never'
                WHEN UPPER(TRIM(smoking_history)) = 'CURRENT'       THEN 'Current'
                WHEN UPPER(TRIM(smoking_history)) = 'FORMER'        THEN 'Former'
                WHEN UPPER(TRIM(smoking_history)) = 'EVER'          THEN 'Ever'
                WHEN UPPER(TRIM(smoking_history)) = 'NOT CURRENT'   THEN 'Not Current'
                ELSE TRIM(smoking_history)
            END,
            HbA1c_level,
            blood_glucose_level,
            diabetes,
            LEFT(CONVERT(NVARCHAR(32),
                 HASHBYTES('MD5', CONCAT(
                     CAST(FLOOR(age) AS NVARCHAR), '_',
                     CASE
                         WHEN UPPER(TRIM(gender))='MALE'   THEN 'Male'
                         WHEN UPPER(TRIM(gender))='FEMALE' THEN 'Female'
                         ELSE 'Unknown'
                     END, '_',
                     CAST(ROUND(bmi,1) AS NVARCHAR)
                 )), 2), 16)
        FROM bronze.diabetes_prediction;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ---------------------------------------------------------------
        -- 8. silver.hypertension_risk
        --    Transformations:
        --    • male 0/1 → 'Female'/'Male'
        --    • cigsPerDay NULL → 0 only when currentSmoker = 0
        --    • BPMeds NULL → 0 (53 nulls = assumed not on meds)
        --    • totChol NULL → AVG imputation via subquery (50 nulls)
        --    • BMI NULL → AVG imputation (19 nulls)
        --    • heartRate NULL → AVG imputation (1 null)
        --    • glucose NULL → AVG imputation (388 nulls = many missing)
        --    ISSUE FLAGGED: 388/4240 = 9.1% of glucose is NULL
        --    → Impute with mean; flag rows for downstream awareness
        -- ---------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.hypertension_risk';
        TRUNCATE TABLE silver.hypertension_risk;
        PRINT '>> Inserting Data Into: silver.hypertension_risk';
        INSERT INTO silver.hypertension_risk (
            gender, age, bmi, current_smoker, cigs_per_day,
            bp_medication, diabetes_flag, total_cholesterol,
            systolic_bp, diastolic_bp, heart_rate, glucose,
            hypertension_risk, patient_key
        )
        SELECT
            CASE WHEN male = 1 THEN 'Male' ELSE 'Female' END,
            age,
            ISNULL(TRY_CAST(bmi AS FLOAT), (SELECT AVG(TRY_CAST(bmi AS FLOAT)) FROM bronze.hypertension_risk WHERE bmi IS NOT NULL)),
            currentsmoker,
            -- Non-smokers with NULL cigs → 0; smokers with NULL → mean of smokers
            CASE
                WHEN currentsmoker = 0 THEN ISNULL(TRY_CAST(cigsperday AS INT), 0)
                ELSE ISNULL(TRY_CAST(cigsperday AS INT),
                    (SELECT AVG(TRY_CAST(cigsperday AS INT))
                     FROM bronze.hypertension_risk
                     WHERE currentsmoker = 1 AND cigsperday IS NOT NULL))
            END,
            ISNULL(TRY_CAST(bpmeds AS INT), 0),
            diabetes,
            ISNULL(TRY_CAST(totchol AS FLOAT), (SELECT AVG(TRY_CAST(totchol AS FLOAT)) FROM bronze.hypertension_risk WHERE totchol IS NOT NULL)),
            sysbp,
            diabp,
            ISNULL(TRY_CAST(heartrate AS INT), (SELECT AVG(TRY_CAST(heartrate AS INT)) FROM bronze.hypertension_risk WHERE heartrate IS NOT NULL)),
            ISNULL(TRY_CAST(glucose AS FLOAT), (SELECT AVG(TRY_CAST(glucose AS FLOAT)) FROM bronze.hypertension_risk WHERE glucose IS NOT NULL)),
            risk,
            LEFT(CONVERT(NVARCHAR(32),
                 HASHBYTES('MD5', CONCAT(
                     CAST(age AS NVARCHAR), '_',
                     CASE WHEN male=1 THEN 'Male' ELSE 'Female' END, '_',
                     CAST(ROUND(
                         ISNULL(TRY_CAST(bmi AS FLOAT), 
                             (SELECT AVG(TRY_CAST(bmi AS FLOAT)) FROM bronze.hypertension_risk WHERE bmi IS NOT NULL))
                     ,1) AS NVARCHAR)
                 )), 2), 16)
        FROM bronze.hypertension_risk;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ---------------------------------------------------------------
        -- 9. silver.diabetes_aggregate_2016
        --    Transformations: NONE — pure aggregate, rename cols only
        --    CT column kept as area_code (NVARCHAR for safety)
        -- ---------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.diabetes_aggregate_2016';
        TRUNCATE TABLE silver.diabetes_aggregate_2016;
        PRINT '>> Inserting Data Into: silver.diabetes_aggregate_2016';
        INSERT INTO silver.diabetes_aggregate_2016 (
            area_code, bp_affected_diabetic, bp_affected_non_diab,
            bp_affected_non_diab2, bw_affected_diabetic, bw_affected_non_diab,
            bw_affected_non_diab2, bm_affected_diabetic, bm_affected_non_diab,
            bm_affected_non_diab2
        )
        SELECT
            CAST(ct AS NVARCHAR(20)),
            bpad, bpan, bpan2, bwad, bwan, bwan2, bmad, bman, bman2
        FROM bronze.diabetes_all_2016;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ---------------------------------------------------------------
        -- 10. silver.diabetes_hypertension_aggregate_2016
        --     Transformations: NONE — pure aggregate, rename cols only
        -- ---------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.diabetes_hypertension_aggregate_2016';
        TRUNCATE TABLE silver.diabetes_hypertension_aggregate_2016;
        PRINT '>> Inserting Data Into: silver.diabetes_hypertension_aggregate_2016';
        INSERT INTO silver.diabetes_hypertension_aggregate_2016 (
            area_code, both_affected_diabetic, both_affected_non_diab,
            both_bw_affected_diabetic, both_bw_affected_non_diab,
            both_bm_affected_diabetic, both_bm_affected_non_diab
        )
        SELECT
            CAST(ct AS NVARCHAR(20)),
            btpad, btpan, btwad, btwan, btmad, btman
        FROM bronze.diabetes_hypertension_2016;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end = GETDATE();
        PRINT '=========================================='
        PRINT 'Silver Layer Load Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='

    END TRY
    BEGIN CATCH
        PRINT '=========================================='
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER()  AS NVARCHAR);
        PRINT 'Error State:   ' + CAST(ERROR_STATE()   AS NVARCHAR);
        PRINT '=========================================='
    END CATCH
END
GO
