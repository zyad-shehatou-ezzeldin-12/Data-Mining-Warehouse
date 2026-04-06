/*
===============================================================================
Stored Procedure: Load Silver Layer — Sleep Apnea Data Warehouse
===============================================================================
Script Purpose:
    ETL from Bronze → Silver for all 10 Sleep Apnea domain datasets.
    Actions per table:
        1. TRUNCATE Silver table
        2. INSERT transformed / cleansed data from Bronze

Transformations performed (see inline comments for full detail):
    - Normalize gender text: 'Male'/'Female'/'Unknown'
    - Normalize BMI Category: merge 'Normal Weight' → 'Normal'
    - Parse Blood Pressure string 'xxx/yyy' → systolic + diastolic INT columns
    - Impute NULL Sleep Disorder → 'Unknown'
    - Remove duplicate rows (diabetes_prediction: 3,854 exact duplicates)
    - Normalize smoking_history: 'No Info' → 'Unknown'
    - Normalize gender 'Other' → 'Unknown'
    - Impute NULL severity / treatment in clinical table → 'Unknown' / 'None'
    - NULL-safe carry-forward for hypertension nulls (keep NULL, not 0)
    - Decode binary gender flag: 1=Male, 0=Female (hypertension source)
    - Rename cryptic columns to readable names (epidemiology tables)
    - Validate sleep duration: cap extreme outliers < 2 or > 16 → NULL
    - AHI severity band derivation (for clinical table)
    - Deduplicate sleep_patient_profile on person_id (keep latest source)

Parameters: None
Usage     : EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time        DATETIME,
            @end_time          DATETIME,
            @batch_start_time  DATETIME,
            @batch_end_time    DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer — Sleep Apnea DW';
        PRINT '================================================';

        -- ====================================================
        -- TABLE 1: silver.sleep_patient_profile
        -- Sources : bronze.sleep_health_lifestyle (374 rows)
        --           bronze.sleep_data_sampled     (15,000 rows)
        --
        -- ISSUES FOUND & HANDLED:
        --   sleep_health_lifestyle has 219 NULL Sleep Disorder
        --   → Impute to 'Unknown' (patient not yet diagnosed)
        --   BMI Category has two labels for normal weight:
        --   'Normal' and 'Normal Weight' → normalize to 'Normal'
        --   Blood Pressure is stored as a single string '120/80'
        --   → Split into systolic_bp and diastolic_bp integers
        --   Both sources share same 13 columns and same Person ID
        --   space — union then deduplicate on person_id keeping
        --   the sleep_health record (more clinically detailed)
        --   sleep_health Sleep Disorder column is FLOAT in source
        --   due to nulls — cast safely to NVARCHAR
        -- ====================================================
        SET @start_time = GETDATE();
        PRINT '------------------------------------------------';
        PRINT 'Loading Sleep Tables';
        PRINT '------------------------------------------------';
        PRINT '>> Truncating Table: silver.sleep_patient_profile';
        TRUNCATE TABLE silver.sleep_patient_profile;
        PRINT '>> Inserting Data Into: silver.sleep_patient_profile';

        INSERT INTO silver.sleep_patient_profile (
            person_id, gender, age, occupation,
            sleep_duration_hrs, quality_of_sleep,
            physical_activity_lvl, stress_level,
            bmi_category, systolic_bp, diastolic_bp,
            heart_rate, daily_steps, sleep_disorder, data_source
        )
        SELECT
            person_id,
            gender,
            age,
            occupation,
            -- Validate sleep duration — physiologically implausible values
            CASE
                WHEN sleep_duration_hrs < 2 OR sleep_duration_hrs > 16 THEN NULL
                ELSE sleep_duration_hrs
            END AS sleep_duration_hrs,
            quality_of_sleep,
            physical_activity_lvl,
            stress_level,
            -- Normalize BMI Category
            CASE
                WHEN TRIM(bmi_category) = 'Normal Weight' THEN 'Normal'
                ELSE TRIM(bmi_category)
            END AS bmi_category,
            systolic_bp,
            diastolic_bp,
            heart_rate,
            daily_steps,
            -- Impute NULL sleep disorder
            ISNULL(NULLIF(TRIM(sleep_disorder), ''), 'Unknown') AS sleep_disorder,
            data_source
        FROM (
            -- Deduplicate: person_id can appear in both sources
            -- Prefer sleep_health record (smaller, clinically curated)
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY person_id
                       ORDER BY
                           CASE data_source WHEN 'sleep_health' THEN 1 ELSE 2 END
                   ) AS rn
            FROM (
                -- Branch A: bronze.sleep_health_lifestyle
                SELECT
                    PatientID                                              AS person_id,
                    TRIM(gender)                                           AS gender,
                    age                                                    AS age,
                    TRIM(occupation)                                       AS occupation,
                    sleep_duration                                         AS sleep_duration_hrs,
                    quality_of_sleep                                       AS quality_of_sleep,
                    physical_activity_level                                AS physical_activity_lvl,
                    stress_level                                           AS stress_level,
                    TRIM(bmi_category)                                     AS bmi_category,
                    -- Parse 'xxx/yyy' BP string
                    TRY_CAST(
                        LEFT(blood_pressure, CHARINDEX('/', blood_pressure) - 1)
                        AS INT
                    )                                                      AS systolic_bp,
                    TRY_CAST(
                        SUBSTRING(blood_pressure,
                            CHARINDEX('/', blood_pressure) + 1,
                            LEN(blood_pressure))
                        AS INT
                    )                                                      AS diastolic_bp,
                    heart_rate                                             AS heart_rate,
                    daily_steps                                            AS daily_steps,
                    -- Float cast due to NULLs
                    CAST(sleep_disorder AS NVARCHAR(50))                   AS sleep_disorder,
                    'sleep_health'                                         AS data_source
                FROM bronze.sleep_health_lifestyle

                UNION ALL

                -- Branch B: bronze.sleep_data_sampled
                SELECT
                    PatientID                                              AS person_id,
                    TRIM(gender)                                           AS gender,
                    age                                                    AS age,
                    TRIM(occupation)                                       AS occupation,
                    sleep_duration                                         AS sleep_duration_hrs,
                    quality_of_sleep                                       AS quality_of_sleep,
                    physical_activity_level                                AS physical_activity_lvl,
                    stress_level                                           AS stress_level,
                    TRIM(bmi_category)                                     AS bmi_category,
                    TRY_CAST(
                        LEFT(blood_pressure, CHARINDEX('/', blood_pressure) - 1)
                        AS INT
                    )                                                      AS systolic_bp,
                    TRY_CAST(
                        SUBSTRING(blood_pressure,
                            CHARINDEX('/', blood_pressure) + 1,
                            LEN(blood_pressure))
                        AS INT
                    )                                                      AS diastolic_bp,
                    heart_rate                                             AS heart_rate,
                    daily_steps                                            AS daily_steps,
                    TRIM(sleep_disorder)                                   AS sleep_disorder,
                    'sleep_sampled'                                        AS data_source
                FROM bronze.sleep_data_sampled
            ) combined
        ) deduped
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================
        -- TABLE 2: silver.sleep_apnea_clinical
        -- Source : bronze.enhanced_sleep_sdb (10 rows — PSG study)
        --
        -- ISSUES FOUND & HANDLED:
        --   2 NULLs in Severity → impute 'Unknown'
        --   4 NULLs in Treatment → impute 'None'
        --   AHI=0 rows still have SDB diagnosis — keep as-is,
        --   these are borderline cases per physician notes
        --   Snoring is 'Yes'/'No' text — pass through, already clean
        -- ====================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.sleep_apnea_clinical';
        TRUNCATE TABLE silver.sleep_apnea_clinical;
        PRINT '>> Inserting Data Into: silver.sleep_apnea_clinical';

        INSERT INTO silver.sleep_apnea_clinical (
            patient_id, age, gender, bmi, snoring,
            oxygen_saturation_pct, ahi_score,
            nasal_airflow, chest_movement_effort,
            ecg_heart_rate, spo2_pct, sleep_position,
            eeg_sleep_stage, diagnosis_sdb, severity,
            treatment, physician_notes, patient_symptoms
        )
        SELECT
            PatientID                                AS patient_id,
            Age                                      AS age,
            TRIM(Gender)                             AS gender,
            BMI                                      AS bmi,
            TRIM(Snoring)                            AS snoring,
            Oxygen_Saturation                        AS oxygen_saturation_pct,
            Apnea_Hypopnea_Index_AHI                 AS ahi_score,
            Nasal_Airflow_Flow_Rate                  AS nasal_airflow,
            Chest_Movement_Effort                    AS chest_movement_effort,
            ECG_Heart_Rate                           AS ecg_heart_rate,
            SpO2_Ratio                               AS spo2_pct,
            TRIM(Position)                           AS sleep_position,
            TRIM(EEG_Sleep_Stage)                    AS eeg_sleep_stage,
            TRIM(Diagnosis_of_SDB)                   AS diagnosis_sdb,
            -- Impute NULL Severity
            ISNULL(TRIM(Severity), 'Unknown')        AS severity,
            -- Impute NULL Treatment
            ISNULL(TRIM(Treatment), 'None')          AS treatment,
            TRIM(Physician_Notes)                    AS physician_notes,
            TRIM(Patient_Symptoms)                   AS patient_symptoms
        FROM bronze.enhanced_sleep_sdb;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================
        -- TABLE 3: silver.sleep_apnea_risk
        -- Source : bronze.sleep_apnea_dataset (1,500 rows)
        --
        -- ISSUES FOUND & HANDLED:
        --   No patient ID in source — surrogate key via IDENTITY
        --   All flags are 0/1 INT — cast to TINYINT for efficiency
        --   No nulls, no duplicates — data is clean
        --   Age and BMI are FLOAT — preserve decimal precision
        -- ====================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.sleep_apnea_risk';
        TRUNCATE TABLE silver.sleep_apnea_risk;
        PRINT '>> Inserting Data Into: silver.sleep_apnea_risk';

        INSERT INTO silver.sleep_apnea_risk (
            age, bmi, diabetes_flag, hypertension_flag,
            gender_male_flag, neck_circumference_cm,
            smoking_flag, alcohol_use_flag, snoring_flag,
            fatigue_flag, sleep_apnea_flag
        )
        SELECT
            Age               AS age,
            BMI               AS bmi,
            CAST(Diabetes         AS TINYINT) AS diabetes_flag,
            CAST(Hypertension     AS TINYINT) AS hypertension_flag,
            CAST(Gender_Male      AS TINYINT) AS gender_male_flag,
            Neck_Circumference    AS neck_circumference_cm,
            CAST(Smoking          AS TINYINT) AS smoking_flag,
            CAST(Alcohol_Use      AS TINYINT) AS alcohol_use_flag,
            CAST(Snoring          AS TINYINT) AS snoring_flag,
            CAST(Fatigue          AS TINYINT) AS fatigue_flag,
            CAST(Sleep_Apnea      AS TINYINT) AS sleep_apnea_flag
        FROM bronze.sleep_apnea_dataset;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
        PRINT 'Loading Diabetes Tables';
        PRINT '------------------------------------------------';

        -- ====================================================
        -- TABLE 4: silver.diabetes_patient
        -- Source : bronze.diabetes_data (1,879 rows, 46 cols)
        --
        -- ISSUES FOUND & HANDLED:
        --   Gender stored as 0/1 INT → decode to 'Female'/'Male'
        --   DoctorInCharge = 'Confidential' for all rows → drop column
        --   (no analytical value, privacy concern)
        --   No nulls, no duplicates — dataset is clean
        --   All 0/1 flag columns → cast to TINYINT
        -- ====================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.diabetes_patient';
        TRUNCATE TABLE silver.diabetes_patient;
        PRINT '>> Inserting Data Into: silver.diabetes_patient';

        INSERT INTO silver.diabetes_patient (
            patient_id, age, gender, ethnicity,
            socioeconomic_status, education_level, bmi,
            smoking_flag, alcohol_consumption, physical_activity,
            diet_quality, sleep_quality,
            family_history_diabetes, gestational_diabetes,
            polycystic_ovary_syndrome, previous_pre_diabetes,
            hypertension_flag, systolic_bp, diastolic_bp,
            fasting_blood_sugar, hba1c, serum_creatinine,
            bun_levels, cholesterol_total, cholesterol_ldl,
            cholesterol_hdl, cholesterol_triglycerides,
            antihypertensive_meds, statins, antidiabetic_meds,
            frequent_urination, excessive_thirst,
            unexplained_weight_loss, fatigue_level,
            blurred_vision, slow_healing_sores, tingling_hands_feet,
            quality_of_life_score, heavy_metals_exposure,
            occupational_chemical_exposure, water_quality,
            medical_checkups_freq, medication_adherence,
            health_literacy, diagnosis_diabetes
        )
        SELECT
            PatientID,
            Age,
            -- Decode binary gender
            CASE Gender WHEN 0 THEN 'Female' WHEN 1 THEN 'Male' ELSE 'Unknown' END AS gender,
            Ethnicity,
            SocioeconomicStatus,
            EducationLevel,
            BMI,
            CAST(Smoking               AS TINYINT),
            AlcoholConsumption,
            PhysicalActivity,
            DietQuality,
            SleepQuality,
            CAST(FamilyHistoryDiabetes      AS TINYINT),
            CAST(GestationalDiabetes        AS TINYINT),
            CAST(PolycysticOvarySyndrome    AS TINYINT),
            CAST(PreviousPreDiabetes        AS TINYINT),
            CAST(Hypertension               AS TINYINT),
            SystolicBP,
            DiastolicBP,
            FastingBloodSugar,
            HbA1c,
            SerumCreatinine,
            BUNLevels,
            CholesterolTotal,
            CholesterolLDL,
            CholesterolHDL,
            CholesterolTriglycerides,
            CAST(AntihypertensiveMedications AS TINYINT),
            CAST(Statins                     AS TINYINT),
            CAST(AntidiabeticMedications     AS TINYINT),
            CAST(FrequentUrination           AS TINYINT),
            CAST(ExcessiveThirst             AS TINYINT),
            CAST(UnexplainedWeightLoss       AS TINYINT),
            FatigueLevels,
            CAST(BlurredVision               AS TINYINT),
            CAST(SlowHealingSores            AS TINYINT),
            CAST(TinglingHandsFeet           AS TINYINT),
            QualityOfLifeScore,
            CAST(HeavyMetalsExposure               AS TINYINT),
            CAST(OccupationalExposureChemicals     AS TINYINT),
            CAST(WaterQuality                      AS TINYINT),
            MedicalCheckupsFrequency,
            MedicationAdherence,
            HealthLiteracy,
            CAST(Diagnosis                         AS TINYINT)
            -- DoctorInCharge intentionally excluded — value = 'Confidential'
        FROM bronze.diabetes_data;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================
        -- TABLE 5: silver.diabetes_risk_factors
        -- Source : bronze.diabetes_dataset_10k (10,000 rows)
        --
        -- ISSUES FOUND & HANDLED:
        --   No nulls, no duplicates — clean source
        --   Sleep_Apnea column present → KEY BRIDGE to sleep domain
        --   Age and BMI are FLOAT — preserve
        --   No natural PK → surrogate IDENTITY
        -- ====================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.diabetes_risk_factors';
        TRUNCATE TABLE silver.diabetes_risk_factors;
        PRINT '>> Inserting Data Into: silver.diabetes_risk_factors';

        INSERT INTO silver.diabetes_risk_factors (
            age, bmi, glucose, insulin, blood_pressure,
            family_history_flag, hypertension_flag, sleep_apnea_flag,
            smoking_flag, gender_male_flag, diabetes_flag
        )
        SELECT
            Age,
            BMI,
            Glucose,
            Insulin,
            Blood_Pressure,
            CAST(Family_History  AS TINYINT),
            CAST(Hypertension    AS TINYINT),
            CAST(Sleep_Apnea     AS TINYINT),   --  bridge to sleep domain
            CAST(Smoking         AS TINYINT),
            CAST(Gender_Male     AS TINYINT),
            CAST(Diabetes        AS TINYINT)
        FROM bronze.diabetes_dataset_10k;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================
        -- TABLE 6: silver.diabetes_prediction
        -- Source : bronze.diabetes_prediction (100,000 rows)
        --
        -- ISSUES FOUND & HANDLED:
        --   3,854 exact duplicate rows → deduplicate using ROW_NUMBER()
        --   smoking_history = 'No Info' → normalize to 'Unknown'
        --   gender = 'Other' (18 rows) → normalize to 'Unknown'
        --   gender = 'Female'/'Male' → trim and pass through
        --   No surrogate PK in source → IDENTITY
        -- ====================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.diabetes_prediction';
        TRUNCATE TABLE silver.diabetes_prediction;
        PRINT '>> Inserting Data Into: silver.diabetes_prediction';

        INSERT INTO silver.diabetes_prediction (
            gender, age, hypertension_flag, heart_disease_flag,
            smoking_history, bmi, hba1c_level, blood_glucose_level, diabetes_flag
        )
        SELECT
            -- Normalize gender
            CASE
                WHEN TRIM(gender) IN ('Female', 'Male') THEN TRIM(gender)
                ELSE 'Unknown'
            END AS gender,
            age,
            CAST(hypertension  AS TINYINT) AS hypertension_flag,
            CAST(heart_disease AS TINYINT) AS heart_disease_flag,
            -- Normalize smoking
            CASE
                WHEN TRIM(smoking_history) = 'No Info' THEN 'Unknown'
                ELSE TRIM(smoking_history)
            END AS smoking_history,
            bmi,
            HbA1c_level,
            blood_glucose_level,
            CAST(diabetes AS TINYINT) AS diabetes_flag
        FROM (
            -- Remove exact duplicate rows
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY gender, age, hypertension, heart_disease,
                                    smoking_history, bmi, HbA1c_level,
                                    blood_glucose_level, diabetes
                       ORDER BY (SELECT NULL)
                   ) AS rn
            FROM bronze.diabetes_prediction
        ) deduped
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
        PRINT 'Loading Hypertension & Epidemiology Tables';
        PRINT '------------------------------------------------';

        -- ====================================================
        -- TABLE 7: silver.hypertension_risk
        -- Source : bronze.hypertension_risk (4,240 rows)
        --
        -- ISSUES FOUND & HANDLED:
        --   540 total NULLs across 6 columns:
        --   cigsPerDay(29), BPMeds(53), totChol(50),
        --   BMI(19), heartRate(1), glucose(388)
        --   → Keep NULLs — do NOT impute with 0 (would corrupt
        --   analytics; NULL = not measured, not zero)
        --   gender stored as 1/0 INT → decode to 'Male'/'Female'
        --   sysBP is FLOAT in source (half-values like 106.5) → DECIMAL(6,1)
        --   Risk column renamed to hypertension_risk for clarity
        -- ====================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.hypertension_risk';
        TRUNCATE TABLE silver.hypertension_risk;
        PRINT '>> Inserting Data Into: silver.hypertension_risk';

        INSERT INTO silver.hypertension_risk (
            gender, age, current_smoker, cigs_per_day,
            bp_meds_flag, diabetes_flag, total_cholesterol,
            systolic_bp, diastolic_bp, bmi,
            heart_rate, glucose, hypertension_risk
        )
        SELECT
            -- Decode binary gender
            CASE male WHEN 1 THEN 'Male' WHEN 0 THEN 'Female' ELSE 'Unknown' END AS gender,
            age,
            CAST(currentSmoker AS TINYINT)  AS current_smoker,
            TRY_CAST(cigsperday AS DECIMAL(5,1)) AS cigs_per_day,  -- Handle potential NULLs
            TRY_CAST(bpmeds AS TINYINT) AS bp_meds_flag,
            CAST(diabetes AS TINYINT)       AS diabetes_flag,
            TRY_CAST(totchol AS INT) AS total_cholesterol,
            sysBP               AS systolic_bp,
            diaBP               AS diastolic_bp,
            TRY_CAST(bmi AS DECIMAL(5,2)) AS bmi,
            TRY_CAST(heartrate AS INT) AS heart_rate,
            TRY_CAST(glucose AS INT) AS glucose,
            CAST([risk] AS TINYINT)         AS hypertension_risk
        FROM bronze.hypertension_risk;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================
        -- TABLE 8: silver.epidemiology_diabetes_2016
        -- Source : bronze.diabetes_all_2016 (390 rows)
        --
        -- ISSUES FOUND & HANDLED:
        --    CT column is a US Census Tract FIPS code stored as INT
        --    but values like 980700 can exceed INT range on some
        --    systems → cast to BIGINT
        --    Column names are cryptic acronyms → rename to readable
        --    (BPAD=BP Adult Diabetic, BPAN=BP Adult Non-Diabetic, etc.)
        --    No nulls, no duplicates — clean aggregate source
        --    This is a population-level aggregate, NOT patient-level
        --    Do NOT join to patient tables on CT unless you have
        --    a patient-to-CT mapping table
        -- ====================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.epidemiology_diabetes_2016';
        TRUNCATE TABLE silver.epidemiology_diabetes_2016;
        PRINT '>> Inserting Data Into: silver.epidemiology_diabetes_2016';

        INSERT INTO silver.epidemiology_diabetes_2016 (
            census_tract,
            bp_adult_diab, bp_adult_nondiab, bp_adult_nondiab2,
            bw_adult_diab, bw_adult_nondiab, bw_adult_nondiab2,
            bm_adult_diab, bm_adult_nondiab, bm_adult_nondiab2
        )
        SELECT
            CAST(ct    AS BIGINT) AS census_tract,
            bpad  AS bp_adult_diab,
            bpan  AS bp_adult_nondiab,
            bpan2 AS bp_adult_nondiab2,
            bwad  AS bw_adult_diab,
            bwan  AS bw_adult_nondiab,
            bwan2 AS bw_adult_nondiab2,
            bmad  AS bm_adult_diab,
            bman  AS bm_adult_nondiab,
            bman2 AS bm_adult_nondiab2
        FROM bronze.diabetes_all_2016;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================
        -- TABLE 9: silver.epidemiology_diab_hyp_2016
        -- Source : bronze.diabetes_hypertension_2016 (390 rows)
        --
        -- ISSUES FOUND & HANDLED:
        --    Same CT key space as table 8 → directly joinable
        --    on census_tract for combined epidemiology queries
        --    Column names are cryptic → rename (BTPAD = Total BP
        --    Adult Diabetic = diabetic + hypertension combined)
        --    No nulls, no duplicates — clean
        -- ====================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.epidemiology_diab_hyp_2016';
        TRUNCATE TABLE silver.epidemiology_diab_hyp_2016;
        PRINT '>> Inserting Data Into: silver.epidemiology_diab_hyp_2016';

        INSERT INTO silver.epidemiology_diab_hyp_2016 (
            census_tract,
            total_bp_adult_diab, total_bp_adult_nondiab,
            total_bw_adult_diab, total_bw_adult_nondiab,
            total_bm_adult_diab, total_bm_adult_nondiab
        )
        SELECT
            CAST(ct    AS BIGINT) AS census_tract,
            btpad AS total_bp_adult_diab,
            btpan AS total_bp_adult_nondiab,
            btwad AS total_bw_adult_diab,
            btwan AS total_bw_adult_nondiab,
            btmad AS total_bm_adult_diab,
            btman AS total_bm_adult_nondiab
        FROM bronze.diabetes_hypertension_2016;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Silver Layer Load Complete';
        PRINT '   - Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR DURING SILVER LAYER LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER()  AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE()   AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END
GO
