/*
===============================================================================
Stored Procedure: Load Gold Layer — Sleep Apnea Data Warehouse
===============================================================================
Script Purpose:
    ETL from Silver → Gold for all dimensions, the central fact table,
    and all 6 analytical marts.

Usage : EXEC gold.load_gold;
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start DATETIME, @batch_end DATETIME;

    BEGIN TRY
        SET @batch_start = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Gold Layer — Sleep Apnea DW';
        PRINT '================================================';

        -- ────────────────────────────────────────────────────
        -- STEP 1: dim_age_band (static seed)
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 1: Loading gold.dim_age_band';
        DELETE FROM gold.dim_age_band;
        INSERT INTO gold.dim_age_band (age_band_id, label, age_min, age_max) VALUES
            (1, 'Under 18',  0, 17),
            (2, '18-30',    18, 30),
            (3, '31-45',    31, 45),
            (4, '46-60',    46, 60),
            (5, '61+',      61, 99);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's | Rows: 5';
        PRINT '>> -------------';

        -- ────────────────────────────────────────────────────
        -- STEP 2: dim_bmi_category (WHO standard)
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 2: Loading gold.dim_bmi_category';
        DELETE FROM gold.dim_bmi_category;
        INSERT INTO gold.dim_bmi_category (bmi_cat_id, label, bmi_min, bmi_max) VALUES
            (1, 'Normal',     0,    24.9),
            (2, 'Overweight', 25,   29.9),
            (3, 'Obese',      30,   99.0),
            (4, 'Unknown',    NULL, NULL);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's | Rows: 4';
        PRINT '>> -------------';

        -- ────────────────────────────────────────────────────
        -- STEP 3: dim_disorder_type
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 3: Loading gold.dim_disorder_type';
        TRUNCATE TABLE gold.dim_disorder_type;
        INSERT INTO gold.dim_disorder_type (disorder_id, label, is_sleep_disorder) VALUES
            (1, 'Healthy',     0),
            (2, 'Sleep Apnea', 1),
            (3, 'Insomnia',    1),
            (4, 'Unknown',     0);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's | Rows: 4';
        PRINT '>> -------------';

        -- ────────────────────────────────────────────────────
        -- STEP 4: dim_gender
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 4: Loading gold.dim_gender';
        TRUNCATE TABLE gold.dim_gender;
        INSERT INTO gold.dim_gender (gender_id, label) VALUES
            (1, 'Male'), (2, 'Female'), (3, 'Unknown');
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's | Rows: 3';
        PRINT '>> -------------';

        -- ────────────────────────────────────────────────────
        -- STEP 5: dim_patient
        -- Source: silver.sleep_patient_profile
        -- Derive age_band inline using CASE
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 5: Loading gold.dim_patient';
        TRUNCATE TABLE gold.dim_patient;
        INSERT INTO gold.dim_patient (
            person_id, gender, age, age_band,
            occupation, bmi_category, sleep_disorder
        )
        SELECT
            person_id,
            gender,
            age,
            CASE
                WHEN age < 18  THEN 'Under 18'
                WHEN age <= 30 THEN '18-30'
                WHEN age <= 45 THEN '31-45'
                WHEN age <= 60 THEN '46-60'
                ELSE '61+'
            END AS age_band,
            occupation,
            CASE
                WHEN TRIM(bmi_category) IN ('Normal','Normal Weight') THEN 'Normal'
                WHEN TRIM(bmi_category) = 'Overweight' THEN 'Overweight'
                WHEN TRIM(bmi_category) = 'Obese'      THEN 'Obese'
                ELSE 'Unknown'
            END AS bmi_category,
            ISNULL(NULLIF(TRIM(sleep_disorder),''), 'Unknown') AS sleep_disorder
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY dwh_create_date DESC) AS rn
            FROM silver.sleep_patient_profile
        ) t
        WHERE rn = 1;
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        PRINT '>> -------------';

        -- ────────────────────────────────────────────────────
        -- STEP 6: fact_patient_health_snapshot
        -- Source: silver.diabetes_risk_factors
        -- This is the only Silver table with ALL 3 comorbidity
        -- flags (sleep_apnea + diabetes + hypertension) in one row.
        -- Join age_band_id and bmi_cat_id from dimensions.
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 6: Loading gold.fact_patient_health_snapshot';
        TRUNCATE TABLE gold.fact_patient_health_snapshot;
        INSERT INTO gold.fact_patient_health_snapshot (
            record_id, age_band_id, bmi_cat_id,
            age, bmi, glucose, insulin, blood_pressure,
            family_history_flag, hypertension_flag, sleep_apnea_flag,
            smoking_flag, gender_male_flag, diabetes_flag,
            age_band, bmi_category
        )
        SELECT
            r.record_id,
            ab.age_band_id,
            bc.bmi_cat_id,
            r.age,
            r.bmi,
            r.glucose,
            r.insulin,
            r.blood_pressure,
            r.family_history_flag,
            r.hypertension_flag,
            r.sleep_apnea_flag,
            r.smoking_flag,
            r.gender_male_flag,
            r.diabetes_flag,
            ab.label   AS age_band,
            bc.label   AS bmi_category
        FROM silver.diabetes_risk_factors r
        LEFT JOIN gold.dim_age_band ab
            ON r.age >= ab.age_min AND r.age <= ab.age_max
        LEFT JOIN gold.dim_bmi_category bc
            ON r.bmi >= bc.bmi_min AND r.bmi <= bc.bmi_max AND bc.label != 'Unknown';
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        PRINT '>> -------------';

        PRINT '';
        PRINT '--- Loading Analytical Marts ---';
        PRINT '------------------------------------------------';

        -- ────────────────────────────────────────────────────
        -- STEP 7: mart_comorbidity_by_age
        -- Pre-aggregate fact table by age band
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 7: Loading gold.mart_comorbidity_by_age';
        TRUNCATE TABLE gold.mart_comorbidity_by_age;
        INSERT INTO gold.mart_comorbidity_by_age (
            age_band, total_patients, diabetes_count, sleep_apnea_count,
            hypertension_count, all_three_count,
            avg_bmi, avg_glucose, avg_blood_pressure,
            diabetes_rate_pct, sleep_apnea_rate_pct,
            hypertension_rate_pct, all_three_rate_pct
        )
        SELECT
            age_band,
            COUNT(*)                                                AS total_patients,
            SUM(diabetes_flag)                                      AS diabetes_count,
            SUM(sleep_apnea_flag)                                   AS sleep_apnea_count,
            SUM(hypertension_flag)                                  AS hypertension_count,
            SUM(CASE WHEN diabetes_flag=1 AND sleep_apnea_flag=1
                     AND hypertension_flag=1 THEN 1 ELSE 0 END)    AS all_three_count,
            ROUND(AVG(CAST(bmi AS FLOAT)), 2)                       AS avg_bmi,
            ROUND(AVG(CAST(glucose AS FLOAT)), 2)                   AS avg_glucose,
            ROUND(AVG(CAST(blood_pressure AS FLOAT)), 2)            AS avg_blood_pressure,
            ROUND(100.0*SUM(diabetes_flag)/COUNT(*), 1)             AS diabetes_rate_pct,
            ROUND(100.0*SUM(sleep_apnea_flag)/COUNT(*), 1)          AS sleep_apnea_rate_pct,
            ROUND(100.0*SUM(hypertension_flag)/COUNT(*), 1)         AS hypertension_rate_pct,
            ROUND(100.0*SUM(CASE WHEN diabetes_flag=1 AND sleep_apnea_flag=1
                                 AND hypertension_flag=1 THEN 1 ELSE 0 END)/COUNT(*), 1) AS all_three_rate_pct
        FROM gold.fact_patient_health_snapshot
        WHERE age_band IS NOT NULL
        GROUP BY age_band
        ORDER BY MIN(age);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        PRINT '>> -------------';

        -- ────────────────────────────────────────────────────
        -- STEP 8: mart_bmi_sleep_disorder
        -- Source: silver.sleep_patient_profile (has BMI + disorder)
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 8: Loading gold.mart_bmi_sleep_disorder';
        TRUNCATE TABLE gold.mart_bmi_sleep_disorder;
        INSERT INTO gold.mart_bmi_sleep_disorder (
            bmi_category, sleep_disorder, patient_count,
            avg_sleep_hrs, avg_stress, avg_systolic, avg_heart_rate
        )
        SELECT
            CASE
                WHEN bmi_category IN ('Normal','Normal Weight') THEN 'Normal'
                WHEN bmi_category = 'Overweight' THEN 'Overweight'
                WHEN bmi_category = 'Obese'      THEN 'Obese'
                ELSE 'Unknown'
            END AS bmi_category,
            ISNULL(NULLIF(TRIM(sleep_disorder),''),'Unknown') AS sleep_disorder,
            COUNT(*)                                            AS patient_count,
            ROUND(AVG(CAST(sleep_duration_hrs AS FLOAT)), 2)   AS avg_sleep_hrs,
            ROUND(AVG(CAST(stress_level AS FLOAT)), 2)         AS avg_stress,
            ROUND(AVG(CAST(systolic_bp AS FLOAT)), 2)          AS avg_systolic,
            ROUND(AVG(CAST(heart_rate AS FLOAT)), 2)           AS avg_heart_rate
        FROM silver.sleep_patient_profile
        WHERE sleep_disorder != 'Unknown' OR sleep_disorder IS NULL
        GROUP BY
            CASE
                WHEN bmi_category IN ('Normal','Normal Weight') THEN 'Normal'
                WHEN bmi_category = 'Overweight' THEN 'Overweight'
                WHEN bmi_category = 'Obese'      THEN 'Obese'
                ELSE 'Unknown'
            END,
            ISNULL(NULLIF(TRIM(sleep_disorder),''),'Unknown');
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        PRINT '>> -------------';

        -- ────────────────────────────────────────────────────
        -- STEP 9: mart_occupation_risk
        -- Source: silver.sleep_patient_profile
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 9: Loading gold.mart_occupation_risk';
        TRUNCATE TABLE gold.mart_occupation_risk;
        INSERT INTO gold.mart_occupation_risk (
            occupation, total_patients, sleep_apnea_count, insomnia_count,
            avg_sleep_hrs, avg_stress, avg_systolic, avg_daily_steps,
            sleep_apnea_rate_pct, insomnia_rate_pct
        )
        SELECT
            occupation,
            COUNT(*)                                                            AS total_patients,
            SUM(CASE WHEN sleep_disorder = 'Sleep Apnea' THEN 1 ELSE 0 END)    AS sleep_apnea_count,
            SUM(CASE WHEN sleep_disorder = 'Insomnia'    THEN 1 ELSE 0 END)    AS insomnia_count,
            ROUND(AVG(CAST(sleep_duration_hrs AS FLOAT)), 2)                   AS avg_sleep_hrs,
            ROUND(AVG(CAST(stress_level AS FLOAT)), 2)                         AS avg_stress,
            ROUND(AVG(CAST(systolic_bp AS FLOAT)), 2)                          AS avg_systolic,
            ROUND(AVG(CAST(daily_steps AS FLOAT)), 2)                          AS avg_daily_steps,
            ROUND(100.0*SUM(CASE WHEN sleep_disorder='Sleep Apnea' THEN 1 ELSE 0 END)/COUNT(*),1) AS sleep_apnea_rate_pct,
            ROUND(100.0*SUM(CASE WHEN sleep_disorder='Insomnia'    THEN 1 ELSE 0 END)/COUNT(*),1) AS insomnia_rate_pct
        FROM silver.sleep_patient_profile
        GROUP BY occupation;
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        PRINT '>> -------------';

        -- ────────────────────────────────────────────────────
        -- STEP 10: mart_clinical_severity
        -- Source: silver.sleep_apnea_clinical (PSG study data)
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 10: Loading gold.mart_clinical_severity';
        TRUNCATE TABLE gold.mart_clinical_severity;
        INSERT INTO gold.mart_clinical_severity (
            patient_id, age, gender, bmi, bmi_category,
            ahi_score, spo2_pct, severity, treatment, diagnosed_sdb
        )
        SELECT
            patient_id,
            age,
            gender,
            bmi,
            CASE
                WHEN bmi < 25 THEN 'Normal'
                WHEN bmi < 30 THEN 'Overweight'
                WHEN bmi >= 30 THEN 'Obese'
                ELSE 'Unknown'
            END AS bmi_category,
            ahi_score,
            spo2_pct,
            severity,
            treatment,
            diagnosis_sdb AS diagnosed_sdb
        FROM silver.sleep_apnea_clinical;
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        PRINT '>> -------------';

        -- ────────────────────────────────────────────────────
        -- STEP 11: mart_epidemiology_combined
        -- Source: JOIN silver.epidemiology_diabetes_2016
        --           + silver.epidemiology_diab_hyp_2016 on census_tract
        -- Adds diab_hyp_overlap_rate = what fraction of diabetics
        -- in each census tract also have hypertension
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 11: Loading gold.mart_epidemiology_combined';
        TRUNCATE TABLE gold.mart_epidemiology_combined;
        INSERT INTO gold.mart_epidemiology_combined (
            census_tract,
            bp_adult_diab, bp_adult_nondiab,
            bw_adult_diab, bw_adult_nondiab,
            bm_adult_diab, bm_adult_nondiab,
            total_bp_diab_hyp, total_bp_nondiab_hyp,
            total_bw_diab_hyp, total_bw_nondiab_hyp,
            total_bm_diab_hyp, total_bm_nondiab_hyp,
            diab_hyp_overlap_rate
        )
        SELECT
            d.census_tract,
            d.bp_adult_diab,
            d.bp_adult_nondiab,
            d.bw_adult_diab,
            d.bw_adult_nondiab,
            d.bm_adult_diab,
            d.bm_adult_nondiab,
            h.total_bp_adult_diab    AS total_bp_diab_hyp,
            h.total_bp_adult_nondiab AS total_bp_nondiab_hyp,
            h.total_bw_adult_diab    AS total_bw_diab_hyp,
            h.total_bw_adult_nondiab AS total_bw_nondiab_hyp,
            h.total_bm_adult_diab    AS total_bm_diab_hyp,
            h.total_bm_adult_nondiab AS total_bm_nondiab_hyp,
            CASE
                WHEN d.bp_adult_diab = 0 THEN NULL
                ELSE ROUND(CAST(h.total_bp_adult_diab AS FLOAT) / d.bp_adult_diab, 4)
            END AS diab_hyp_overlap_rate
        FROM silver.epidemiology_diabetes_2016 d
        INNER JOIN silver.epidemiology_diab_hyp_2016 h
            ON d.census_tract = h.census_tract;
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        PRINT '>> -------------';

        -- ────────────────────────────────────────────────────
        -- STEP 12: mart_diabetes_risk_profile
        -- Source: silver.diabetes_prediction (96k deduplicated rows)
        -- Segment by age × gender × BMI → diabetes rate
        -- ────────────────────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> STEP 12: Loading gold.mart_diabetes_risk_profile';
        TRUNCATE TABLE gold.mart_diabetes_risk_profile;
        INSERT INTO gold.mart_diabetes_risk_profile (
            age_band, gender, bmi_category, total,
            diabetes_count, hypertension_count,
            avg_hba1c, avg_glucose, diabetes_rate_pct
        )
        SELECT
            CASE
                WHEN age < 18  THEN 'Under 18'
                WHEN age <= 30 THEN '18-30'
                WHEN age <= 45 THEN '31-45'
                WHEN age <= 60 THEN '46-60'
                ELSE '61+'
            END AS age_band,
            gender,
            CASE
                WHEN bmi < 25  THEN 'Normal'
                WHEN bmi < 30  THEN 'Overweight'
                WHEN bmi >= 30 THEN 'Obese'
                ELSE 'Unknown'
            END AS bmi_category,
            COUNT(*)                                            AS total,
            SUM(diabetes_flag)                                  AS diabetes_count,
            SUM(hypertension_flag)                              AS hypertension_count,
            ROUND(AVG(CAST(hba1c_level AS FLOAT)), 6)          AS avg_hba1c,
            ROUND(AVG(CAST(blood_glucose_level AS FLOAT)), 6)  AS avg_glucose,
            ROUND(100.0*SUM(diabetes_flag)/COUNT(*), 1)        AS diabetes_rate_pct
        FROM silver.diabetes_prediction
        WHERE gender != 'Unknown'
        GROUP BY
            CASE
                WHEN age < 18  THEN 'Under 18'
                WHEN age <= 30 THEN '18-30'
                WHEN age <= 45 THEN '31-45'
                WHEN age <= 60 THEN '46-60'
                ELSE '61+'
            END,
            gender,
            CASE
                WHEN bmi < 25  THEN 'Normal'
                WHEN bmi < 30  THEN 'Overweight'
                WHEN bmi >= 30 THEN 'Obese'
                ELSE 'Unknown'
            END;
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        PRINT '>> -------------';

        SET @batch_end = GETDATE();
        PRINT '';
        PRINT '==========================================';
        PRINT 'Gold Layer Load Complete';
        PRINT '   Total Duration: ' + CAST(DATEDIFF(SECOND,@batch_start,@batch_end) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR: ' + ERROR_MESSAGE();
        PRINT 'Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State : ' + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END
GO
