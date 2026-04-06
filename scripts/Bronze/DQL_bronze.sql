\/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    BEGIN TRY
        PRINT '================================================';
        PRINT 'Starting Bronze Layer Load...';
        PRINT '================================================';

        -----------------------------------------------------------------------
        -- 1. diabetes_data
        -----------------------------------------------------------------------
        PRINT '>> Truncating and Loading: bronze.diabetes_data';
        TRUNCATE TABLE bronze.diabetes_data;
        BULK INSERT bronze.diabetes_data
        FROM 'D:\Datawarehouse\source\diabetes_data.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        -----------------------------------------------------------------------
        -- 2. diabetes_dataset_10k
        -----------------------------------------------------------------------
        PRINT '>> Truncating and Loading: bronze.diabetes_dataset_10k';
        TRUNCATE TABLE bronze.diabetes_dataset_10k;
        BULK INSERT bronze.diabetes_dataset_10k
        FROM 'D:\Datawarehouse\source\diabetes_dataset_10k.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        -----------------------------------------------------------------------
        -- 3. diabetes_prediction
        -----------------------------------------------------------------------
        PRINT '>> Truncating and Loading: bronze.diabetes_prediction';
        TRUNCATE TABLE bronze.diabetes_prediction;
        BULK INSERT bronze.diabetes_prediction
        FROM 'D:\Datawarehouse\source\diabetes_prediction_dataset11.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        -----------------------------------------------------------------------
        -- 4. enhanced_sleep_sdb
        -----------------------------------------------------------------------
        PRINT '>> Truncating and Loading: bronze.enhanced_sleep_sdb';
        TRUNCATE TABLE bronze.enhanced_sleep_sdb;
        BULK INSERT bronze.enhanced_sleep_sdb
        FROM 'D:\Datawarehouse\source\enhanced_sleep_disordered_breathing_dataset.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            ROWTERMINATOR = '\n',
            CODEPAGE = '65001', 
            TABLOCK
        );

        -----------------------------------------------------------------------
        -- 5. sleep_apnea_dataset
        -----------------------------------------------------------------------
        PRINT '>> Truncating and Loading: bronze.sleep_apnea_dataset';
        TRUNCATE TABLE bronze.sleep_apnea_dataset;
        BULK INSERT bronze.sleep_apnea_dataset
        FROM 'D:\Datawarehouse\source\sleep_apnea_dataset.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            ROWTERMINATOR = '\n',
            CODEPAGE = '65001',
            TABLOCK
        );

        -----------------------------------------------------------------------
        -- 6. diabetes_all_2016
        -----------------------------------------------------------------------
        PRINT '>> Truncating and Loading: bronze.diabetes_all_2016';
        TRUNCATE TABLE bronze.diabetes_all_2016;
        BULK INSERT bronze.diabetes_all_2016
        FROM 'D:\Datawarehouse\source\diabetes_all_2016.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            ROWTERMINATOR = '\n',
            CODEPAGE = '65001',
            TABLOCK
        );

        -----------------------------------------------------------------------
        -- 7. diabetes_hypertension_2016
        -----------------------------------------------------------------------
        PRINT '>> Truncating and Loading: bronze.diabetes_hypertension_2016';
        TRUNCATE TABLE bronze.diabetes_hypertension_2016;
        BULK INSERT bronze.diabetes_hypertension_2016
        FROM 'D:\Datawarehouse\source\diabetes_hypertension_all_2016.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            ROWTERMINATOR = '\n',
            CODEPAGE = '65001',
            TABLOCK
        );

        -----------------------------------------------------------------------
        -- 8. hypertension_risk
        -----------------------------------------------------------------------
        PRINT '>> Truncating and Loading: bronze.hypertension_risk';
        TRUNCATE TABLE bronze.hypertension_risk;
        BULK INSERT bronze.hypertension_risk
        FROM 'D:\Datawarehouse\source\Hypertension_risk_model_main.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            ROWTERMINATOR = '\n',
            CODEPAGE = '65001',
            KEEPNULLS,
            TABLOCK
        );

        -----------------------------------------------------------------------
        -- 9. sleep_data_sampled
        -----------------------------------------------------------------------
        PRINT '>> Truncating and Loading: bronze.sleep_data_sampled';
        TRUNCATE TABLE bronze.sleep_data_sampled;
        BULK INSERT bronze.sleep_data_sampled
        FROM 'D:\Datawarehouse\source\Sleep_Data_Sampled.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            ROWTERMINATOR = '\n',
            CODEPAGE = '65001',
            KEEPNULLS,
            TABLOCK
        );

        -----------------------------------------------------------------------
        -- 10. sleep_health_lifestyle
        -----------------------------------------------------------------------
        PRINT '>> Truncating and Loading: bronze.sleep_health_lifestyle';
        TRUNCATE TABLE bronze.sleep_health_lifestyle;
        BULK INSERT bronze.sleep_health_lifestyle
        FROM 'D:\Datawarehouse\source\Sleep_health_and_lifestyle_dataset.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            ROWTERMINATOR = '\n',
            CODEPAGE = '65001',
            KEEPNULLS,
            TABLOCK
        );

        PRINT '================================================';
        PRINT 'Bronze Layer Load Completed Successfully!';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        -- This block is mandatory if BEGIN TRY is used
        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
        PRINT 'ERROR OCCURRED DURING LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
    END CATCH
END
GO
