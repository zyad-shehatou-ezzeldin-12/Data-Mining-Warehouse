/*
===============================================================================
DDL Script: Create Silver Tables — Sleep Apnea Data Warehouse
===============================================================================
Script Purpose:
    Creates all Bronze layer tables for the Sleep Apnea DW domain,Tables are built from 10
    source datasets that cover:
        - Sleep health & lifestyle (patient-level)
        - Sleep apnea clinical detail (PSG/sensor signals)
        - Diabetes comorbidity
        - Hypertension risk
        - Population-level aggregates (2016 epidemiology)

Data Sources (datasets):
    sleep_health_lifestyle        
    sleep_data_sampled            
    enhanced_sleep_sdb            
    sleep_apnea_dataset           
    diabetes_data                 
    diabetes_dataset_10k          
    diabetes_prediction           
    hypertension_risk             
    diabetes_all_2016             
    diabetes_hypertension_2016    
===============================================================================
*/
IF OBJECT_ID('bronze.diabetes_data', 'U') IS NOT NULL
    DROP TABLE bronze.diabetes_data;
GO

create table bronze.diabetes_data(
PatientID int,
Age int, 
Gender nvarchar(50),
Ethnicity int, 
SocioeconomicStatus int,
EducationLevel int, 
BMI float,
Smoking int,
AlcoholConsumption float,
PhysicalActivity float,
DietQuality float, 
SleepQuality float,
FamilyHistoryDiabetes int,
GestationalDiabetes int,
PolycysticOvarySyndrome int,
PreviousPreDiabetes int,
Hypertension int,
SystolicBP int,
DiastolicBP int,
FastingBloodSugar float,
HbA1c float,
SerumCreatinine float,
BUNLevels float,
CholesterolTotal float,
CholesterolLDL float, 
CholesterolHDL float,
CholesterolTriglycerides float,
AntihypertensiveMedications int,
Statins int,
AntidiabeticMedications int,
FrequentUrination int,
ExcessiveThirst int,
UnexplainedWeightLoss int,
FatigueLevels float,
BlurredVision int,
SlowHealingSores int,
TinglingHandsFeet int,
QualityOfLifeScore float,
HeavyMetalsExposure int,
OccupationalExposureChemicals int,
WaterQuality int,
MedicalCheckupsFrequency float,
MedicationAdherence float,
HealthLiteracy float,
Diagnosis int,
DoctorInCharge nvarchar(50),
);

GO



IF OBJECT_ID('bronze.diabetes_dataset_10k', 'U') IS NOT NULL
    DROP TABLE bronze.diabetes_dataset_10k;
GO
create table bronze.diabetes_dataset_10k(
    PatientID int,
    Age FLOAT,
    BMI FLOAT,
    Glucose FLOAT,
    Insulin FLOAT,
    Blood_Pressure FLOAT,
    Family_History INT,
    Hypertension INT,
    Sleep_Apnea INT,
    Smoking INT,
    Gender_Male INT,
    Diabetes INT,

);

GO


IF OBJECT_ID('bronze.diabetes_prediction', 'U') IS NOT NULL
    DROP TABLE bronze.diabetes_prediction;
GO
create table bronze.diabetes_prediction(
    PatientID int,
    gender NVARCHAR(50),
    age FLOAT,
    hypertension INT,
    heart_disease INT,
    smoking_history NVARCHAR(50),
    bmi FLOAT,
    HbA1c_level FLOAT,
    blood_glucose_level INT,
    diabetes INT,

);
GO


IF OBJECT_ID('bronze.enhanced_sleep_sdb', 'U') IS NOT NULL
    DROP TABLE bronze.enhanced_sleep_sdb;
GO

create table bronze.enhanced_sleep_sdb(
PatientID int,
Age int,
Gender nvarchar(50),
BMI float,
Snoring nvarchar(50),
Oxygen_Saturation float,
Apnea_Hypopnea_Index_AHI int,
Nasal_Airflow_Flow_Rate float,
Chest_Movement_Effort float,
ECG_Heart_Rate int,
SpO2_Ratio float,
Position nvarchar(50),
EEG_Sleep_Stage nvarchar(50),
Diagnosis_of_SDB nvarchar(50),
Severity nvarchar(50),
Treatment nvarchar(50),
Physician_Notes nvarchar(max),
Patient_Symptoms nvarchar(max),
);
GO


IF OBJECT_ID('bronze.sleep_apnea_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.sleep_apnea_dataset;
GO

create table bronze.sleep_apnea_dataset(
PatientID int,
Age float,
BMI float,
Diabetes int,
Hypertension int,
Gender_Male int,
Neck_Circumference float,
Smoking int,
Alcohol_Use int,
Snoring int,
Fatigue int,
Sleep_Apnea int,


);
GO


IF OBJECT_ID('bronze.sleep_health_lifestyle', 'U') IS NOT NULL
    DROP TABLE bronze.sleep_health_lifestyle;
GO

create table bronze.sleep_health_lifestyle(
    PatientID int,
    gender NVARCHAR(50),
    age INT,
    occupation NVARCHAR(50),
    sleep_duration FLOAT,
    quality_of_sleep INT,
    physical_activity_level INT,
    stress_level INT,
    bmi_category NVARCHAR(50),
    blood_pressure NVARCHAR(50),
    heart_rate INT,
    daily_steps INT,
    sleep_disorder NVARCHAR(50),

);
GO

IF OBJECT_ID('bronze.diabetes_all_2016', 'U') IS NOT NULL
    DROP TABLE bronze.diabetes_all_2016;
GO
create table bronze.diabetes_all_2016(
    PatientID int,
    ct INT,
    bpad INT,
    bpan INT,
    bpan2 INT,
    bwad INT,
    bwan INT,
    bwan2 INT,
    bmad INT,
    bman INT,
    bman2 INT,

);
GO

IF OBJECT_ID('bronze.diabetes_hypertension_2016', 'U') IS NOT NULL
    DROP TABLE bronze.diabetes_hypertension_2016;
GO

create table bronze.diabetes_hypertension_2016(
    PatientID int,
    ct INT,
    btpad INT,
    btpan INT,
    btwad INT,
    btwan INT,
    btmad INT,
    btman INT,
);
GO


IF OBJECT_ID('bronze.hypertension_risk', 'U') IS NOT NULL
    DROP TABLE bronze.hypertension_risk;
GO
create table bronze.hypertension_risk(
    PatientID int,
    male INT,
    age INT,
    currentsmoker INT,
    cigsperday NVARCHAR(50),
    bpmeds NVARCHAR(50),
    diabetes INT,
    totchol NVARCHAR(50) ,
    sysbp FLOAT,
    diabp FLOAT,
    bmi NVARCHAR(50),
    heartrate NVARCHAR(50),
    glucose NVARCHAR(50),
    risk INT,

);
GO

IF OBJECT_ID('bronze.sleep_data_sampled', 'U') IS NOT NULL
    DROP TABLE bronze.sleep_data_sampled;
GO
create table bronze.sleep_data_sampled(
    PatientID int,
    gender NVARCHAR(50),
    age INT,
    occupation NVARCHAR(50),
    sleep_duration FLOAT,
    quality_of_sleep INT,
    physical_activity_level INT,
    stress_level INT,
    bmi_category NVARCHAR(50),
    blood_pressure NVARCHAR(50),
    heart_rate INT,
    daily_steps INT,
    sleep_disorder NVARCHAR(50),


);
GO
