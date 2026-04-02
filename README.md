# Data-Mining-Warehouse
Here is the complete, professional README.md file, formatted and ready for you to copy and paste directly into your GitHub repository or project folder.

Integrated Health Analytics Warehouse (IHAW)
Medical Data Warehouse Implementation using Medallion Architecture & SQL Server
📌 Project Overview
This project centralizes and processes 12+ heterogeneous clinical datasets—covering Diabetes, Hypertension, and Sleep Disorders—into a structured SQL Server environment. By utilizing the Medallion Architecture, the system transforms raw CSV ingestion into a high-performance relational database optimized for predictive healthcare analytics and population health monitoring.

🏗 Data Warehouse Architecture
The project is implemented within SQL Server Management Studio (SSMS) across three logical schemas to ensure data quality and lineage:

Bronze (Raw Ingestion): * Action: Bulk insertion of 130,000+ records from raw CSV files.

State: Immutable, "source of truth" staging tables.

Silver (Validated & Standardized): * Action: T-SQL scripts for data cleaning, handling missing values via median/mode imputation, and deduplication.

Standardization: Unified data types (e.g., DECIMAL(5,2) for BMI) and snake_case column normalization across all clinical domains.

Gold (Curated & Analytical): * Action: Complex JOIN operations and business logic application.

Output: Analytical views such as vw_RiskFactorCorrelation and vw_OccupationalHealthMetrics optimized for Power BI and ML pipelines.

📊 Clinical Domains Integrated
Metabolic Health: Glucose, HbA1c, and Insulin dynamics (100,000+ records).

Cardiovascular Risk: Blood pressure (Systolic/Diastolic), sodium intake, and cholesterol metrics.

Sleep & Lifestyle: Sleep Apnea severity (AHI), SpO2 levels, and stress scores correlated by Occupation.

🛠 Tech Stack
Database Engine: Microsoft SQL Server

Management Tool: SQL Server Management Studio (SSMS)

ETL/Scripting: T-SQL (Transact-SQL)

Data Prep: Python (Pandas) for initial format validation

Visualization: Power BI (Connected via SQL Server)

🚀 Implementation Workflow
1. Database Initialization
SQL
-- Creating the Warehouse and Medallion Schemas
CREATE DATABASE HealthWarehouse;
GO
USE HealthWarehouse;
GO
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO
2. The Silver Transformation
The Silver layer focuses on medical data integrity. T-SQL scripts normalize smoking statuses (e.g., "former", "never", "current") and verify that clinical metrics like Blood Pressure fall within biological ranges.

3. Gold Analytical Views
Gold views are designed for immediate insight. For example, the gold.vw_SleepDisorderAnalysis aggregates data to show the prevalence of Sleep Apnea within specific high-stress professions like Nursing and Engineering.

📈 Key Research Capabilities
Multi-Morbidity Analysis: Study the intersection of Sleep Apnea and Hypertension.

Occupational Risk Profiling: Analyze how job-related stress impacts cardiovascular health.

Predictive Modeling Ready: Features are pre-engineered for ingestion into XGBoost or Random Forest models.
