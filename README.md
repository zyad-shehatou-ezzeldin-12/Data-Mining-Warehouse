# Data-Mining-Warehouse
***
# 🏥 Integrated Health Analytics Warehouse (IHAW)
### **Medallion Architecture & SQL Server Implementation**

---

## **📌 Project Overview**
This project centralizes and processes **12+ heterogeneous clinical datasets**—covering **Diabetes, Hypertension, and Sleep Disorders**—into a structured SQL Server environment. By utilizing the **Medallion Architecture**, the system transforms raw CSV ingestion into a high-performance relational database optimized for predictive healthcare analytics and population health monitoring.

---

## **🏗 Data Warehouse Architecture**
The project is implemented within **SQL Server Management Studio (SSMS)** across three logical schemas to ensure data quality and lineage:

### **1. Bronze (Raw Ingestion Layer)**
* **Action:** Bulk insertion of **130,000+ records** from raw CSV files.
* **State:** Immutable, "source of truth" staging tables.
* **Storage:** Data is kept in its native format to allow for reprocessing if logic changes.

---

### **2. Silver (Validated & Standardized Layer)**
* **Action:** T-SQL scripts for data cleaning, handling missing values via median/mode imputation, and deduplication.
* **Standardization:** Unified data types (e.g., **DECIMAL(5,2)** for BMI) and **snake_case** column normalization across all clinical domains.
* **Integrity:** Includes constraints to ensure medical metrics (like Blood Pressure) fall within realistic biological ranges.

---

### **3. Gold (Curated & Analytical Layer)**
* **Action:** Complex **JOIN** operations and business logic application.
* **Output:** Analytical views such as **vw_RiskFactorCorrelation** and **vw_OccupationalHealthMetrics**.
* **Usage:** Optimized for direct consumption by **Power BI** dashboards and **Machine Learning** pipelines (XGBoost/Random Forest).

---

## **📊 Clinical Domains Integrated**
* **Metabolic Health:** Glucose, HbA1c, and Insulin dynamics.
* **Cardiovascular Risk:** Blood pressure (Systolic/Diastolic), sodium intake, and cholesterol metrics.
* **Sleep & Lifestyle:** Sleep Apnea severity (AHI), SpO2 levels, and stress scores correlated by **Occupation**.

---

## **🛠 Tech Stack**
* **Database Engine:** Microsoft SQL Server
* **Management Tool:** SQL Server Management Studio (SSMS)
* **ETL/Scripting:** T-SQL (Transact-SQL)
* **Data Prep:** Python (Pandas) for initial format validation
* **Visualization:** Power BI

---

## **🚀 Implementation Workflow**

### **1. Database Initialization**
```sql
-- Create Database
create database DataWarehouse;
go
use DataWarehouse;
go

-- Create schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
```

---

### **2. Data Transformation Logic**
The **Silver layer** focuses on medical data integrity. T-SQL scripts normalize smoking statuses (e.g., "former", "never", "current") to ensure consistency when merging datasets from different research sources.

---

### **3. Analytical Insights**
**Gold views** are designed for immediate insight. For example, the `gold.vw_SleepDisorderAnalysis` aggregates data to show the prevalence of Sleep Apnea within specific high-stress professions like **Nursing** and **Engineering**.

---

## **📝 Future Roadmap**
* [ ] Automate ETL pipelines using **SQL Server Integration Services (SSIS)**.
* [ ] Implement **Row-Level Security (RLS)** for HIPAA-compliant data access.
* [ ] Transition to **Parquet** file formats in the Bronze layer to optimize storage performance.

---
