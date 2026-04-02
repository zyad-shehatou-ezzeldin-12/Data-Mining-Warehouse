/*
==================================
create Database and schemas
==================================

script purpose:
this script create a new database named DataWarehouse and schemas for three layers 
bronze, silver, and gold.
*/




USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
  
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
